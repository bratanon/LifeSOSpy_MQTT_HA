"""
This module contains the Translator class.
"""

import asyncio
from datetime import datetime
import json
import logging
import signal
from typing import List, Any, Dict
import dateutil
from janus import Queue
from lifesospy.baseunit import BaseUnit
from lifesospy.contactid import ContactID
from lifesospy.device import Device, SpecialDevice
from lifesospy.enums import (
    DeviceEventCode, DCFlags, ESFlags, SSFlags, SwitchFlags,
    OperationMode, BaseUnitState, ContactIDEventQualifier as EventQualifier,
    ContactIDEventCategory as EventCategory, DeviceType)
from lifesospy.propertychangedinfo import PropertyChangedInfo
from paho.mqtt.client import (
    Client as MQTTClient, MQTTMessage, CONNACK_ACCEPTED,
    connack_string, MQTT_ERR_SUCCESS)
from lifesospy_mqtt.config import (
    Config, TranslatorBaseUnitConfig, TranslatorDeviceConfig)
from lifesospy_mqtt.const import QOS_1, SCHEME_MQTTS
from lifesospy_mqtt.enums import OnOff, OpenClosed
from lifesospy_mqtt.subscribetopic import SubscribeTopic

logging.basicConfig(format='%(asctime)s,%(msecs)03d %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt='%Y-%m-%d:%H:%M:%S',
                    level=logging.DEBUG)

_LOGGER = logging.getLogger(__name__)


class Translator(object):
    """Translates messages between the LifeSOS and MQTT interfaces."""

    # Default interval to wait before resetting Trigger device state to Off
    AUTO_RESET_INTERVAL = 30

    # Keys for Home Assistant MQTT discovery configuration
    AVAILABILITY_TOPIC = 'availability_topic'
    COMMAND_TOPIC = 'command_topic'
    DEVICE = 'device'
    DEVICE_CLASS = 'device_class'
    ENTITY_CATEGORY = 'entity_category'
    ICON = 'icon'
    IDENTIFIERS = 'identifiers'
    NAME = 'name'
    OBJECT_ID = 'object_id'
    PAYLOAD_ARM_AWAY = 'payload_arm_away'
    PAYLOAD_ARM_HOME = 'payload_arm_home'
    PAYLOAD_AVAILABLE = 'payload_available'
    PAYLOAD_DISARM = 'payload_disarm'
    PAYLOAD_NOT_AVAILABLE = 'payload_not_available'
    PAYLOAD_OFF = 'payload_off'
    PAYLOAD_ON = 'payload_on'
    STATE_TOPIC = 'state_topic'
    UNIQUE_ID = 'unique_id'
    UNIT_OF_MEASUREMENT = 'unit_of_measurement'

    # Device class to classify the sensor type in Home Assistant
    DC_BATTERY = 'battery'
    DC_DOOR = 'door'
    DC_MOTION = 'motion'
    DC_SMOKE = 'smoke'

    # Icons in Home Assistant
    ICON_RSSI = 'mdi:wifi'

    # Entity class to classify the sensor type in Home Assistant
    DIAGNOSTIC = 'diagnostic'

    # Platforms in Home Assistant to represent our devices
    PLATFORM_ALARM_CONTROL_PANEL = 'alarm_control_panel'
    PLATFORM_BINARY_SENSOR = 'binary_sensor'
    PLATFORM_SENSOR = 'sensor'

    # Alarm states in Home Assistant
    STATE_ARMED_AWAY = 'armed_away'
    STATE_ARMED_HOME = 'armed_home'
    STATE_DISARMED = 'disarmed'
    STATE_PENDING = 'pending'
    STATE_TRIGGERED = 'triggered'

    # Unit of measurement for Home Assistant sensors
    UOM_RSSI = 'dB'

    # Ping MQTT broker this many seconds apart to check we're connected
    KEEP_ALIVE = 30

    # Attempt reconnection this many seconds apart
    # (starts at min, doubles on retry until max reached)
    RECONNECT_MAX_DELAY = 120
    RECONNECT_MIN_DELAY = 15

    # Sub-topic to clear the alarm/warning LEDs on base unit and stop siren
    TOPIC_CLEAR_STATUS = 'clear_status'

    # Sub-topic to access the remote date/time
    TOPIC_DATETIME = 'datetime'

    # Sub-topic to provide alarm state that is recognised by Home Assistant
    TOPIC_STATE = 'ha_state'

    # Sub-topic that will be subscribed to on topics that can be set
    TOPIC_SET = 'set'

    def __init__(self, config: Config):
        self._config = config
        self._loop = asyncio.get_event_loop()
        self._shutdown = False
        self._get_task = None
        self._auto_reset_handles = {}
        self._state = None
        self._ha_state = None

        # Create LifeSOS base unit instance and attach callbacks
        self._baseunit = BaseUnit(self._config.lifesos.host, self._config.lifesos.port)

        if self._config.lifesos.password:
            self._baseunit.password = self._config.lifesos.password

        self._baseunit.on_device_added = self._baseunit_device_added
        self._baseunit.on_device_deleted = self._baseunit_device_deleted
        self._baseunit.on_event = self._baseunit_event
        self._baseunit.on_properties_changed = self._baseunit_properties_changed

        # Create MQTT client instance
        self._mqtt = MQTTClient(client_id=self._config.mqtt.client_id, clean_session=False)
        self._mqtt.enable_logger()
        self._mqtt.will_set(
            '{}/{}'.format(
                self._config.translator.baseunit.topic,
                BaseUnit.PROP_IS_CONNECTED
            ),
            str(False).encode(),
            QOS_1,
            True
        )
        self._mqtt.reconnect_delay_set(Translator.RECONNECT_MIN_DELAY, Translator.RECONNECT_MAX_DELAY)

        if self._config.mqtt.uri.username:
            self._mqtt.username_pw_set(self._config.mqtt.uri.username, self._config.mqtt.uri.password)

        if self._config.mqtt.uri.scheme == SCHEME_MQTTS:
            self._mqtt.tls_set()

        self._mqtt.on_connect = self._mqtt_on_connect
        self._mqtt.on_disconnect = self._mqtt_on_disconnect
        self._mqtt.on_message = self._mqtt_on_message
        self._mqtt_was_connected = False
        self._mqtt_last_connection = None
        self._mqtt_last_disconnection = None

        # Generate a list of topics we'll need to subscribe to
        self._subscribetopics = []
        self._subscribetopics.append(
            SubscribeTopic(
                '{}/{}'.format(
                    self._config.translator.baseunit.topic,
                    Translator.TOPIC_CLEAR_STATUS
                ),
                self._on_message_clear_status
            )
        )
        self._subscribetopics.append(
            SubscribeTopic(
                '{}/{}/{}'.format(
                    self._config.translator.baseunit.topic,
                    Translator.TOPIC_DATETIME,
                    Translator.TOPIC_SET
                ),
                self._on_message_set_datetime
            )
        )
        names = [BaseUnit.PROP_OPERATION_MODE]
        for name in names:
            self._subscribetopics.append(
                SubscribeTopic(
                    '{}/{}/{}'.format(
                        self._config.translator.baseunit.topic,
                        name, Translator.TOPIC_SET
                    ),
                    self._on_message_baseunit,
                    args=name
                )
            )

        if self._config.translator.birth_topic:
            self._subscribetopics.append(
                SubscribeTopic(
                    self._config.translator.birth_topic,
                    self._on_message
                )
            )

        # Also create a lookup dict for the topics to subscribe to
        self._subscribetopics_lookup = \
            {st.topic: st for st in self._subscribetopics}

        # Create queue to store pending messages from our subscribed topics
        self._pending_messages = Queue()

    #
    # METHODS - Public
    #

    async def async_start(self) -> None:
        """Starts up the LifeSOS interface and connects to MQTT broker."""

        self._shutdown = False

        # Start up the LifeSOS interface
        self._baseunit.start()

        # Connect to the MQTT broker
        self._mqtt_was_connected = False
        if self._config.mqtt.uri.port:
            self._mqtt.connect_async(
                self._config.mqtt.uri.hostname,
                self._config.mqtt.uri.port,
                keepalive=Translator.KEEP_ALIVE
            )
        else:
            self._mqtt.connect_async(
                self._config.mqtt.uri.hostname,
                keepalive=Translator.KEEP_ALIVE
            )

        # Start processing MQTT messages
        self._mqtt.loop_start()

    async def async_loop(self) -> None:
        """Loop indefinitely to process messages from our subscriptions."""

        # Trap SIGINT and SIGTERM so that we can shutdown gracefully
        signal.signal(signal.SIGINT, self.signal_shutdown)
        signal.signal(signal.SIGTERM, self.signal_shutdown)
        try:
            while not self._shutdown:
                # Wait for next message
                self._get_task = self._loop.create_task(self._pending_messages.async_q.get())
                try:
                    message = await self._get_task
                except asyncio.CancelledError:
                    _LOGGER.debug('Translator loop cancelled.')
                    continue
                except Exception:  # pylint: disable=broad-except
                    # Log any exception but keep going
                    _LOGGER.error("Exception waiting for message to be delivered", exc_info=True)
                    continue
                finally:
                    self._get_task = None

                # Do topic callback to handle message
                try:
                    topic = self._subscribetopics_lookup[message.topic]
                    topic.on_message(topic, message)
                except Exception:  # pylint: disable=broad-except
                    _LOGGER.error("Exception processing message from subscribed topic: %s", message.topic,
                                  exc_info=True)
                finally:
                    self._pending_messages.async_q.task_done()

            # Turn off is_connected flag before leaving
            self._publish_baseunit_property(BaseUnit.PROP_IS_CONNECTED, False)
            await asyncio.sleep(0)
        finally:
            signal.signal(signal.SIGINT, signal.SIG_DFL)

    async def async_stop(self) -> None:
        """Shuts down the LifeSOS interface and disconnects from MQTT broker."""

        # Stop the LifeSOS interface
        self._baseunit.stop()

        # Cancel any outstanding auto reset tasks
        for item in self._auto_reset_handles.copy().items():
            item[1].cancel()
            self._auto_reset_handles.pop(item[0])

        # Stop processing MQTT messages
        self._mqtt.loop_stop()

        # Disconnect from the MQTT broker
        self._mqtt.disconnect()

    def signal_shutdown(self, sig, frame):
        """Flag shutdown when signal received."""
        _LOGGER.debug('%s received; shutting down...',
                      signal.Signals(sig).name)  # pylint: disable=no-member
        self._shutdown = True
        if self._get_task:
            self._get_task.cancel()

        # Issue #8 - Cancel not processed until next message added to queue.
        # Just put a dummy object on the queue to ensure it is handled immediately.
        self._pending_messages.sync_q.put_nowait(None)

    #
    # METHODS - Private / Internal
    #

    def _mqtt_on_connect(self, client: MQTTClient, userdata: Any,
                         flags: Dict[str, Any], result_code: int) -> None:
        # On error, log it and don't go any further; client will retry
        if result_code != CONNACK_ACCEPTED:
            _LOGGER.warning(connack_string(result_code))  # pylint: disable=no-member
            return

        # Successfully connected
        self._mqtt_last_connection = datetime.now()
        if not self._mqtt_was_connected:
            _LOGGER.debug("MQTT client connected to broker")
            self._mqtt_was_connected = True
        else:
            try:
                outage = self._mqtt_last_connection - self._mqtt_last_disconnection
                _LOGGER.warning("MQTT client reconnected to broker. "
                                "Outage duration was %s", str(outage))
            except Exception:  # pylint: disable=broad-except
                _LOGGER.warning("MQTT client reconnected to broker")

        # Republish the 'is_connected' state; this will have automatically
        # been set to False on MQTT client disconnection due to our will
        # (even though this app might still be connected to the LifeSOS unit)
        self._publish(
            '{}/{}'.format(
                self._config.translator.baseunit.topic,
                BaseUnit.PROP_IS_CONNECTED
            ),
            self._baseunit.is_connected,
            True
        )

        # Subscribe to topics we are capable of actioning
        for subscribetopic in self._subscribetopics:
            self._mqtt.subscribe(subscribetopic.topic, subscribetopic.qos)

    def _mqtt_on_disconnect(self, client: MQTTClient, userdata: Any, result_code: int) -> None:
        # When disconnected from broker and we didn't initiate it...
        if result_code != MQTT_ERR_SUCCESS:
            _LOGGER.warning("MQTT client lost connection to broker (RC: %i). "
                            "Will attempt to reconnect periodically", result_code)
            self._mqtt_last_disconnection = datetime.now()

    def _mqtt_on_message(self, client: MQTTClient, userdata: Any, message: MQTTMessage):
        # Add message to our queue, to be processed on main thread
        self._pending_messages.sync_q.put_nowait(message)

    def _baseunit_device_added(self, baseunit: BaseUnit, device: Device) -> None:
        # Hook up callbacks for device that was added / discovered
        device.on_event = self._device_on_event
        device.on_properties_changed = self._device_on_properties_changed

        # Get configuration settings for device; don't go any further when
        # device is not included in the config
        device_config = self._config.translator.devices.get(device.device_id)
        if not device_config:
            _LOGGER.info("Ignoring device as it was not listed in the config file: %s", device)
            return

        # Publish initial property values for device
        # if device_config.topic:
        #     props = device.as_dict()
        #     for name in props.keys():
        #         self._publish_device_property(device_config.topic, device, name, getattr(device, name))

        # When HA discovery is enabled, publish device configuration to it
        if self._config.translator.discovery_prefix:
            if device_config.topic:
                self._publish_device_config(device, device_config)
                self._publish_device_rssi_config(device, device_config)
                self._publish_device_battery_config(device, device_config)

    def _baseunit_device_deleted(self, baseunit: BaseUnit, device: Device) -> None:  # pylint: disable=no-self-use
        # Remove callbacks from deleted device
        device.on_event = None
        device.on_properties_changed = None

    def _baseunit_event(self, baseunit: BaseUnit, contact_id: ContactID):
        # When base unit event occurs, publish the event data
        # (don't bother retaining; events are time sensitive)
        event_data = json.dumps(contact_id.as_dict())
        self._publish(
            '{}/event'.format(self._config.translator.baseunit.topic),
            event_data, False)

        # For clients that can't handle json, we will also provide the event
        # qualifier and code via these topics
        if contact_id.event_code:
            if contact_id.event_qualifier == EventQualifier.Event:
                self._publish(
                    '{}/event_code'.format(
                        self._config.translator.baseunit.topic),
                    contact_id.event_code, False)
            elif contact_id.event_qualifier == EventQualifier.Restore:
                self._publish(
                    '{}/restore_code'.format(
                        self._config.translator.baseunit.topic),
                    contact_id.event_code, False)

        # This is just for Home Assistant; the 'alarm_control_panel.mqtt'
        # component currently requires these hard-coded state values
        if contact_id.event_qualifier == EventQualifier.Event and contact_id.event_category == EventCategory.Alarm:
            self._ha_state = Translator.STATE_TRIGGERED
            self._publish(
                '{}/{}'.format(self._config.translator.baseunit.topic, Translator.TOPIC_STATE),
                self._ha_state,
                True
            )

    def _baseunit_properties_changed(self, baseunit: BaseUnit, changes: List[PropertyChangedInfo]) -> None:
        # When base unit properties change, publish them
        has_connected = False
        for change in changes:
            self._publish_baseunit_property(change.name, change.new_value)

            # Also check if connection has just been established
            if change.name == BaseUnit.PROP_IS_CONNECTED and change.new_value:
                has_connected = True

        # On connection, publish config for Home Assistant if needed
        if has_connected:
            self._publish_ha_config()

    def _device_on_event(self, device: Device, event_code: DeviceEventCode) -> None:
        device_config = self._config.translator.devices.get(device.device_id)
        if device_config and device_config.topic:
            # When device event occurs, publish the event code
            # (don't bother retaining; events are time sensitive)
            self._publish('{}/event_code'.format(device_config.topic), event_code, False)

            if event_code in {DeviceEventCode.BatteryLow, DeviceEventCode.PowerOnReset}:
                self._publish('{}/battery'.format(device_config.topic), event_code, True)

            if event_code == DeviceEventCode.Tamper:
                self._publish('{}/tamper'.format(device_config.topic), True, False)

            # When it is a Trigger event, set state to On and schedule an
            # auto reset callback to occur after specified interval
            if event_code == DeviceEventCode.Trigger:
                self._publish(device_config.topic, OnOff.parse_value(True), True)
                handle = self._auto_reset_handles.get(device.device_id)
                if handle:
                    handle.cancel()
                handle = self._loop.call_later(
                    device_config.auto_reset_interval or Translator.AUTO_RESET_INTERVAL,
                    self._auto_reset, device.device_id)
                self._auto_reset_handles[device.device_id] = handle

    def _auto_reset(self, device_id: int):
        # Auto reset a Trigger device to Off state
        device_config = self._config.translator.devices.get(device_id)
        if device_config and device_config.topic:
            self._publish(device_config.topic, OnOff.parse_value(False), True)
        self._auto_reset_handles.pop(device_id)

    def _device_on_properties_changed(self, device: Device, changes: List[PropertyChangedInfo]):
        # When device properties change, publish them
        device_config = self._config.translator.devices.get(device.device_id)
        if device_config and device_config.topic:
            for change in changes:
                self._publish_device_property(
                    device_config.topic, device, change.name, change.new_value)

    def _publish_baseunit_property(self, name: str, value: Any) -> None:
        topic_parent = self._config.translator.baseunit.topic

        # Base Unit topic holds the state
        if name == BaseUnit.PROP_STATE:
            self._state = value
            self._publish(topic_parent, value, True)

            # This is just for Home Assistant; the 'alarm_control_panel.mqtt'
            # component currently requires these hard-coded state values
            topic = '{}/{}'.format(topic_parent, Translator.TOPIC_STATE)
            if value in {BaseUnitState.Disarm, BaseUnitState.Monitor}:
                self._ha_state = Translator.STATE_DISARMED
                self._publish(topic, self._ha_state, True)
            elif value == BaseUnitState.Home:
                self._ha_state = Translator.STATE_ARMED_HOME
                self._publish(topic, self._ha_state, True)
            elif value == BaseUnitState.Away:
                self._ha_state = Translator.STATE_ARMED_AWAY
                self._publish(topic, self._ha_state, True)
            elif value in {BaseUnitState.AwayExitDelay,
                           BaseUnitState.AwayEntryDelay}:
                self._ha_state = Translator.STATE_PENDING
                self._publish(topic, self._ha_state, True)

        # Other supported properties in a topic using property name
        elif name in {
            BaseUnit.PROP_IS_CONNECTED, BaseUnit.PROP_ROM_VERSION,
            BaseUnit.PROP_EXIT_DELAY, BaseUnit.PROP_ENTRY_DELAY,
            BaseUnit.PROP_OPERATION_MODE}:
            self._publish('{}/{}'.format(topic_parent, name), value, True)

    def _publish_device_property(self, topic_parent: str, device: Device,
                                 name: str, value: Any) -> None:
        # Device topic holds the state
        if (not isinstance(device, SpecialDevice)) and \
                name == Device.PROP_IS_CLOSED:
            # For regular device; this is the Is Closed property for magnet
            # sensors, otherwise default to Off for trigger-based devices
            if device.type == DeviceType.DoorMagnet:
                self._publish(topic_parent, OpenClosed.parse_value(value), True)
            else:
                self._publish(topic_parent, OnOff.Off, True)
        elif isinstance(device, SpecialDevice) and \
                name == SpecialDevice.PROP_CURRENT_READING:
            # For special device, this is the current reading
            self._publish(topic_parent, value, True)

        # Category will have sub-topics for it's properties
        elif name == Device.PROP_CATEGORY:
            for prop in value.as_dict().items():
                if prop[0] in {'code', 'description'}:
                    self._publish('{}/{}/{}'.format(
                        topic_parent, name, prop[0]), prop[1], True)

        # Flag enums; expose as sub-topics with a bool state per flag
        elif name == Device.PROP_CHARACTERISTICS:
            for item in iter(DCFlags):
                self._publish(
                    '{}/{}/{}'.format(topic_parent, name, item.name),
                    bool(value & item.value), True)
        elif name == Device.PROP_ENABLE_STATUS:
            for item in iter(ESFlags):
                self._publish(
                    '{}/{}/{}'.format(topic_parent, name, item.name),
                    bool(value & item.value), True)
        elif name == Device.PROP_SWITCHES:
            for item in iter(SwitchFlags):
                self._publish(
                    '{}/{}/{}'.format(topic_parent, name, item.name),
                    bool(value & item.value), True)
        elif name == SpecialDevice.PROP_SPECIAL_STATUS:
            for item in iter(SSFlags):
                self._publish(
                    '{}/{}/{}'.format(topic_parent, name, item.name),
                    bool(value & item.value), True)

        # Device ID; value should be formatted as hex
        elif name == Device.PROP_DEVICE_ID:
            self._publish('{}/{}'.format(topic_parent, name),
                          '{:06x}'.format(value), True)

        # Other supported properties in a topic using property name
        elif name in {
            Device.PROP_DEVICE_ID, Device.PROP_ZONE, Device.PROP_TYPE,
            Device.PROP_RSSI_DB, Device.PROP_RSSI_BARS,
            SpecialDevice.PROP_HIGH_LIMIT, SpecialDevice.PROP_LOW_LIMIT,
            SpecialDevice.PROP_CONTROL_LIMIT_FIELDS_EXIST,
            SpecialDevice.PROP_CONTROL_HIGH_LIMIT,
            SpecialDevice.PROP_CONTROL_LOW_LIMIT
        }:
            self._publish('{}/{}'.format(topic_parent, name), value, True)

    def _publish_ha_config(self):
        # Skip if Home Assistant discovery disabled
        if not self._config.translator.discovery_prefix:
            return

        # Publish config for the base unit when enabled
        if self._config.translator.baseunit.topic:
            self._publish_baseunit_config(self._baseunit, self._config.translator.baseunit)

        # Publish config for each device when enabled
        for device_id in self._config.translator.devices.keys():
            if self._shutdown:
                return
            device_config = self._config.translator.devices[device_id]
            device = self._baseunit.devices.get(device_id)
            if device:
                if device_config.topic:
                    self._publish_device_config(device, device_config)
                    self._publish_device_rssi_config(device, device_config)
                    self._publish_device_battery_config(device, device_config)

    def _publish_baseunit_config(self, baseunit: BaseUnit, baseunit_config: TranslatorBaseUnitConfig):
        # Generate message that can be used to automatically configure the
        # alarm control panel in Home Assistant using MQTT Discovery
        message = {
            Translator.NAME: None,
            Translator.OBJECT_ID: 'lifesos_baseunit',
            Translator.UNIQUE_ID: 'lifesos_baseunit',
            Translator.STATE_TOPIC: '{}/{}'.format(
                baseunit_config.topic, Translator.TOPIC_STATE),
            Translator.COMMAND_TOPIC: '{}/{}/{}'.format(
                baseunit_config.topic, BaseUnit.PROP_OPERATION_MODE,
                Translator.TOPIC_SET),
            Translator.PAYLOAD_DISARM: str(OperationMode.Disarm),
            Translator.PAYLOAD_ARM_HOME: str(OperationMode.Home),
            Translator.PAYLOAD_ARM_AWAY: str(OperationMode.Away),
            Translator.AVAILABILITY_TOPIC: '{}/{}'.format(
                baseunit_config.topic, BaseUnit.PROP_IS_CONNECTED),
            Translator.PAYLOAD_AVAILABLE: str(True),
            Translator.PAYLOAD_NOT_AVAILABLE: str(False),
            Translator.DEVICE: {
                **{Translator.IDENTIFIERS: 'lifesos_baseunit'},
                **baseunit_config.device_info,
            }
        }
        self._publish(
            '{}/{}/{}/config'.format(
                self._config.translator.discovery_prefix,
                Translator.PLATFORM_ALARM_CONTROL_PANEL,
                message[Translator.UNIQUE_ID]),
            json.dumps(message), False)

    def _publish_device_config(self, device: Device, device_config: TranslatorDeviceConfig):
        # Generate message that can be used to automatically configure the
        # device in Home Assistant using MQTT Discovery

        message = {
            Translator.NAME: None,
            Translator.OBJECT_ID: 'lifesos_{:06x}'.format(device.device_id),
            Translator.UNIQUE_ID: 'lifesos_{:06x}'.format(device.device_id),
            Translator.STATE_TOPIC: device_config.topic,
            Translator.AVAILABILITY_TOPIC: '{}/{}'.format(
                self._config.translator.baseunit.topic,
                BaseUnit.PROP_IS_CONNECTED),
            Translator.PAYLOAD_AVAILABLE: str(True),
            Translator.PAYLOAD_NOT_AVAILABLE: str(False),
            Translator.DEVICE: self._add_device_identifiers(
                device.device_id,
                device_config.device_info
            ),
        }

        if device.type in {DeviceType.SmokeDetector}:
            ha_platform = Translator.PLATFORM_BINARY_SENSOR
            message[Translator.DEVICE_CLASS] = Translator.DC_SMOKE
            message[Translator.PAYLOAD_ON] = str(OnOff.On)
            message[Translator.PAYLOAD_OFF] = str(OnOff.Off)
        elif device.type in {DeviceType.DoorMagnet}:
            ha_platform = Translator.PLATFORM_BINARY_SENSOR
            message[Translator.DEVICE_CLASS] = Translator.DC_DOOR
            message[Translator.PAYLOAD_ON] = str(OpenClosed.Open)
            message[Translator.PAYLOAD_OFF] = str(OpenClosed.Closed)
        elif device.type in {DeviceType.PIRSensor}:
            ha_platform = Translator.PLATFORM_BINARY_SENSOR
            message[Translator.DEVICE_CLASS] = Translator.DC_MOTION
            message[Translator.PAYLOAD_ON] = str(OnOff.On)
            message[Translator.PAYLOAD_OFF] = str(OnOff.Off)
        else:
            _LOGGER.warning("Device type '%s' cannot be represented in Home "
                            "Assistant and will be skipped.", str(device.type))
            return
        self._publish(
            '{}/{}/{}/config'.format(
                self._config.translator.discovery_prefix,
                ha_platform,
                message[Translator.UNIQUE_ID]),
            json.dumps(message), False)

    def _publish_device_rssi_config(self, device: Device,
                                    device_config: TranslatorDeviceConfig):
        # Generate message that can be used to automatically configure a sensor
        # for the device's RSSI in Home Assistant using MQTT Discovery
        message = {
            Translator.NAME: 'RSSI',
            Translator.OBJECT_ID: 'lifesos_{:06x}_rssi'.format(device.device_id),
            Translator.UNIQUE_ID: 'lifesos_{:06x}_rssi'.format(device.device_id),
            Translator.ICON: Translator.ICON_RSSI,
            Translator.STATE_TOPIC: '{}/{}'.format(
                device_config.topic,
                Device.PROP_RSSI_DB),
            Translator.UNIT_OF_MEASUREMENT: Translator.UOM_RSSI,
            Translator.AVAILABILITY_TOPIC: '{}/{}'.format(
                self._config.translator.baseunit.topic,
                BaseUnit.PROP_IS_CONNECTED),
            Translator.PAYLOAD_AVAILABLE: str(True),
            Translator.PAYLOAD_NOT_AVAILABLE: str(False),
            Translator.ENTITY_CATEGORY: Translator.DIAGNOSTIC,
            Translator.DEVICE: self._add_device_identifiers(
                device.device_id,
                device_config.device_info
            ),
        }

        self._publish(
            '{}/{}/{}/config'.format(
                self._config.translator.discovery_prefix,
                Translator.PLATFORM_SENSOR,
                message[Translator.UNIQUE_ID]),
            json.dumps(message), False)

    def _publish_device_battery_config(self, device: Device,
                                       device_config: TranslatorDeviceConfig):
        # Generate message that can be used to automatically configure a binary
        # sensor for the device's battery state in Home Assistant using
        # MQTT Discovery
        message = {
            Translator.NAME: 'Battery',
            Translator.OBJECT_ID: 'lifesos_{:06x}_battery'.format(device.device_id),
            Translator.UNIQUE_ID: 'lifesos_{:06x}_battery'.format(device.device_id),
            Translator.DEVICE_CLASS: Translator.DC_BATTERY,
            Translator.PAYLOAD_ON: str(DeviceEventCode.BatteryLow),
            Translator.PAYLOAD_OFF: str(DeviceEventCode.PowerOnReset),
            Translator.STATE_TOPIC: '{}/battery'.format(
                device_config.topic),
            Translator.AVAILABILITY_TOPIC: '{}/{}'.format(
                self._config.translator.baseunit.topic,
                BaseUnit.PROP_IS_CONNECTED),
            Translator.PAYLOAD_AVAILABLE: str(True),
            Translator.PAYLOAD_NOT_AVAILABLE: str(False),
            Translator.ENTITY_CATEGORY: Translator.DIAGNOSTIC,
            Translator.DEVICE: self._add_device_identifiers(
                device.device_id,
                device_config.device_info
            ),
        }

        self._publish(
            '{}/{}/{}/config'.format(
                self._config.translator.discovery_prefix,
                Translator.PLATFORM_BINARY_SENSOR,
                message[Translator.UNIQUE_ID]),
            json.dumps(message), False)

    def _add_device_identifiers(self, device_id: int, ha_device_info: Dict) -> Any:
        identifiers = {Translator.IDENTIFIERS: 'LifeSOS_{:06x}'.format(device_id)}
        return {**ha_device_info, **identifiers}

    def _publish(self, topic: str, payload: Any, retain: bool) -> None:
        self._mqtt.publish(topic, payload, QOS_1, retain)

    def _on_message_baseunit(self,
                             subscribetopic: SubscribeTopic,
                             message: MQTTMessage) -> None:
        if subscribetopic.args == BaseUnit.PROP_OPERATION_MODE:
            # Set operation mode
            name = None if not message.payload else message.payload.decode()
            operation_mode = OperationMode.parse_name(name)
            if operation_mode is None:
                _LOGGER.warning("Cannot set operation_mode to '%s'", name)
                return
            if operation_mode == OperationMode.Disarm and \
                    self._state == BaseUnitState.Disarm and \
                    self._ha_state == Translator.STATE_TRIGGERED:
                # Special case to ensure HA can return from triggered state
                # when triggered by an alarm in Disarm mode (eg. panic,
                # tamper)... the set disarm operation will not generate a
                # response from the base unit as there is no change, so we
                # need to reset 'ha_state' here.
                _LOGGER.debug("Resetting triggered ha_state in disarmed mode")
                self._ha_state = Translator.STATE_DISARMED
                self._publish(
                    '{}/{}'.format(self._config.translator.baseunit.topic,
                                   Translator.TOPIC_STATE),
                    self._ha_state, True)
            self._loop.create_task(
                self._baseunit.async_set_operation_mode(operation_mode))
        else:
            raise NotImplementedError

    def _on_message_clear_status(self,
                                 subscribetopic: SubscribeTopic,
                                 message: MQTTMessage) -> None:
        # Clear the alarm/warning LEDs on base unit and stop siren
        self._loop.create_task(
            self._baseunit.async_clear_status())

    def _on_message_set_datetime(self,
                                 subscribetopic: SubscribeTopic,
                                 message: MQTTMessage) -> None:
        # Set remote date/time to specified date/time (or current if None)
        value = None if not message.payload else message.payload.decode()
        if value:
            value = dateutil.parser.parse(value)
        self._loop.create_task(
            self._baseunit.async_set_datetime(value))

    def _on_message(self, subscribetopic: SubscribeTopic,
                       message: MQTTMessage) -> None:
        # When Home Assistant comes online, publish our configuration to it
        payload = None if not message.payload else message.payload.decode()
        if not payload:
            return
        if payload == self._config.translator.birth_payload:
            self._publish_ha_config()
