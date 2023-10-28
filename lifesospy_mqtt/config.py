"""
Configuration settings.
"""

import logging
import os
from typing import Any, Dict, Optional
from urllib.parse import urlparse, ParseResult
import yaml
from lifesospy.enums import SwitchNumber
from lifesospy_mqtt.const import SCHEME_MQTT, SCHEME_MQTTS
from lifesospy_mqtt.enums import LoggerLevel

_LOGGER = logging.getLogger(__name__)

CONF_AUTO_RESET_INTERVAL = 'auto_reset_interval'
CONF_BASEUNIT = 'baseunit'
CONF_BIRTH_PAYLOAD = 'birth_payload'
CONF_BIRTH_TOPIC = 'birth_topic'
CONF_CLIENT_ID = 'client_id'
CONF_DEFAULT = 'default'
CONF_DEVICE_ID = 'device_id'
CONF_DEVICE_INFO = 'device_info'
CONF_DEVICES = 'devices'
CONF_DISCOVERY_PREFIX = 'discovery_prefix'
CONF_HOST = 'host'
CONF_NAMESPACES = 'namespaces'
CONF_PASSWORD = 'password'
CONF_PORT = 'port'
CONF_TOPIC = 'topic'
CONF_URI = 'uri'

GROUP_LIFESOS = 'lifesos'
GROUP_LOGGER = 'logger'
GROUP_MQTT = 'mqtt'
GROUP_TRANSLATOR = 'translator'

DEFAULT_LOGGERLEVEL = LoggerLevel.Info

DEFAULT_CONFIG = """
# Settings for the LifeSOS interface
""" + GROUP_LIFESOS + """:

  # Host and Port for the LifeSOS TCP Server to connect as a client,
  # or Port only to listen as a server for the LifeSOS TCP Client
  """ + CONF_HOST + """: 192.168.1.100
  """ + CONF_PORT + """: 1680
  
  # Master password, if needed by network interface
  """ + CONF_PASSWORD + """: ''

# Settings for the MQTT client
""" + GROUP_MQTT + """:

  # URI providing the details needed to connect to the MQTT broker
  # Refer https://github.com/mqtt/mqtt.github.io/wiki/URI-Scheme
  """ + CONF_URI + """: mqtt://username:password@127.0.0.1:1883
  
  # Unique client identifier; no need to change unless running multiple instances
  """ + CONF_CLIENT_ID + """: lifesos

# Settings for the translator between LifeSOS and MQTT
""" + GROUP_TRANSLATOR + """:

  # To automatically configure devices in Home Assistant, ensure this line
  # matches the setting in Home Assistant's config file.
  # Refer https://www.home-assistant.io/docs/mqtt/discovery/
  """ + CONF_DISCOVERY_PREFIX + """: homeassistant
  
  # Topic and Payload to announce Home Assistant has come online. When received,
  # our MQTT client will send out the device configurations for discovery.
  """ + CONF_BIRTH_TOPIC + """: homeassistant/status
  """ + CONF_BIRTH_PAYLOAD + """: online

  # Provide a topic for the Base Unit here
  """ + CONF_BASEUNIT + """:
    """ + CONF_TOPIC + """: home/alarm
  
  # List your enrolled devices here and provide a topic
  # Hint: Run with '-e' option to get a list of device ids
  """ + CONF_DEVICES + """:
    #- """ + CONF_DEVICE_ID + """: '012cba'
    #  """ + CONF_TOPIC + """: home/remote
    #- """ + CONF_DEVICE_ID + """: '345def'
    #  """ + CONF_TOPIC + """: home/front/door
    #  """ + CONF_DEVICE_INFO + """:
    #    manufacturer: 'LifeSOS'
    #    model: 'TX-3DS'
    #- """ + CONF_DEVICE_ID + """: '123abc'
    #  """ + CONF_TOPIC + """: home/lounge/motion
    #  """ + CONF_AUTO_RESET_INTERVAL + """: 180

# Settings to configure logging
# Valid severity levels are:
# critical, error, warning, info, debug
""" + GROUP_LOGGER + """:

  """ + CONF_DEFAULT + """: """ + str(DEFAULT_LOGGERLEVEL).lower() + """
  
  #""" + CONF_NAMESPACES + """:
  #  lifesospy: """ + str(LoggerLevel.Debug).lower() + """
  #  lifesospy_mqtt: """ + str(LoggerLevel.Debug).lower() + """
  #  paho.mqtt: """ + str(LoggerLevel.Debug).lower() + """
"""


class Config(object):
    """Contains the configuration settings."""

    def __init__(self, settings: Dict[str, Any], is_default: bool):
        self._lifesos = LifeSOSConfig(settings[GROUP_LIFESOS])
        self._mqtt = MQTTConfig(settings[GROUP_MQTT])
        self._translator = TranslatorConfig(settings[GROUP_TRANSLATOR])
        self._logger = LoggerConfig(settings.get(GROUP_LOGGER))
        self._is_default = is_default

    @property
    def is_default(self) -> bool:
        """True if default configuration file was created; otherwise, False."""
        return self._is_default

    @property
    def lifesos(self) -> 'LifeSOSConfig':
        """Configuration settings for the LifeSOS group."""
        return self._lifesos

    @property
    def logger(self) -> 'LoggerConfig':
        """Configuration settings for the Logger group."""
        return self._logger

    @property
    def mqtt(self) -> 'MQTTConfig':
        """Configuration settings for the MQTT group."""
        return self._mqtt

    @property
    def translator(self) -> 'TranslatorConfig':
        """Configuration settings for the Translator group."""
        return self._translator

    @classmethod
    def load(cls, config_path: str) -> Optional['Config']:
        """Load the configuration file, or create default if none exists."""
        is_default = False

        if os.path.isfile(config_path):
            # Specified file exists; we can simply use that
            _LOGGER.debug("Loading configuration file '%s'", config_path)
        else:
            # Create new configuration file with default settings
            _LOGGER.debug("Creating default configuration file '%s'",
                          config_path)
            try:
                with open(config_path, 'wt') as config_file:
                    config_file.write(DEFAULT_CONFIG)
            except Exception: # pylint: disable=broad-except
                _LOGGER.error("Failed to create default configuration file",
                              exc_info=True)
                return None
            is_default = True

        # Load the configuration settings
        try:
            with open(config_path, encoding='utf-8') as config_file:
                settings = yaml.load(config_file, Loader=yaml.SafeLoader) or {}
        except Exception: # pylint: disable=broad-except
            _LOGGER.error("Failed to parse configuration file", exc_info=True)
            return None

        # Return instance of the configuration settings
        return Config(settings, is_default)

    def __repr__(self):
        return "<{}: is_default={}, {}, {}, {}, {}>".format(
            self.__class__.__name__,
            self._is_default,
            self._lifesos,
            self._mqtt,
            self._translator,
            self._logger,
        )


class LifeSOSConfig(object):
    """Configuration settings for the LifeSOS interface."""

    def __init__(self, settings: Dict[str, Any]):
        self._host = settings.get(CONF_HOST)
        self._port = settings[CONF_PORT]
        self._password = settings.get(CONF_PASSWORD)

    @property
    def host(self) -> Optional[str]:
        """
        Host name or IP address for the LifeSOS Server if we are to be run as
        a client, or None if we are to run as a server.
        """
        return self._host

    @property
    def password(self) -> str:
        """Control password, if one has been assigned on the base unit."""
        return self._password

    @property
    def port(self) -> int:
        """
        Port number to connect to / listen on, depending on whether we're
        running as a client or server.
        """
        return self._port

    def __repr__(self):
        return "<{}: host={}, port={}, password={}>".format(
            self.__class__.__name__,
            self._host,
            self._port,
            "None" if not self._password else ''.ljust(len(self._password), '*'),
        )


class MQTTConfig(object):
    """Configuration settings for the MQTT client."""

    def __init__(self, settings: Dict[str, Any]):
        self._uri = urlparse(settings[CONF_URI])
        self._client_id = settings[CONF_CLIENT_ID]

        # Check URI specifies a supported scheme
        if not (self._uri.scheme == SCHEME_MQTT or self._uri.scheme == SCHEME_MQTTS):
            raise ValueError(
                "URI scheme '{}' is not supported".format(self._uri.scheme))

    @property
    def client_id(self) -> str:
        """Unique client identifier."""
        return self._client_id

    @property
    def uri(self) -> ParseResult:
        """URI providing the details needed to connect to the MQTT broker."""
        return self._uri

    def __repr__(self):
        return "<{}: uri={}, client_id={}>".format(
            self.__class__.__name__,
            self._uri,
            self._client_id,
        )


class TranslatorConfig(object):
    """Configuration settings for the translator between LifeSOS and MQTT."""

    def __init__(self, settings: Dict[str, Any]):
        self._birth_payload = settings.get(CONF_BIRTH_PAYLOAD)
        self._birth_topic = settings.get(CONF_BIRTH_TOPIC)
        self._discovery_prefix = settings.get(CONF_DISCOVERY_PREFIX)
        baseunit_settings = settings[CONF_BASEUNIT]
        self._baseunit = TranslatorBaseUnitConfig(baseunit_settings)

        self._devices = {}
        devices_settings = settings.get(CONF_DEVICES)
        if devices_settings:
            for device_settings in devices_settings:
                device_id = int(device_settings[CONF_DEVICE_ID], 16)
                self._devices[device_id] = \
                    TranslatorDeviceConfig(device_settings)
    @property
    def baseunit(self) -> 'TranslatorBaseUnitConfig':
        """Configuration for the base unit."""
        return self._baseunit

    @property
    def devices(self) -> Dict[int, 'TranslatorDeviceConfig']:
        """Configuration for each enrolled device; lookup by device id."""
        return self._devices

    @property
    def birth_payload(self) -> str:
        """Payload used to identify when Home Assistant comes online."""
        return self._birth_payload

    @property
    def birth_topic(self) -> str:
        """Topic used to identify when Home Assistant comes online."""
        return self._birth_topic

    @property
    def discovery_prefix(self) -> str:
        """Discovery prefix to auto configure devices in Home Assistant."""
        return self._discovery_prefix

    def __repr__(self):
        return "<{}: baseunit={}, devices={}>".format(
            self.__class__.__name__,
            self._baseunit,
            self._devices,
        )


class TranslatorBaseUnitConfig(object):
    """Configuration settings for the translator specific to base unit."""

    def __init__(self, settings: Dict[str, Any]):
        self._topic = settings[CONF_TOPIC]
        self._device_info = settings.get(CONF_DEVICE_INFO)

    @property
    def topic(self) -> str:
        """Topic for the base unit."""
        return self._topic

    @property
    def device_info(self) -> Dict[str, str]:
        """Device info to assign the device's device in Home Assistant."""
        return self._device_info

    def __repr__(self):
        return "<{}: topic={} device_info={}>".format(
            self.__class__.__name__,
            self._topic,
            self._device_info,
        )


class TranslatorDeviceConfig(object):
    """Configuration settings for the translator specific to a device."""

    def __init__(self, settings: Dict[str, Any]):
        self._topic = settings[CONF_TOPIC]
        self._auto_reset_interval = settings.get(CONF_AUTO_RESET_INTERVAL)
        self._device_info = settings.get(CONF_DEVICE_INFO)

    @property
    def auto_reset_interval(self) -> int:
        """Interval to wait before resetting state of a Trigger device."""
        return self._auto_reset_interval

    @property
    def device_info(self) -> Dict[str, str]:
        """Device info to assign the device's device in Home Assistant."""
        return self._device_info

    @property
    def topic(self) -> str:
        """Topic for the device."""
        return self._topic

    def __repr__(self):
        return "<{}: topic={}, auto_reset_interval={}, device_info={}>".format(
                    self.__class__.__name__,
                    self._topic,
                    self._auto_reset_interval,
                    self._device_info,
                )


class LoggerConfig(object):
    """Configuration settings for logging."""

    def __init__(self, settings: Dict[str, Any]):
        self._default = DEFAULT_LOGGERLEVEL
        self._namespaces = {}

        if not settings:
            return

        self._default = LoggerLevel.parse_name(settings.get(CONF_DEFAULT))

        namespaces = settings.get(CONF_NAMESPACES)
        if namespaces:
            for namespace in namespaces.items():
                self._namespaces[namespace[0]] = \
                    LoggerLevel.parse_name(namespace[1])

    @property
    def default(self) -> LoggerLevel:
        """Default minimum severity level for logging."""
        return self._default

    @property
    def namespaces(self) -> Dict[str, LoggerLevel]:
        """Minimum severity level for a specific namespace."""
        return self._namespaces

    def __repr__(self):
        return "<{}: default={}, namespaces={}>".format(
            self.__class__.__name__,
            str(self._default),
            self._namespaces,
        )
