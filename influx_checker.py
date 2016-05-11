"""
"""
import logging
import time

import voluptuous as vol

from homeassistant.const import STATE_UNKNOWN, CONF_PLATFORM
from homeassistant.helpers.entity import Entity
import homeassistant.helpers.config_validation as cv


_LOGGER = logging.getLogger(__name__)

REQUIREMENTS = ['influxdb==2.12.0']

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 8086
DEFAULT_DATABASE = 'home_assistant'
DEFAULT_SSL = False
DEFAULT_VERIFY_SSL = False

CONF_HOST = 'host'
CONF_PORT = 'port'
CONF_DB_NAME = 'database'
CONF_USERNAME = 'username'
CONF_PASSWORD = 'password'
CONF_SSL = 'ssl'
CONF_VERIFY_SSL = 'verify_ssl'

PLATFORM_SCHEMA = vol.Schema({
    vol.Required(CONF_PLATFORM): 'influx_checker',
    vol.Optional(CONF_HOST, default=DEFAULT_HOST): cv.string,
    vol.Optional(CONF_PORT, default=DEFAULT_PORT): cv.positive_int,
    vol.Optional(CONF_DB_NAME, default=DEFAULT_DATABASE): cv.string,
    vol.Optional(CONF_USERNAME, default=None): cv.string,
    vol.Optional(CONF_PASSWORD, default=None): cv.string,
    vol.Optional(CONF_SSL, default=DEFAULT_SSL): cv.boolean,
    vol.Optional(CONF_VERIFY_SSL, default=DEFAULT_VERIFY_SSL): cv.boolean
})


def setup_platform(hass, config, add_devices, discovery_info=None):
    from influxdb import InfluxDBClient
    from influxdb.exceptions import InfluxDBClientError, InfluxDBServerError

    try:
        client = InfluxDBClient(host=config[CONF_HOST],
                                port=config[CONF_PORT],
                                username=config[CONF_USERNAME],
                                password=config[CONF_PASSWORD],
                                database=config[CONF_DB_NAME],
                                ssl=config[CONF_SSL],
                                verify_ssl=config[CONF_VERIFY_SSL])
    except (InfluxDBClientError, InfluxDBServerError) as e:
        _LOGGER.error("Unable to connect to InfluxDB: %s", e)
        return False

    # Get all home ids
    data = client.query('SHOW TAG VALUES WITH KEY = home_id')

    # Only keep track of unique
    data = set([x['value'] for measurement in data for x in measurement])

    add_devices([InfluxSensor(client, home) for home in data])


class InfluxSensor(Entity):
    """Representation of a Influx sensor."""

    def __init__(self, influx, name):
        """Initialize a Influx sensor."""
        self._influx = influx
        self._name = name
        self._unit_of_measurement = 's'
        self._state = STATE_UNKNOWN
        self.update()

    @property
    def name(self):
        """Return the name of the sensor."""
        return "{} Updated".format(self._name.replace("_", " ").title())

    @property
    def unit_of_measurement(self):
        return self._unit_of_measurement

    @property
    def state(self):
        """Return the state of the device."""
        return self._state

    def update(self):
        """Update sensor"""

        data = self._influx.query("SELECT value from /.*/ where home_id = '{}'"
                                  " ORDER BY DESC LIMIT 1".format(self._name),
                                  epoch='ns')

        times = sorted([x[0]['time'] for x in data], reverse=True)
        self._state = round(time.time() - (times[0] / 1e9))
