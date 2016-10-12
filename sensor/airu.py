import json
import logging

import voluptuous as vol

import homeassistant.components.mqtt as mqtt
from homeassistant.components.sensor import PLATFORM_SCHEMA
import homeassistant.helpers.config_validation as cv
from homeassistant.helpers.entity import Entity
import homeassistant.util.dt as dt_util

_LOGGER = logging.getLogger(__name__)

DEPENDENCIES = ['mqtt']

CONF_MONITOR = 'monitor'
CONF_SENSORS = 'sensors'

DEFAULT_SENSORS = ['temperature', 'humidity', 'pressure', 'altitude',
                   'lat', 'lon', 'pm1', 'pm25', 'pm10', 'sequence']
SENSOR_TYPES = {
    'temperature': '°C',
    'humidity': '%',
    'pressure': 'Pa',
    'altitude': 'm',
    'lat': 'deg',
    'lon': 'deg',
    'pm1': 'ug/m3',
    'pm25': 'ug/m3',
    'pm10': 'ug/m3',
    'sequence': 'sequence'
}

PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend({
    vol.Required(CONF_MONITOR): cv.string,
    vol.Optional(CONF_SENSORS, default=DEFAULT_SENSORS): cv.ensure_list,
})

def setup_platform(hass, config, add_devices, discovery_info=None):
    topic = "prisms/airUquality/{}".format(config['monitor'])

    callbacks = []
    sensors = []

    for sensor in config['sensors']:
        airu = AirUSensor(config['monitor'], sensor)

        sensors.append(airu)
        callbacks.append(airu.update)

    def message_received(topic, payload, qos):
        data = json.loads(payload)

        for cb in callbacks:
            cb(data)

    mqtt.subscribe(hass, topic, message_received, qos=0)
    add_devices(sensors)


class AirUSensor(Entity):
    def __init__(self, monitor, sensor_name):
        self._monitor = monitor
        self._name = sensor_name
        self._unit_of_measurement = SENSOR_TYPES[sensor_name]
        self._data = None

    def update(self, data):
        self._data = data
        self.update_ha_state()

    @property
    def should_poll(self):
        """No polling needed."""
        return False

    @property
    def name(self):
        """Return the name of the sensor."""
        return '{} {}'.format(self._monitor, self._name)

    @property
    def unit_of_measurement(self):
        """Return the unit of measurement of this entity, if any."""
        return self._unit_of_measurement

    @property
    def device_state_attributes(self):
        """Return the state attributes."""
        if self._data is None:
            return None

        return {'sequence': self._data['sequence'],
                'sample_time': dt_util.utc_from_timestamp(self._data['sampletime'])}

    @property
    def state(self):
        """Return the state of the entity."""
        if self._data is None:
            return None

        return self._data[self._name]

    @property
    def force_update(self):
        return True


