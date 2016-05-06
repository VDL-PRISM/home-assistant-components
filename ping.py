"""
Component to keep track of if devices are able to connect to it. This
component requires some additional configuration on the devices you want to
track. They must connect to http(s)://<server>/api/ping and send a home_id and
keep-alive value. Here is an example using curl:

curl -X POST --data '{"id":"test", "keep-alive":5}' http://<server>/api/ping

And using httpie:

http -v POST http://<server>/api/ping keep-alive=5 id=test
"""
from datetime import timedelta
import logging

from homeassistant.components.binary_sensor import BinarySensorDevice
from homeassistant.const import (
    HTTP_UNPROCESSABLE_ENTITY, HTTP_CREATED, HTTP_OK, EVENT_TIME_CHANGED)
from homeassistant.helpers.event import track_point_in_utc_time
import homeassistant.util.dt as dt_util

_LOGGER = logging.getLogger(__name__)

DEPENDENCIES = ['http']

URL_API_PING_ENDPOINT = "/api/ping"

devices = {}


def setup_platform(hass, config, add_devices, discovery_info=None):
    def ping(handler, path_match, data):
        if 'id' not in data:
            handler.write_text('"id" not specified',
                               HTTP_UNPROCESSABLE_ENTITY)
            return

        if 'keep-alive' not in data:
            handler.write_text('"keep-alive" not specified',
                               HTTP_UNPROCESSABLE_ENTITY)
            return

        try:
            name = data['id']
            keep_alive = int(data['keep-alive'])
        except ValueError:
            handler.write_text('"keep-alive" must be an integer',
                               HTTP_UNPROCESSABLE_ENTITY)
            return

        next_time = dt_util.utcnow() + timedelta(seconds=1.5 * keep_alive)

        # Check if device has already been added
        if name not in devices:
            sensor = PingSensor(hass, name, next_time)
            add_devices([sensor])
            devices[name] = sensor

            handler.write_text('Created new sensor', HTTP_CREATED)
        else:
            devices[name].alive_time = next_time
            devices[name].update(dt_util.utcnow())

            handler.write_text('Updated sensor', HTTP_OK)

    hass.http.register_path(
        'POST', URL_API_PING_ENDPOINT, ping)


class PingSensor(BinarySensorDevice):
    """Representation of a Ping binary sensor."""

    def __init__(self, hass, name, alive_time):
        """Initialize a Ping binary sensor."""
        self._hass = hass
        self._name = name
        self._still_alive = True
        self._timer_callback = None

        self.alive_time = alive_time

    @property
    def name(self):
        """Return the name of the binary sensor."""
        return "{} Connected".format(self._name)

    @property
    def sensor_class(self):
        """Return the class of this sensor."""
        return 'connectivity'

    @property
    def is_on(self):
        """Return true if the binary sensor is on."""
        return self._still_alive

    @property
    def should_poll(self):
        return False

    @property
    def alive_time(self):
        return self._alive_time

    @alive_time.setter
    def alive_time(self, value):
        self._alive_time = value

        if self._timer_callback is not None:
            self._hass.bus.remove_listener(EVENT_TIME_CHANGED,
                                           self._timer_callback)
            self._timer_callback = None

        self._timer_callback = track_point_in_utc_time(self._hass,
                                                       self.update,
                                                       self._alive_time)

    def update(self, now):
        """Update sensor"""
        self._still_alive = self.alive_time > now
        self.update_ha_state()
