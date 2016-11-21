from datetime import datetime, timedelta
import json
import logging
from queue import Queue
import random
import struct

from coapthon.messages.message import Message
from coapthon import defines
from coapthon.client.coap import CoAP
from coapthon.messages.request import Request
from coapthon.utils import generate_random_token, parse_uri
import msgpack
import voluptuous as vol

import homeassistant.components.mqtt as mqtt
from homeassistant.components.sensor import PLATFORM_SCHEMA
from homeassistant.const import CONF_HOST, STATE_UNKNOWN
import homeassistant.helpers.config_validation as cv
from homeassistant.helpers.entity import Entity
from homeassistant.helpers.event import track_point_in_time
import homeassistant.util.dt as dt_util


_LOGGER = logging.getLogger(__name__)

DEPENDENCIES = []
REQUIREMENTS = ['msgpack-python==0.4.8', 'CoAPy==4.1.0']

CONF_MONITOR = 'monitor'
CONF_SENSORS = 'sensors'
CONF_UPDATE = 'update_time'

DEFAULT_SENSORS = ['temperature', 'humidity', 'large', 'small', 'sequence']
SENSOR_TYPES = {
    'temperature': '°C',
    'humidity': '%',
    'large': 'pm',
    'small': 'pm',
    'sequence': 'sequence'
}

PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend({
    vol.Required(CONF_MONITOR): cv.string,
    vol.Required(CONF_HOST): cv.string,
    vol.Optional(CONF_SENSORS, default=DEFAULT_SENSORS): cv.ensure_list,
    vol.Optional(CONF_UPDATE, default=60): cv.positive_int
})

def setup_platform(hass, config, add_devices, discovery_info=None):
    dylos_data = DylosData(config[CONF_HOST])
    sensors = []

    for sensor in config[CONF_SENSORS]:
        dylos = DylosSensor(config[CONF_MONITOR], sensor)

        sensors.append(dylos)
        dylos_data.add_callback(dylos.update)

    add_devices(sensors)

    def next_time():
        return dt_util.now() + timedelta(seconds=config[CONF_UPDATE])

    def action(now):
        dylos_data.update()

        # Schedule again
        track_point_in_time(hass, action, next_time())

    # Set up reoccurring update
    track_point_in_time(hass, action, next_time())


class DylosSensor(Entity):
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


class DylosData(object):
    def __init__(self, host):
        self.host = host
        self.callbacks = []
        self.client = Client(server=(host, 5683))
        self.acks = 0
        self.size = 20

    def add_callback(self, cb):
        self.callbacks.append(cb)

    def update(self):
        _LOGGER.debug("Getting new data from %s (%s)", self.host, datetime.now())

        try:
            data = None

            while True:
                _LOGGER.debug("ACKing %s and requesting %s", self.acks, self.size)
                payload = struct.pack('!HH', self.acks, self.size)
                response = self.client.get('air_quality', payload=payload)

                if response is None:
                    _LOGGER.debug("Did not receive a response from sensor %s", self.host)
                    return

                _LOGGER.debug("Received payload: %s", response.payload)

                data = msgpack.unpackb(response.payload, use_list=False)
                _LOGGER.debug("Received data: %s", data)
                self.acks = len(data)

                keys = ['humidity', 'large', 'monitorname', 'sampletime',
                        'sequence', 'small', 'temperature']

                # For each new piece of data, notify everyone that has registered
                # a callback
                for d in data:
                    # Transform data into a dict
                    d = dict(zip(keys, d))

                    for cb in self.callbacks:
                        cb(d)

                # If we get all of the data we ask for, then let's request more right away
                if self.acks != self.size:
                    return

        except Exception:
            self.acks = 0
            _LOGGER.exception("Unable to receive data or unpack data: %s", data)


class Client(object):
    def __init__(self, server):
        self.server = server
        self.protocol = CoAP(self.server, random.randint(1, 65535), self._wait_response, self._timeout)
        self.queue = Queue()

    def _wait_response(self, message):
        if message.code != defines.Codes.CONTINUE.number:
            self.queue.put(message)

    def _timeout(self, message):
        _LOGGER.warning("Timed out trying to send message: %s", message)
        self.queue.put(None)

    def stop(self):
        self.protocol.stopped.set()
        self.queue.put(None)

    def _thread_body(self, request, callback):
        self.protocol.send_message(request)
        while not self.protocol.stopped.isSet():
            response = self.queue.get(block=True)
            callback(response)

    def cancel_observing(self, response, send_rst):  # pragma: no cover
        if send_rst:
            message = Message()
            message.destination = self.server
            message.code = defines.Codes.EMPTY.number
            message.type = defines.Types["RST"]
            message.token = response.token
            message.mid = response.mid
            self.protocol.send_message(message)
        self.stop()

    def get(self, path, payload=None, callback=None):  # pragma: no cover
        request = Request()
        request.destination = self.server
        request.code = defines.Codes.GET.number
        request.uri_path = path
        request.payload = payload

        if callback is not None:
            thread = threading.Thread(target=self._thread_body, args=(request, callback))
            thread.start()
        else:
            self.protocol.send_message(request)
            response = self.queue.get(block=True)
            return response

    def observe(self, path, callback):  # pragma: no cover
        request = Request()
        request.destination = self.server
        request.code = defines.Codes.GET.number
        request.uri_path = path
        request.observe = 0

        if callback is not None:
            thread = threading.Thread(target=self._thread_body, args=(request, callback))
            thread.start()
        else:
            self.protocol.send_message(request)
            response = self.queue.get(block=True)
            return response

    def delete(self, path, callback=None):  # pragma: no cover
        request = Request()
        request.destination = self.server
        request.code = defines.Codes.DELETE.number
        request.uri_path = path
        if callback is not None:
            thread = threading.Thread(target=self._thread_body, args=(request, callback))
            thread.start()
        else:
            self.protocol.send_message(request)
            response = self.queue.get(block=True)
            return response

    def post(self, path, payload, callback=None):  # pragma: no cover
        request = Request()
        request.destination = self.server
        request.code = defines.Codes.POST.number
        request.token = generate_random_token(2)
        request.uri_path = path
        request.payload = payload
        if callback is not None:
            thread = threading.Thread(target=self._thread_body, args=(request, callback))
            thread.start()
        else:
            self.protocol.send_message(request)
            response = self.queue.get(block=True)
            return response

    def put(self, path, payload, callback=None):  # pragma: no cover
        request = Request()
        request.destination = self.server
        request.code = defines.Codes.PUT.number
        request.uri_path = path
        request.payload = payload
        if callback is not None:
            thread = threading.Thread(target=self._thread_body, args=(request, callback))
            thread.start()
        else:
            self.protocol.send_message(request)
            response = self.queue.get(block=True)
            return response

    def discover(self, callback=None):  # pragma: no cover
        request = Request()
        request.destination = self.server
        request.code = defines.Codes.GET.number
        request.uri_path = defines.DISCOVERY_URL
        if callback is not None:
            thread = threading.Thread(target=self._thread_body, args=(request, callback))
            thread.start()
        else:
            self.protocol.send_message(request)
            response = self.queue.get(block=True)
            return response

    def send_request(self, request, callback=None):  # pragma: no cover
        if callback is not None:
            thread = threading.Thread(target=self._thread_body, args=(request, callback))
            thread.start()
        else:
            self.protocol.send_message(request)
            response = self.queue.get(block=True)
            return response

    def send_empty(self, empty):  # pragma: no cover
        self.protocol.send_message(empty)
