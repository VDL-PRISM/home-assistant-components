from datetime import datetime, timedelta
import logging
from queue import Queue
import random
import struct
import time

from coapthon.messages.message import Message
from coapthon import defines
from coapthon.client.coap import CoAP
from coapthon.messages.request import Request
from coapthon.utils import generate_random_token
import msgpack
import voluptuous as vol

from homeassistant.components.sensor import PLATFORM_SCHEMA
from homeassistant.const import CONF_HOST, CONF_NAME
import homeassistant.helpers.config_validation as cv
from homeassistant.helpers.entity import Entity
from homeassistant.helpers.event import track_point_in_time
import homeassistant.util.dt as dt_util


_LOGGER = logging.getLogger(__name__)

DEPENDENCIES = []
REQUIREMENTS = ['msgpack-python==0.4.8', 'CoAPy==4.1.0']

CONF_MONITORS = 'monitors'
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
    vol.Required(CONF_MONITORS): [{
        vol.Required(CONF_NAME): cv.string,
        vol.Required(CONF_HOST): cv.string
    }],
    vol.Optional(CONF_SENSORS, default=DEFAULT_SENSORS): cv.ensure_list,
    vol.Optional(CONF_UPDATE, default=60): cv.positive_int
})

def setup_platform(hass, config, add_devices, discovery_info=None):
    dylos_data = DylosData(hass, config[CONF_UPDATE])
    sensors = []

    for monitor in config[CONF_MONITORS]:
        callbacks = []

        for sensor in config[CONF_SENSORS]:
            dylos_sensor = DylosSensor(monitor[CONF_NAME], sensor)
            sensors.append(dylos_sensor)
            callbacks.append(dylos_sensor.update)

        dylos_data.add_device(DylosDevice(monitor[CONF_HOST],
                                          monitor[CONF_NAME],
                                          callbacks))

    add_devices(sensors)
    dylos_data.start()


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


class DylosDevice(object):
    def __init__(self, host, monitor, callbacks):
        self.host = host
        self.monitor = monitor
        self.callbacks = callbacks
        self.ack = 0

        self.client = Client(server=(host, 5683))


class DylosData(object):
    def __init__(self, hass, update_time):
        self.hass = hass
        self.update_time = update_time
        self.devices = []

        self.size = 20

    def add_device(self, device):
        self.devices.append(device)

    def start(self):
        def next_time():
            return dt_util.now() + timedelta(seconds=self.update_time)

        def action(now):
            self.update()

            # Schedule again
            next = next_time()
            _LOGGER.debug("Scheduling to get data at %s", next)
            track_point_in_time(self.hass, action, next)

        # Set up reoccurring update
        next = next_time()
        _LOGGER.debug("Scheduling to get data at %s", next)
        track_point_in_time(self.hass, action, next)

    def update_device(self, device):
        _LOGGER.debug("Getting new data from %s (%s) at %s",
                      device.host,
                      device.monitor,
                      datetime.now())

        try:
            data = None
            total_packets = 0

            while True:
                _LOGGER.debug("ACKing %s and requesting %s (%s - %s)",
                              device.ack,
                              self.size,
                              device.monitor,
                              device.host)
                payload = struct.pack('!HH', device.ack, self.size)
                response = device.client.get('air_quality', payload=payload)

                if response is None:
                    _LOGGER.debug(
                        "Did not receive a response from sensor %s - %s",
                        device.monitor, device.host)
                    break

                if len(response.payload) == 0:
                    _LOGGER.debug(
                        "Received an empty payload from %s - %s",
                        device.monitor, device.host)
                    break

                data = msgpack.unpackb(response.payload, use_list=False)
                _LOGGER.debug("Received data (%s): %s (%s - %s - %s)",
                              len(data),
                              data,
                              device.monitor,
                              device.host,
                              response.mid)

                device.ack = len(data)
                total_packets += device.ack

                keys = ['humidity', 'large', 'monitorname', 'sampletime',
                        'sequence', 'small', 'temperature']

                # For each new piece of data, notify everyone that has
                # registered a callback
                for d in data:
                    _LOGGER.debug("Calling callbacks for %s - %s on %s",
                                  device.monitor,
                                  device.host, d)

                    # Transform data into a dict
                    d = dict(zip(keys, d))

                    for cb in device.callbacks:
                        cb(d)
                        time.sleep(.1)

                # If we get all of the data we ask for, then let's request more
                # right away
                if device.ack != self.size:
                    _LOGGER.debug(
                        "%s - %s: Stopping because acks (%s) != size (%s)",
                        device.monitor, device.host, device.ack, self.size)
                    time.sleep(5)
                    break

                # Let's give the system some time to catch up
                # We will try again after CONF_UPDATE amount of time
                if total_packets >= 120:
                    _LOGGER.debug(
                        "%s - %s: Stopping because total_packets (%s) > 120",
                        device.monitor, device.host, total_packets)
                    time.sleep(5)
                    break

        except Exception:
            device.ack = 0
            _LOGGER.exception(
                "Unable to receive data or unpack data: %s (%s - %s)",
                data, device.monitor, device.host)

    def update(self):
        _LOGGER.debug("Updating Dylos data")

        for device in self.devices:
            self.update_device(device)


class Client(object):
    def __init__(self, server):
        self.server = server
        self.protocol = CoAP(self.server,
                             random.randint(1, 65535),
                             self._wait_response,
                             self._timeout)
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

    def get(self, path, payload=None):  # pragma: no cover
        request = Request()
        request.destination = self.server
        request.code = defines.Codes.GET.number
        request.uri_path = path
        request.payload = payload

        self.protocol.send_message(request)
        response = self.queue.get(block=True)
        _LOGGER.debug("%s: Got response to GET request with MID: %s", self.server[0], request.mid)
        return response

    def discover(self):  # pragma: no cover
        request = Request()
        request.destination = self.server
        request.code = defines.Codes.GET.number
        request.uri_path = defines.DISCOVERY_URL

        self.protocol.send_message(request)
        response = self.queue.get(block=True)
        return response

