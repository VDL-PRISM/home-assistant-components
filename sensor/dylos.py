from datetime import datetime, timedelta
import logging
from queue import Queue, Empty
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
CONF_DISCOVER = 'discovery'
CONF_UPDATE_TIME = 'update_time'
CONF_DISCOVER_TIME = 'discover_time'
CONF_BATCH_SIZE = 'batch_size'
CONF_MAX_DATA_TRANSFERED = 'max_data_transfered'

DEFAULT_SENSORS = ['temperature', 'humidity', 'large', 'small', 'sequence']
SENSOR_TYPES = {
    'temperature': '°C',
    'humidity': '%',
    'large': 'pm',
    'small': 'pm',
    'sequence': 'sequence'
}

PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend({
    vol.Optional(CONF_MONITORS, default=[]): [{
        vol.Required(CONF_NAME): cv.string,
        vol.Required(CONF_HOST): cv.string
    }],
    vol.Optional(CONF_DISCOVER, default=True): cv.boolean,
    vol.Optional(CONF_SENSORS, default=DEFAULT_SENSORS): cv.ensure_list,
    vol.Optional(CONF_UPDATE_TIME, default=60): cv.positive_int,
    vol.Optional(CONF_DISCOVER_TIME, default=300): cv.positive_int,
    vol.Optional(CONF_BATCH_SIZE, default=20): cv.positive_int,
    vol.Optional(CONF_MAX_DATA_TRANSFERED, default=120): cv.positive_int,
})

def setup_platform(hass, config, add_devices, discovery_info=None):
    def next_data_time():
        return dt_util.now() + timedelta(seconds=config[CONF_UPDATE_TIME])

    def next_discover_time():
        return dt_util.now() + timedelta(seconds=config[CONF_DISCOVER_TIME])

    devices = {}
    sensors = []

    for monitor in config[CONF_MONITORS]:
        callbacks = []

        for sensor in config[CONF_SENSORS]:
            dylos_sensor = DylosSensor(monitor[CONF_NAME], sensor)
            sensors.append(dylos_sensor)
            callbacks.append(dylos_sensor.update)

        devices[monitor[CONF_HOST]] = DylosDevice(monitor[CONF_HOST],
                                                  monitor[CONF_NAME],
                                                  callbacks)

    def data_action(now):
        for device in devices.values():
            get_data(device,
                     config[CONF_BATCH_SIZE],
                     config[CONF_MAX_DATA_TRANSFERED])

        # Schedule again
        next = next_data_time()
        _LOGGER.debug("Scheduling to get data at %s", next)
        track_point_in_time(hass, data_action, next)

    # Set up reoccurring update
    next = next_data_time()
    _LOGGER.debug("Scheduling to get data at %s", next)
    track_point_in_time(hass, data_action, next)


    def discover_action(now):
        discover(devices, add_devices, config[CONF_SENSORS])

        # Schedule again
        next = next_discover_time()
        _LOGGER.debug("Scheduling to discover at %s", next)
        track_point_in_time(hass, discover_action, next)

    if config[CONF_DISCOVER]:
        # Set up reoccurring discovery
        next = next_discover_time()
        _LOGGER.debug("Scheduling to discover at %s", next)
        track_point_in_time(hass, discover_action, next)

    # Finish setting up
    add_devices(sensors)


def discover(devices, add_devices, config_sensor):
    # Connect to multicast address
    client = Client(server=('224.0.1.187', 5683))

    responses = client.multicast_discover()

    for response in responses:
        # TODO: I should actually parse the response and not just match
        if b'</air_quality>' not in response.payload:
            # It's not a dylos sensor
            continue

        hostname = response.source[0]
        _LOGGER.debug("Found device: %s", hostname)
        if hostname in devices:
            _LOGGER.debug("Dylos device has already been discovered")
            continue

        # TODO: Get name of device

        # Add the new device to home assistant
        sensors = []
        callbacks = []

        _LOGGER.debug("Adding %s to home assistant", hostname)
        for sensor in config_sensor:
            dylos_sensor = DylosSensor('???', sensor)
            sensors.append(dylos_sensor)
            callbacks.append(dylos_sensor.update)


        devices[hostname] = DylosDevice(hostname,
                                        '???',
                                        callbacks)
        add_devices(sensors)



def get_data(device, batch_size, max_data_transfered):
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
                          batch_size,
                          device.monitor,
                          device.host)
            payload = struct.pack('!HH', device.ack, batch_size)
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
            if device.ack != batch_size:
                _LOGGER.debug(
                    "%s - %s: Stopping because acks (%s) != size (%s)",
                    device.monitor, device.host, device.ack, batch_size)
                time.sleep(5)
                break

            # Let's give the system some time to catch up
            # We will try again after CONF_UPDATE_TIME amount of time
            if total_packets >= max_data_transfered:
                _LOGGER.debug(
                    "%s - %s: Stopping because total_packets (%s) > %s",
                    device.monitor, device.host, total_packets, max_data_transfered)
                time.sleep(5)
                break

    except Exception:
        device.ack = 0
        _LOGGER.exception(
            "Unable to receive data or unpack data: %s (%s - %s)",
            data, device.monitor, device.host)


class DylosDevice(object):
    def __init__(self, host, monitor, callbacks):
        self.host = host
        self.monitor = monitor
        self.callbacks = callbacks
        self.ack = 0

        self.client = Client(server=(host, 5683))


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

    def multicast_discover(self): # pragma: no cover
        request = Request()
        request.destination = self.server
        request.code = defines.Codes.GET.number
        request.uri_path = defines.DISCOVERY_URL

        self.protocol.send_message(request)

        responses = []

        try:
            while True:
                responses.append(self.queue.get(block=True, timeout=5))
        except Empty:
            pass

        return responses

