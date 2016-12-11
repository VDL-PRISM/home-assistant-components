from datetime import datetime, timedelta
import logging
from queue import Queue, Empty
import random
import re
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
REQUIREMENTS = ['msgpack-python==0.4.8', 'CoAPy==4.1.4']

CONF_MONITORS = 'monitors'
CONF_SENSORS = 'sensors'
CONF_DISCOVER = 'discovery'
CONF_UPDATE_TIME = 'update_time'
CONF_DISCOVER_TIME = 'discover_time'
CONF_BATCH_SIZE = 'batch_size'
CONF_MAX_DATA_TRANSFERRED = 'max_data_transferred'

SECONDS_IN_A_YEAR = 31536000

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
    vol.Optional(CONF_MAX_DATA_TRANSFERRED, default=120): cv.positive_int,
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
        device_list = list(devices.values())
        for device in device_list:
            try:
                get_data(device,
                         config[CONF_BATCH_SIZE],
                         config[CONF_MAX_DATA_TRANSFERRED])
            except Exception as exp:
                _LOGGER.exception(
                    "Error occurred while getting data from %s: %s",
                    device,
                    exp)

        # Schedule again
        next = next_data_time()
        _LOGGER.debug("Scheduling to get data at %s", next)
        track_point_in_time(hass, data_action, next)

    # Schedule a time to update
    next = next_data_time()
    _LOGGER.debug("Scheduling to get data at %s", next)
    track_point_in_time(hass, data_action, next)


    if config[CONF_DISCOVER]:
        # Connect to multicast address
        client = Client(server=('224.0.1.187', 5683))

        def discover_action(now):
            try:
                discover(client, devices, add_devices, config[CONF_SENSORS])
            except Exception as exp:
                _LOGGER.exception(
                    "Error occurred while discovering devices: %s",
                    exp)

            # Schedule again
            next = next_discover_time()
            _LOGGER.debug("Scheduling to discover at %s", next)
            track_point_in_time(hass, discover_action, next)

        # Start discovery
        _LOGGER.debug("Discovering new sensors now")
        discover_action(None)

    # Finish setting up
    add_devices(sensors)


def discover(client, devices, add_devices, config_sensor):
    # Send a message to discover new devices
    responses = client.multicast_discover()

    _LOGGER.debug("Processing discovered sensors (%s):", len(responses))
    for response in responses:
        # TODO: I should actually parse the response and not just match
        if b'</air_quality>' not in response.payload:
            # It's not a dylos sensor
            continue

        # Get the hostname
        m = re.search("</name=(.*?)>", response.payload.decode('utf8'))
        if m is None:
            _LOGGER.warning("Couldn't find hostname in response: %s", response)
            continue

        name = m.group(1)
        address = address = response.source[0]
        _LOGGER.debug("Found device: %s - %s", address, name)

        if address in devices:
            _LOGGER.debug("Dylos device has already been discovered")
            continue

        # Add the new device to home assistant
        sensors = []
        callbacks = []

        _LOGGER.debug("Adding %s to home assistant", address)
        for sensor in config_sensor:
            dylos_sensor = DylosSensor(name, sensor)
            sensors.append(dylos_sensor)
            callbacks.append(dylos_sensor.update)


        devices[address] = DylosDevice(address,
                                       name,
                                       callbacks)
        add_devices(sensors)



def get_data(device, batch_size, max_data_transferred):
    _LOGGER.debug("Getting new data from %s (%s) at %s",
                  device.name,
                  device.address,
                  datetime.now())

    try:
        data = None
        total_packets = 0

        while True:
            _LOGGER.debug("ACKing %s and requesting %s (%s - %s)",
                          device.ack,
                          batch_size,
                          device.name,
                          device.address)
            payload = struct.pack('!HH', device.ack, batch_size)
            response = device.client.get('air_quality', payload=payload)

            if response is None:
                device.ack = 0
                _LOGGER.debug(
                    "Did not receive a response from sensor %s - %s",
                    device.name, device.address)
                break

            if len(response.payload) == 0:
                device.ack = 0
                _LOGGER.debug(
                    "Received an empty payload from %s - %s",
                    device.name, device.address)
                break

            data = msgpack.unpackb(response.payload, use_list=False)
            _LOGGER.debug("Received data (%s): %s (%s - %s - %s)",
                          len(data),
                          data,
                          device.name,
                          device.address,
                          response.mid)

            device.ack = len(data)
            total_packets += device.ack

            keys = ['humidity', 'large', 'sampletime',
                    'sequence', 'small', 'temperature']
            now = time.time()

            # For each new piece of data, notify everyone that has
            # registered a callback
            for d in data:
                # Make sure data matches the number of keys expected
                if len(keys) != len(d):
                    _LOGGER.warning(
                        "Data does not match the number of keys. Ignoring: %s",
                        d)
                    continue

                # Transform data into a dict
                d = dict(zip(keys, d))

                # Make sure the timestamp makes sense
                if abs(now - d['sampletime']) >= SECONDS_IN_A_YEAR:
                    _LOGGER.warning(
                        "Sample time is too far off: %s. Ignoring data: %s",
                        d['sampletime'],
                        d)

                _LOGGER.debug("Calling callbacks for %s - %s on %s",
                              device.name,
                              device.address,
                              d)
                for cb in device.callbacks:
                    cb(d)
                    time.sleep(.1)

            # If we get all of the data we ask for, then let's request more
            # right away
            if device.ack != batch_size:
                _LOGGER.debug(
                    "%s - %s: Stopping because acks (%s) != size (%s)",
                    device.name, device.address, device.ack, batch_size)
                time.sleep(5)
                break

            # Let's give the system some time to catch up
            # We will try again after CONF_UPDATE_TIME amount of time
            if total_packets >= max_data_transferred:
                _LOGGER.debug(
                    "%s - %s: Stopping because total_packets (%s) > %s",
                    device.name, device.address, total_packets, max_data_transferred)
                time.sleep(5)
                break

    except Exception:
        device.ack = 0
        _LOGGER.exception(
            "Unable to receive data or unpack data: %s (%s - %s)",
            data, device.name, device.address)


class DylosDevice(object):
    def __init__(self, address, name, callbacks):
        self.address = address
        self.name = name
        self.callbacks = callbacks
        self.ack = 0

        self.client = Client(server=(address, 5683))


class DylosSensor(Entity):
    def __init__(self, monitor_name, sensor_name):
        self._monitor_name = monitor_name
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
        return '{} {}'.format(self._monitor_name, self._name)

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

    def get(self, path, payload=None):  # pragma: no cover
        request = Request()
        request.destination = self.server
        request.code = defines.Codes.GET.number
        request.uri_path = path
        request.payload = payload

        # Clear out queue before sending a request. It is possible that an old
        # response was received between requests. We don't want the requests
        # and responses to be mismatched. I expect the protocol to take care of
        # that, but I don't have confidence in the CoAP library.
        try:
            while True:
                self.queue.get_nowait()
        except Empty:
            pass

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
        first_response = self.queue.get(block=True)

        if first_response is None:
            # The message timed out
            return []

        responses = [first_response]
        try:
            # Keep trying to get more responses if they come in
            while True:
                responses.append(self.queue.get(block=True, timeout=10))
        except Empty:
            pass

        return responses

