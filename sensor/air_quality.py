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
from homeassistant.const import CONF_HOST, CONF_NAME, EVENT_HOMEASSISTANT_STOP
import homeassistant.helpers.config_validation as cv
from homeassistant.helpers.entity import Entity
from homeassistant.helpers.event import track_point_in_time
import homeassistant.util.dt as dt_util


_LOGGER = logging.getLogger(__name__)

DEPENDENCIES = []
REQUIREMENTS = ['msgpack-python==0.4.8', 'CoAPy==4.1.5']

CONF_UPDATE_TIME = 'update_time'
CONF_DISCOVER_TIME = 'discover_time'
CONF_DEVICE_CLEANUP_TIME = 'device_cleanup_time'
CONF_BATCH_SIZE = 'batch_size'
CONF_MAX_DATA_TRANSFERRED = 'max_data_transferred'
CONF_MONITORS = 'monitors'

SECONDS_IN_A_YEAR = 31536000

SENSORS = {
    'dylos':   ['humidity', 'large', 'sampletime', 'sequence', 'small',
                'temperature'],
    'dylos-2': ['associated', 'data_rate', 'humidity', 'invalid_misc', 'large',
                'link_quality', 'local_ping_errors', 'local_ping_latency',
                'local_ping_packet_loss', 'local_ping_total', 'noise_level',
                'queue_length', 'remote_ping_errors', 'remote_ping_latency',
                'remote_ping_packet_loss', 'remote_ping_total',
                'rx_invalid_crypt', 'rx_invalid_frag', 'rx_invalid_nwid',
                'sampletime', 'sequence', 'signal_level', 'small',
                'temperature', 'tx_retires'],
    'airu':    ['humidity', 'pm1', 'pm10', 'pm25', 'sampletime', 'sequence',
                'temperature'],
}
SENSOR_TYPES = {
    'associated': 'associated',
    'data_rate': 'Mbps',
    'data_points_received': 'num',
    'humidity': '%',
    'invalid_misc': 'num',
    'large': 'pm',
    'link_quality': 'num',
    'local_ping_errors': 'num',
    'local_ping_latency': 'ms',
    'local_ping_packet_loss': 'num',
    'local_ping_total': 'num',
    'noise_level': 'dBm',
    'pm1': 'ug/m3',
    'pm10': 'ug/m3',
    'pm25': 'ug/m3',
    'queue_length': 'num',
    'remote_ping_errors': 'num',
    'remote_ping_latency': 'ms',
    'remote_ping_packet_loss': 'num',
    'remote_ping_total': 'num',
    'rx_invalid_crypt': 'num',
    'rx_invalid_frag': 'num',
    'rx_invalid_nwid': 'num',
    'sampletime': 's',
    'sequence': 'sequence',
    'signal_level': 'dBm',
    'small': 'pm',
    'temperature': '°C',
    'tx_retires': 'num',
}

RUNNING = True

PLATFORM_SCHEMA = PLATFORM_SCHEMA.extend({
    vol.Optional(CONF_UPDATE_TIME,
                 default=timedelta(seconds=60)): cv.time_period,
    vol.Optional(CONF_DISCOVER_TIME,
                 default=timedelta(minutes=5)): cv.time_period,
    vol.Optional(CONF_DEVICE_CLEANUP_TIME,
                 default=timedelta(days=1)): cv.time_period,
    vol.Optional(CONF_BATCH_SIZE, default=10): cv.positive_int,
    vol.Optional(CONF_MAX_DATA_TRANSFERRED, default=120): cv.positive_int,
    vol.Optional(CONF_MONITORS, default=[]):
        vol.All(cv.ensure_list, [cv.string]),
})

def setup_platform(hass, config, add_devices, discovery_info=None):
    def next_data_time():
        return dt_util.now() + config[CONF_UPDATE_TIME]

    def next_discover_time():
        return dt_util.now() + config[CONF_DISCOVER_TIME]

    # Device name => device
    devices = {}
    device_cleanup_time = config[CONF_DEVICE_CLEANUP_TIME]

    def data_action(now):
        now = dt_util.now()
        def active(device):
            if now - device.last_seen > device_cleanup_time:
                _LOGGER.warning("Skipping device %s - %s: Hasn't been "
                                "seen since %s", device.name, device.address,
                                device.last_seen)
                return False
            else:
                return True

        device_list = list(devices.values())
        device_list = [device for device in device_list if active(device)]

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

    # Connect to multicast address
    discover_client = Client(server=('224.0.1.187', 5683))
    provided_devices = config[CONF_MONITORS]

    def discover_action(now):
        try:
            discover(discover_client,
                     devices,
                     provided_devices,
                     add_devices)
        except Exception as exp:
            _LOGGER.exception(
                "Error occurred while discovering devices: %s",
                exp)

        # Schedule again
        next = next_discover_time()
        _LOGGER.debug("Scheduling to discover at %s", next)
        track_point_in_time(hass, discover_action, next)

    # Start discovery
    next = dt_util.now() + timedelta(seconds=5)
    _LOGGER.debug("Scheduling to discover at %s", next)
    track_point_in_time(hass, discover_action, next)

    def stop(event):
        global RUNNING

        RUNNING = False

        _LOGGER.info("Shutting down Air Quality component")
        if discover_client is not None:
            discover_client.stop()

        device_list = list(devices.values())
        for device in device_list:
            device.client.stop()

        _LOGGER.debug("Done shutting down Air Quality component")

    # Register to know when home assistant is stopping
    hass.bus.listen_once(EVENT_HOMEASSISTANT_STOP, stop)


def discover(discover_client, devices, provided_devices, add_devices):
    _LOGGER.info("Looking for new Air Quality devices")

    # Send a message to discover new devices
    responses = discover_client.multicast_discover()
    _LOGGER.debug("Finished multicast discovering devices (%s)", len(responses))

    # Discover clients that have been provided
    for device in provided_devices:
        _LOGGER.debug("Discovering %s", device)
        client = Client(server=(device, 5683))
        response = client.discover()
        if response is not None:
            responses.append(response)
        client.stop()

        if not RUNNING:
            _LOGGER.debug("Stop discovering new devices")
            return

    now = dt_util.now()

    _LOGGER.info("Processing discovered sensors (%s):", len(responses))
    for response in responses:
        if response is None:
            # This means that we are trying to exit in the middle of discovery
            break

        # TODO: I should actually parse the response and not just match
        if b'</air_quality>' not in response.payload:
            # It's not a sensor we care about
            continue

        # Get the sensor type
        m = re.search("</type=(.*?)>", response.payload.decode('utf8'))
        if m is None:
            _LOGGER.warning("Couldn't find type in response: %s", response)
            continue

        sensor_type = m.group(1)

        # Get the hostname
        m = re.search("</name=(.*?)>", response.payload.decode('utf8'))
        if m is None:
            _LOGGER.warning("Couldn't find hostname in response: %s", response)
            continue

        name = m.group(1)
        address = response.source[0]
        _LOGGER.info("\tFound device: %s - %s", name, address)

        if name in devices:
            _LOGGER.info("\tDevice has already been discovered")
            devices[name].last_seen = now

            if devices[name].address != address:
                _LOGGER.warning("\tAddress of device has changed!")
                devices[name].address = address

            continue

        if sensor_type not in SENSORS:
            _LOGGER.error("\t%s is not a recognized sensor type", sensor_type)
            continue

        # Add the new device to home assistant
        sensors = []
        callbacks = []

        _LOGGER.info("\tAdding %s (%s) to home assistant", name, address)
        for sensor in SENSORS[sensor_type]:
            air_quality_sensor = AirQualitySensor(name, sensor)
            sensors.append(air_quality_sensor)
            callbacks.append(air_quality_sensor.update)

        # Create a special sensor that keeps track of how many
        # packets are received from a sensor
        air_quality_sensor = AirQualitySensor(name, 'data_points_received')
        sensors.append(air_quality_sensor)

        if RUNNING:
            devices[name] = AirQualityDevice(address,
                                             name,
                                             sensor_type,
                                             callbacks,
                                             air_quality_sensor.update)
            add_devices(sensors)


def get_data(device, batch_size, max_data_transferred):
    _LOGGER.info("Getting new data from %s (%s) at %s",
                  device.name,
                  device.address,
                  dt_util.now())

    try:
        data = None
        total_packets = 0

        while True:
            _LOGGER.info("ACKing %s and requesting %s (%s - %s)",
                          device.ack,
                          batch_size,
                          device.name,
                          device.address)
            payload = struct.pack('!HH', device.ack, batch_size)
            response = device.client.get('air_quality', payload=payload)

            if response is None:
                device.ack = 0
                _LOGGER.info(
                    "Did not receive a response from sensor %s - %s",
                    device.name, device.address)
                break

            if len(response.payload) == 0:
                device.ack = 0
                _LOGGER.info(
                    "Received an empty payload from %s - %s",
                    device.name, device.address)
                break

            data = msgpack.unpackb(response.payload, use_list=False)
            _LOGGER.info("Received data from %s: %s samples", device.name, len(data))
            _LOGGER.debug("Data (%s): %s (%s - %s - %s)",
                          len(data),
                          data,
                          device.name,
                          device.address,
                          response.mid)

            device.last_seen = dt_util.now()
            device.ack = len(data)
            total_packets += device.ack

            keys = SENSORS[device.sensor_type]

            device.packet_received_callback({'data_points_received': len(data),
                                             'sequence': 0,
                                             'sampletime': time.time()})

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
                        "Sample time is too far off: %s. Data: %s",
                        d['sampletime'],
                        d)

                _LOGGER.debug("Calling callbacks for %s - %s on %s",
                              device.name,
                              device.address,
                              d)
                for cb in device.callbacks:
                    cb(d)
                    time.sleep(.05)

            # If we get all of the data we ask for, then let's request more
            # right away
            if device.ack != batch_size:
                _LOGGER.info(
                    "%s - %s: Stopping because acks (%s) != size (%s)",
                    device.name, device.address, device.ack, batch_size)
                time.sleep(1)
                break

            # Let's give the system some time to catch up
            # We will try again after CONF_UPDATE_TIME amount of time
            _LOGGER.info("%s (total_packets) >= %s (max_data_transferred)",
                          total_packets,
                          max_data_transferred)
            if total_packets >= max_data_transferred:
                _LOGGER.info(
                    "%s - %s: Stopping because total_packets (%s) > %s",
                    device.name, device.address, total_packets, max_data_transferred)
                time.sleep(1)
                break

    except Exception:
        device.ack = 0
        _LOGGER.exception(
            "Unable to receive data or unpack data: %s (%s - %s)",
            data, device.name, device.address)
        time.sleep(1)


class AirQualityDevice(object):
    def __init__(self, address, name, sensor_type, callbacks, packet_received_callback):
        self._address = address
        self.name = name
        self.sensor_type = sensor_type
        self.callbacks = callbacks
        self.packet_received_callback = packet_received_callback

        self.ack = 0
        self.last_seen = dt_util.now()
        self.client = Client(server=(address, 5683))

    @property
    def address(self):
        return self._address

    @address.setter
    def address(self, new_address):
        _LOGGER.debug("Updating address from {} to {}",
                      self._address,
                      new_address)
        self._address = new_address

        _LOGGER.debug("Stopping client of old address")
        self.client.stop()

        _LOGGER.debug("Creating a new client with new address")
        self.client = Client(server=(new_address, 5683))


class AirQualitySensor(Entity):
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
        self.running = True

    def _wait_response(self, message):
        if message.code != defines.Codes.CONTINUE.number:
            self.queue.put(message)

    def _timeout(self, message):
        _LOGGER.warning("Timed out trying to send message: %s", message)
        self.queue.put(None)

    def stop(self):
        self.running = False
        self.protocol.stop()
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

    def discover(self):
        request = Request()
        request.destination = self.server
        request.code = defines.Codes.GET.number
        request.uri_path = defines.DISCOVERY_URL

        try:
            while True:
                self.queue.get_nowait()
        except Empty:
            pass

        self.protocol.send_message(request)
        response = self.queue.get(block=True)
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
            while self.running:
                responses.append(self.queue.get(block=True, timeout=10))
        except Empty:
            pass

        return responses

