"""
A component which allows you to send data to an Influx database.

For more details about this component, please refer to the documentation at
https://home-assistant.io/components/influxdb/
"""
from datetime import timedelta
import functools
import logging
import itertools
import json

from persistent_queue import PersistentQueue
import requests
import voluptuous as vol

from homeassistant.const import (EVENT_STATE_CHANGED, STATE_UNAVAILABLE,
                                 STATE_UNKNOWN, EVENT_HOMEASSISTANT_STOP)
from homeassistant.helpers import state as state_helper
from homeassistant.helpers import template
import homeassistant.helpers.config_validation as cv
from homeassistant.helpers.event import track_point_in_time
import homeassistant.util.dt as dt_util

_LOGGER = logging.getLogger(__name__)

DOMAIN = "prisms_influxdb"
DEPENDENCIES = []

DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 8086
DEFAULT_DATABASE = 'home_assistant'
DEFAULT_SSL = False
DEFAULT_VERIFY_SSL = False
DEFAULT_BATCH_TIME = 0
DEFAULT_CHUNK_SIZE = 500

REQUIREMENTS = ['influxdb==3.0.0', 'python-persistent-queue==1.3.0']

CONF_HOST = 'host'
CONF_DEPLOYMENT_ID = 'home_id'
CONF_PORT = 'port'
CONF_DB_NAME = 'database'
CONF_USERNAME = 'username'
CONF_PASSWORD = 'password'
CONF_SSL = 'ssl'
CONF_VERIFY_SSL = 'verify_ssl'
CONF_BLACKLIST = 'blacklist'
CONF_WHITELIST = 'whitelist'
CONF_TAGS = 'tags'
CONF_BATCH_TIME = 'batch_time'
CONF_CHUNK_SIZE = 'chunk_size'

CONFIG_SCHEMA = vol.Schema({
    DOMAIN: vol.Schema({
        vol.Required(CONF_HOST): cv.string,
        vol.Required(CONF_DEPLOYMENT_ID): cv.string,
        vol.Optional(CONF_PORT, default=DEFAULT_PORT): cv.positive_int,
        vol.Optional(CONF_DB_NAME, default=DEFAULT_DATABASE): cv.string,
        vol.Optional(CONF_USERNAME, default=None): vol.Any(cv.string, None),
        vol.Optional(CONF_PASSWORD, default=None): vol.Any(cv.string, None),
        vol.Optional(CONF_SSL, default=DEFAULT_SSL): cv.boolean,
        vol.Optional(CONF_VERIFY_SSL,
                     default=DEFAULT_VERIFY_SSL): cv.boolean,
        vol.Optional(CONF_BLACKLIST, default=[]): cv.ensure_list,
        vol.Optional(CONF_WHITELIST, default=[]): cv.ensure_list,
        vol.Optional(CONF_TAGS, default={}): dict,
        vol.Optional(CONF_BATCH_TIME,
                     default=DEFAULT_BATCH_TIME): cv.positive_int,
        vol.Optional(CONF_CHUNK_SIZE,
                     default=DEFAULT_CHUNK_SIZE): cv.positive_int,
    })
}, extra=vol.ALLOW_EXTRA)

RUNNING = True

# pylint: disable=too-many-locals
def setup(hass, config):
    """Setup the InfluxDB component."""
    from influxdb import InfluxDBClient

    conf = config[DOMAIN]
    blacklist = conf[CONF_BLACKLIST]
    whitelist = conf[CONF_WHITELIST]
    tags = conf[CONF_TAGS]
    batch_time = conf[CONF_BATCH_TIME]
    chunk_size = conf[CONF_CHUNK_SIZE]

    tags[CONF_DEPLOYMENT_ID] = conf[CONF_DEPLOYMENT_ID]

    influx = InfluxDBClient(host=conf[CONF_HOST],
                            port=conf[CONF_PORT],
                            username=conf[CONF_USERNAME],
                            password=conf[CONF_PASSWORD],
                            database=conf[CONF_DB_NAME],
                            ssl=conf[CONF_SSL],
                            verify_ssl=conf[CONF_VERIFY_SSL])

    events = PersistentQueue('prisms_influxdb.queue',
                             path=hass.config.config_dir)
    render = functools.partial(get_json_body, hass=hass, tags=tags)

    def influx_event_listener(event):
        """Listen for new messages on the bus and sends them to Influx."""
        state = event.data.get('new_state')
        if state is None or state.state in (
                STATE_UNKNOWN, '', STATE_UNAVAILABLE) or \
                state.entity_id in blacklist:
            # The state is unknown or it is on the black list
            return

        if len(whitelist) > 0 and state.entity_id not in whitelist:
            # It is not on the white list
            return

        if batch_time == 0:
            # Since batch time hasn't been set, just upload as soon as an event
            # occurs
            try:
                _LOGGER.debug("Since batch_time == 0, writing data")
                json_body = render(event)
                write_data(influx, json_body)
            except ValueError as e:
                _LOGGER.error("Something is wrong with the provided template: %s", e)
                return
        else:
            # Convert object to pickle-able. Since State.attributes uses
            # MappingProxyType, it is not pickle-able
            if event.data['new_state']:
                event.data['new_state'].attributes = dict(event.data['new_state'].attributes)

            if event.data['old_state']:
                event.data['old_state'].attributes = dict(event.data['old_state'].attributes)

            # Store event to be uploaded later
            events.push(event)
            _LOGGER.debug("Saving event for later (%s)", len(events))

    hass.bus.listen(EVENT_STATE_CHANGED, influx_event_listener)

    if batch_time != 0:
        # Set up task to upload batch data
        _LOGGER.debug("Starting task to upload batch data")
        write_batch_data(hass, events, influx, render, batch_time, chunk_size)

    def stop(event):
        global RUNNING
        _LOGGER.info("Shutting down PRISMS InfluxDB component")
        RUNNING = False

    # Register to know when home assistant is stopping
    hass.bus.listen_once(EVENT_HOMEASSISTANT_STOP, stop)

    return True


def write_data(influx, json_body):
    from influxdb import exceptions

    try:
        influx.write_points(json_body)
    except requests.exceptions.RequestException as e:
        _LOGGER.exception('Unable to connect to database: %s', e)
        return False
    except exceptions.InfluxDBClientError as e:
        error = json.loads(e.content)['error']
        _LOGGER.exception('Error saving event "%s": %s', str(json_body)[:1000], error)
        return False
    except exceptions.InfluxDBServerError as e:
        _LOGGER.exception('Error saving event "%s" to InfluxDB: %s', str(json_body)[:1000], e)
        return False
    except Exception:  # Catch anything else
        _LOGGER.exception("An unknown exception happened while uploading data")
        return False

    return True


def write_batch_data(hass, events, influx, render, batch_time, chunk_size):
    def next_time():
        return dt_util.now() + timedelta(seconds=batch_time)

    def action(now):
        while RUNNING:
            _LOGGER.info("Trying to upload data")

            if len(events) == 0:
                # No more events to upload
                _LOGGER.info("Nothing to upload")
                break

            events_chunk = events.peek(chunk_size)
            size = len(events_chunk)
            _LOGGER.info("Uploading chunk of size %s (%s)", size, len(events))

            try:
                # Render and write events
                data = itertools.chain(*[render(event) for event in events_chunk])
                result = write_data(influx, list(data))
            except ValueError as e:
                _LOGGER.error("Something is wrong with the provided template: %s", e)
                return

            if result:
                # Chunk got saved so remove events
                _LOGGER.info("Data was uploaded successfully so deleting data")
                events.delete(size)

                if size < chunk_size:
                    _LOGGER.debug("Finished uploading data because size <"
                                  " chunk_size: %s < %s (%s)", size,
                                  chunk_size, len(events))
                    break

            else:
                # Unable to write data so give up for now
                _LOGGER.error("Error while trying to upload data. Trying again later")
                break

        if RUNNING:
            _LOGGER.debug("Flushing all events that were deleted")
            events.flush()

            # Schedule again
            next = next_time()
            _LOGGER.info("Scheduling to upload data at %s", next)
            track_point_in_time(hass, action, next)

    # Start the action
    next = next_time()
    _LOGGER.info("Scheduling to upload data at %s", next)
    track_point_in_time(hass, action, next)


def get_json_body(event, hass, tags):
    state = event.data.get('new_state')

    try:
        _state = float(state_helper.state_as_number(state))
        _state_key = "value"
    except ValueError:
        _state = state.state
        _state_key = "state"

    measurement = state.attributes.get('unit_of_measurement')
    if measurement in (None, ''):
        measurement = state.entity_id

    event_time = state.attributes.get('sample_time', event.time_fired)

    json_body = [
        {
            'measurement': measurement,
            'tags': {
                'domain': state.domain,
                'entity_id': state.object_id,
            },
            'time': event_time,
            'fields': {
                _state_key: _state,
            }
        }
    ]

    for tag in tags:
        json_body[0]['tags'][tag] = tags[tag]

    return json_body
