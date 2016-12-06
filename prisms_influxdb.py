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
                                 STATE_UNKNOWN, CONF_VALUE_TEMPLATE)
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
DEFAULT_CHUNK_SIZE = 1000

REQUIREMENTS = ['influxdb==3.0.0', 'python-persistent-queue==1.3.0']

CONF_HOST = 'host'
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
        vol.Optional(CONF_VALUE_TEMPLATE, default=None): cv.template,
        vol.Optional(CONF_BATCH_TIME,
                     default=DEFAULT_BATCH_TIME): cv.positive_int,
        vol.Optional(CONF_CHUNK_SIZE,
                     default=DEFAULT_CHUNK_SIZE): cv.positive_int,
    })
}, extra=vol.ALLOW_EXTRA)


# pylint: disable=too-many-locals
def setup(hass, config):
    """Setup the InfluxDB component."""
    from influxdb import InfluxDBClient

    conf = config[DOMAIN]
    blacklist = conf[CONF_BLACKLIST]
    whitelist = conf[CONF_WHITELIST]
    tags = conf[CONF_TAGS]
    value_template = conf[CONF_VALUE_TEMPLATE]
    batch_time = conf[CONF_BATCH_TIME]
    chunk_size = conf[CONF_CHUNK_SIZE]

    influx = InfluxDBClient(host=conf[CONF_HOST],
                            port=conf[CONF_PORT],
                            username=conf[CONF_USERNAME],
                            password=conf[CONF_PASSWORD],
                            database=conf[CONF_DB_NAME],
                            ssl=conf[CONF_SSL],
                            verify_ssl=conf[CONF_VERIFY_SSL])

    events = PersistentQueue('prisms_influxdb.queue',
                             path=hass.config.config_dir)
    render = functools.partial(get_json_body, hass=hass, tags=tags,
                               value_template=value_template)

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
            except json.decoder.JSONDecodeError as e:
                # Something is wrong with the template. This error can't be
                # fixed without restarting HA. Remove handler so we don't keep trying.
                _LOGGER.error("Something is wrong with the provided template: %s", e)
                hass.bus.listen(EVENT_STATE_CHANGED, influx_event_listener)
                return
        else:
            # Store event to be uploaded later
            _LOGGER.debug("Saving event for later")

            # Convert object to pickle-able. Since State.attributes uses
            # MappingProxyType, it is not pickle-able
            if event.data['new_state']:
                event.data['new_state'].attributes = dict(event.data['new_state'].attributes)

            if event.data['old_state']:
                event.data['old_state'].attributes = dict(event.data['old_state'].attributes)

            events.push(event)

    hass.bus.listen(EVENT_STATE_CHANGED, influx_event_listener)

    if batch_time != 0:
        # Set up task to upload batch data
        _LOGGER.debug("Starting task to upload batch data")
        write_batch_data(hass, events, influx, render, batch_time, chunk_size,
                         influx_event_listener)

    return True


def write_data(influx, json_body):
    from influxdb import exceptions

    try:
        influx.write_points(json_body)
    except requests.exceptions.RequestException as e:
        _LOGGER.error('Unable to connect to database: %s', e)
        return False
    except exceptions.InfluxDBClientError as e:
        error = json.loads(e.content)['error']
        _LOGGER.error('Error saving event "%s": %s', json_body, error)
        return False
    except exceptions.InfluxDBServerError as e:
        _LOGGER.error('Error saving event "%s" to InfluxDB: %s', json_body, e)
        return False

    return True


def write_batch_data(hass, events, influx, render, batch_time, chunk_size, event_listener):
    def next_time():
        return dt_util.now() + timedelta(seconds=batch_time)

    def action(now):
        while True:
            _LOGGER.debug("Trying to upload data")

            if len(events) == 0:
                # No more events to upload
                _LOGGER.debug("Nothing to upload")
                break

            size = min(len(events), chunk_size)
            events_chunk = events.peek(size)
            _LOGGER.debug("Uploading chunk of size %s", size)

            try:
                # Render and write events
                data = itertools.chain(*[render(event) for event in events_chunk])
                result = write_data(influx, list(data))
            except json.decoder.JSONDecodeError as e:
                # Something is wrong with the template. This error can't be
                # fixed without restarting HA. Remove handler so we don't keep trying.
                _LOGGER.error("Something is wrong with the provided template: %s", e)
                hass.bus.listen(EVENT_STATE_CHANGED, event_listener)
                return

            if result:
                # Chunk got saved so remove events
                _LOGGER.debug("Data was uploaded successfully so deleting data")
                events.delete(size)
                events.flush()

                if size <= chunk_size:
                    _LOGGER.debug("Finished uploading data because size <= than"
                                  " chunk_size: %s < %s (%s)", size,
                                  chunk_size, len(events))
                    break

            else:
                # Unable to write data so give up for now
                _LOGGER.debug("Error while trying to upload data. Trying again later")
                break

        # Schedule again
        track_point_in_time(hass, action, next_time())

    # Start the action
    track_point_in_time(hass, action, next_time())


def get_json_body(event, hass, tags, value_template):
    state = event.data.get('new_state')

    try:
        _state = state_helper.state_as_number(state)
    except ValueError:
        _state = state.state

    measurement = state.attributes.get('unit_of_measurement')
    if measurement in (None, ''):
        measurement = state.entity_id

    if value_template is None:
        json_body = [
            {
                'measurement': measurement,
                'tags': {
                    'domain': state.domain,
                    'entity_id': state.object_id,
                },
                'time': event.time_fired,
                'fields': {
                    'value': _state,
                }
            }
        ]
    else:
        json_body = template.render(hass, value_template, event=event,
                                    measurement=measurement, state=state,
                                    state_value=_state)

        try:
            json_body = json.loads(json_body)
        except json.decoder.JSONDecodeError as e:
            _LOGGER.error('%s does not result in valid json: %s: %s',
                          CONF_VALUE_TEMPLATE, e, json_body)
            raise e

    for tag in tags:
        json_body[0]['tags'][tag] = tags[tag]

    return json_body
