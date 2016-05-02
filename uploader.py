"""
custom_components.uploader
~~~~~~~~~~~~~~~~~~~~~~~~~
"""
import time
import logging
import itertools
from datetime import timedelta
import requests

import homeassistant.util as util
from homeassistant.helpers import validate_config
from homeassistant.helpers.event import track_point_in_time

import homeassistant.util.dt as dt_util

_LOGGER = logging.getLogger(__name__)

DOMAIN = "uploader"
DEPENDENCIES = []
REQUIREMENTS = ['influxdb==2.12.0']

DEFAULT_INTERVAL = 120
DEFAULT_PORT = 8086
DEFAULT_DATABASE = 'home_assistant'
DEFAULT_SSL = False
DEFAULT_VERIFY_SSL = False
DEFAULT_REMOTE_RETRIES = 60
DEFAULT_REMOTE_RETRY_TIME = 30

CONF_INTERVAL = 'interval'
CONF_HOME_ID = 'home_id'

CONF_LOCAL_HOST = 'local_host'
CONF_LOCAL_PORT = 'local_port'
CONF_LOCAL_DB_NAME = 'local_database'
CONF_LOCAL_USERNAME = 'local_username'
CONF_LOCAL_PASSWORD = 'local_password'
CONF_LOCAL_SSL = 'local_ssl'
CONF_LOCAL_VERIFY_SSL = 'local_verify_ssl'

CONF_REMOTE_HOST = 'remote_host'
CONF_REMOTE_PORT = 'remote_port'
CONF_REMOTE_DB_NAME = 'remote_database'
CONF_REMOTE_USERNAME = 'remote_username'
CONF_REMOTE_PASSWORD = 'remote_password'
CONF_REMOTE_SSL = 'remote_ssl'
CONF_REMOTE_VERIFY_SSL = 'remote_verify_ssl'
CONF_REMOTE_RETRIES = "remote_retries"
CONF_REMOTE_RETRY_TIME = "remote_retry_time"


def setup(hass, config):
    """ Setup uploader component. """
    from influxdb import exceptions

    if not validate_config(config, {DOMAIN: [CONF_LOCAL_HOST, CONF_REMOTE_HOST,
                                             CONF_HOME_ID]}, _LOGGER):
        return False

    conf = config[DOMAIN]

    local_host = conf[CONF_LOCAL_HOST]
    local_port = util.convert(conf.get(CONF_LOCAL_PORT), int, DEFAULT_PORT)
    local_database = util.convert(
        conf.get(CONF_LOCAL_DB_NAME), str, DEFAULT_DATABASE)
    local_username = util.convert(conf.get(CONF_LOCAL_USERNAME), str)
    local_password = util.convert(conf.get(CONF_LOCAL_PASSWORD), str)
    local_ssl = util.convert(conf.get(CONF_LOCAL_SSL), bool, DEFAULT_SSL)
    local_verify_ssl = util.convert(
        conf.get(CONF_LOCAL_VERIFY_SSL), bool, DEFAULT_VERIFY_SSL)

    remote_host = conf[CONF_REMOTE_HOST]
    remote_port = util.convert(conf.get(CONF_REMOTE_PORT), int, DEFAULT_PORT)
    remote_database = util.convert(
        conf.get(CONF_REMOTE_DB_NAME), str, DEFAULT_DATABASE)
    remote_username = util.convert(conf.get(CONF_REMOTE_USERNAME), str)
    remote_password = util.convert(conf.get(CONF_REMOTE_PASSWORD), str)
    remote_ssl = util.convert(conf.get(CONF_REMOTE_SSL), bool, DEFAULT_SSL)
    remote_verify_ssl = util.convert(
        conf.get(CONF_REMOTE_VERIFY_SSL), bool, DEFAULT_VERIFY_SSL)
    remote_retries = util.convert(
        conf.get(CONF_REMOTE_RETRIES), int, DEFAULT_REMOTE_RETRIES)
    remote_retry_time = util.convert(
        conf.get(CONF_REMOTE_RETRY_TIME), int, DEFAULT_REMOTE_RETRY_TIME)
    home_id = conf[CONF_HOME_ID]
    interval = util.convert(conf.get(CONF_INTERVAL), int, DEFAULT_INTERVAL)

    try:
        _LOGGER.info("Connecting to remote database")
        uploader = Uploader(remote_host, remote_port, remote_database,
                            remote_username, remote_password, remote_ssl,
                            remote_verify_ssl, home_id)
    except exceptions.InfluxDBClientError as exc:
        _LOGGER.error("Remote database host is not accessible due to '%s', "
                      "please check your entries in the configuration file "
                      "and that the database exists and is READ/WRITE.", exc)
        # Since this is the remote database, there is nothing that can be done,
        # so just give up.
        return False
    except exceptions.InfluxDBServerError as exc:
        _LOGGER.error("Unable to connect with server: %s", exc)
        # Since this is the remote database, there is nothing that can be done,
        # so just give up.
        return False
    except requests.exceptions.RequestException as exc:
        _LOGGER.error("Unable to connect to remote database: %s", exc)
        # Since this is the remote database, there is nothing that can be done,
        # so just give up.
        return False

    # Sometimes the local InfluxDB takes awhile to start up. We will try a
    # few times before giving up.
    _LOGGER.info("Connecting to local database")
    for i in range(remote_retries):
        _LOGGER.info("Trying %s out of %s", i + 1, remote_retries)
        try:
            downloader = Downloader(local_host, local_port, local_database,
                                    local_username, local_password, local_ssl,
                                    local_verify_ssl)
            break
        except exceptions.InfluxDBClientError as exc:
            _LOGGER.warn("Local database host is not accessible due to '%s', "
                         "please check your entries in the configuration file "
                         "and that the database exists and is READ/WRITE.",
                         exc)
        except requests.exceptions.RequestException as exc:
            _LOGGER.warn("Unable to connect to local database: %s", exc)

        _LOGGER.info("Retrying again in %s seconds", remote_retry_time)
        time.sleep(remote_retry_time)

    else:
        # All of the retries didn't work, so fail
        _LOGGER.error("Unable to connect to database or the database is not"
                      " accessible after %s retries (%s second(s) apart).",
                      remote_retries, remote_retry_time)
        return False

    def next_time():
        return dt_util.now() + timedelta(seconds=interval)

    def action(now):
        try:
            data = downloader.get_data()
        except exceptions.InfluxDBClientError as exc:
            data = None
            _LOGGER.error(
                "Exception while getting data from local database: %s", exc)
        except requests.exceptions.RequestException as exc:
            data = None
            _LOGGER.error("Unable to connect to local database: %s", exc)

        try:
            # Make sure there is data to upload
            if data is None or 'series' not in data.raw:
                _LOGGER.info("No data to upload")
            else:
                last = uploader.upload_data(data)
                downloader.last_time = last
        except exceptions.InfluxDBClientError as exc:
            _LOGGER.error(
                "Exception while uploading data to remote database: %s", exc)
        except exceptions.InfluxDBServerError as exc:
            _LOGGER.error(
                "Exception while uploading data to remote database: %s", exc)
        except requests.exceptions.RequestException as exc:
            _LOGGER.error("Unable to connect to remote database: %s", exc)

        track_point_in_time(hass, action, next_time())

    track_point_in_time(hass, action, next_time())
    return True


class Downloader:
    def __init__(self, host, port, database, username, password, ssl,
                 verify_ssl):
        from influxdb import InfluxDBClient

        self.client = InfluxDBClient(host=host,
                                     port=port,
                                     username=username,
                                     password=password,
                                     database=database,
                                     ssl=ssl,
                                     verify_ssl=verify_ssl)
        # Make sure client can connect
        self.client.query("select * from /.*/ LIMIT 1;")
        self.database = database
        self._last_time = None

    @property
    def last_time(self):
        if self._last_time is not None:
            return self._last_time

        # Check to see if the value is already in the database
        last_time = self.client.query(
            "select value from last_time")
        _LOGGER.info("last_downloaded: %s", last_time)

        if len(last_time) == 0:
            # If not, use the oldest value in the database
            _LOGGER.warning(
                "last_time value not found! Using 0 as the oldest value")
            self._last_time = 0
        else:
            assert len(last_time) == 1
            self._last_time = list(
                last_time.get_points('last_time'))[0]['value']

        return self._last_time

    @last_time.setter
    def last_time(self, value):
        # Save the value
        self._last_time = value

        # Also record it in case uploader stops
        self.client.write_points([{
            "measurement": "last_time",
            "time": 0,
            "fields": {
                "value": value
            }
        }])

    def get_data(self):
        _LOGGER.info("Getting data (time > {})".format(self.last_time))

        GET_QUERY = "select * from /.*/ WHERE time > {}"
        data = self.client.query(GET_QUERY.format(self.last_time),
                                 epoch='ns')
        return data


class Uploader:
    def __init__(self, host, port, database, username, password, ssl,
                 verify_ssl, home_id):
        from influxdb import InfluxDBClient, exceptions

        self.client = InfluxDBClient(host=host,
                                     port=port,
                                     username=username,
                                     password=password,
                                     database=database,
                                     ssl=ssl,
                                     verify_ssl=verify_ssl)
        self.home_id = home_id

        # Make sure client can connect
        try:
            self.client.query("select * from /.*/ LIMIT 1;")
        except exceptions.InfluxDBClientError as exc:
            if exc.code == 401:
                # This is okay because we are trying to read from the database,
                # but we might only have write permissions. At least we know
                # that the database exists and we can connect to it.
                pass
            else:
                raise exc

    def upload_data(self, data):
        data = data.raw

        _LOGGER.info("Uploading data")
        formated_data = [self._format_data(**series)
                         for series in data['series']]
        formated_data = itertools.chain.from_iterable(formated_data)
        formated_data = list(formated_data)  # Necessary because of batch_size
        result = self.client.write_points(formated_data, batch_size=10000)

        if not result:
            from influxdb.exceptions import InfluxDBClientError
            _LOGGER.error("Unable to upload data to remote database")
            raise InfluxDBClientError

        last_time = formated_data[-1]['time']
        return last_time

    def _format_data(self, name, columns, values):
        time_index = columns.index('time')
        value_index = columns.index('value')
        tag_indexes = set(range(len(columns))) - set([time_index, value_index])

        for v in values:
            time = v[time_index]
            value = v[value_index]
            tags = {columns[i]: v[i] for i in tag_indexes}

            # Add home id to tags
            tags['home_id'] = self.home_id

            if value is None:
                # Skip over all None values
                continue

            # Try to convert value
            try:
                value = float(value)
            except (ValueError, TypeError):
                pass

            yield {"measurement": name,
                   "tags": tags,
                   "time": time,
                   "fields": {"value": value}}
