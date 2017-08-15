# PRISMS Custom Components

Custom components for PRISMS project to be used with [Home Assistant](https://home-assistant.io). These custom components are pre-installed when using the PRISMS gateway image.

## Install

To use, put these files inside the `custom_components` folder of your Home Assistant install (typically `~/.homeassistant/custom_components/`). You must also install the requirements for the modules. This can be done by running

```bash
pip install -r requirements.txt
```

Note: Make sure to use the same `pip` that Home Assistant is using.

### PRISMS Influxdb Component

This component takes care of uploading all data that comes into Home Assistant into InfluxDB. All data is stored persistently while uploading occurs in case of failure or power outage.

To configure this component add the following to your Home Assistant `configuration.yaml` file:

```
prisms_influxdb:
  host: <something-here>
  home_id: <something-here>
```

Configuration variables:

- **host** (_Required_): IP address or host name of your database, e.g. 192.168.1.10 or db.test.com
- **home_id** (_Required_): The ID of the home that the data is being uploaded from. This is used to identify all data that comes from that home.
- **port** (_Optional_): Port to use. Defaults to 8086.
- **database** (_Optional_): Name of the database to use. Defaults to `home_assistant`. The database must already exist.
- **username** (_Optional_): The username of the database user.
- **password** (_Optional_): The password for the database user account.
- **ssl** (_Optional_): Use https instead of http to connect. Defaults to false.
- **verify_ssl** (_Optional_): Verify SSL certificate for https request. Defaults to false.
- **blacklist** (_Optional_): List of entities that should not be logged to InfluxDB.
- **whitelist** (_Optional_): List of the entities (only) that will be logged to InfluxDB. If not set, all entities will be logged. Values set by the blacklist option will prevail.
- **tags** (_Optional_): Tags to mark the data.
- **batch_time** (_Optional_): The number of seconds between uploading data. This helps to group events together for more efficient uploads. Defaults to 10. Warning: If you set batch time to 0, data will not be saved persistently on disk.
- **chunk_size** (_Optional_): The maximum amount of events that will be uploaded at once. Defaults to 500.

Example configuration:

```
prisms_influxdb:
  host: database.utah.edu
  port: 443
  username: writer
  password: xyz1234
  home_id: home_001
  ssl: true
  verify_ssl: true
  batch_time: 10
  blacklist:
  - persistent_notification.invalid_config
```

### PRISMS WiFi Sensor Component

This component is a generic sensor that connects with a PRISMS WiFi sensor. Sensors should be automatically discovered when using this component.

To configure this component add the following to your Home Assistant `configuration.yaml` file:

```
sensor:
- platform: prisms_wifi_sensor
```

Configuration variables:

- **update_time** (_Optional_): How often data is pulled from sensors (in seconds). Default is 60 seconds.
- **discover_time** (_Optional_): How often to discover new monitors (in seconds). Default is 5 minutes.
- **device_cleanup_time** (_Optional_): How long before a sensor gets ignored if it hasn't responded to discovery requests or data requests (in seconds). Default is 1 day.
- **batch_size** (_Optional_): The amount of data points to request from a sensor. The default is 10 data points.
- **max_data_transferred** (_Optional_): The amount of total data points to request from a sensor before it moves on to the next sensor. The default is 120.
- **monitors** (_Optional_): If discovery does not work, monitor IP address can be provided as a list.

Example configuration:

```
sensor:
  - platform: prisms_wifi_sensor
    update_time: 5
    discover_time: 300
    batch_size: 6
    max_data_transferred: 30
```
