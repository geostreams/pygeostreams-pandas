from pygeostreams.sensors import SensorsApi
from pygeostreams.datapoints import DatapointsApi
import pandas as pd
from datetime import datetime


class ToPandas:
    host = None
    username = None
    password = None

    sensor_client = None
    datapoint_client = None

    def __init__(self, host, username, password):
        self.host = host
        self.username = username
        self.password = password

    # TODO: Needed?
    # TODO: Needed? Not used anywhere
    def create_geocode(self, coordinates):
        geocode = ''
        for coord in coordinates:
            geocode += str(round(coord[1], 8)) + '%2C' + str(round(coord[0], 8)) + '%2C'
        geocode = geocode[:-3]
        return geocode

    def get_sensors_by_geocode(self, geocode):
        if not self.sensorclient:
            self.sensorclient = SensorsApi(host=self.host, username=self.username, password=self.password)

        r = self.sensorclient.sensors_by_polygon(geocode)
        if r.status_code != 200:
            print("Failed to get sensors with status code %s" % r.status_code)
        sensors = r.json()['sensors']

        return self.create_sensor_dataframe(sensors)

    def get_sensors_by_sensor_ids(self, sensor_ids):
        if not self.sensorclient:
            self.sensorclient = SensorsApi(host=self.host, username=self.username, password=self.password)

        sensors = []
        for sensor_id in sensor_ids:
            sensor = self.sensorclient.sensor_get(sensor_id).json()['sensor']
            if sensor['max_end_time'] == 'N/A':
                print("Updating sensor statistics for sensor_id=%s" % sensor_id)
                self.sensorclient.sensor_statistics_post(sensor_id)
            sensors.append(sensor)

        return self.create_sensor_dataframe(sensors)

    # TODO: Needed? Not used anywhere
    def get_sensors_parameters(self, sensors):
        all_parameters = []
        for sensor in sensors:
            for parameter in sensor['parameters']:
                if parameter[-2:] == 'qc':
                    continue
                if parameter not in all_parameters:
                    all_parameters.append(parameter)
        return all_parameters

    # Create dataframe from list of sensor objects
    def create_sensor_dataframe(self, sensors):
        sensor_rows = []
        for sensor in sensors:
            row = []
            row.append(sensor['id'])
            row.append(sensor['name'])
            if 'type' in sensor['properties'] and 'network' in sensor['properties']['type']:
                row.append(sensor['properties']['type']['network'])
            else:
                row.append('')
            row.append(sensor['min_start_time'])
            row.append(sensor['max_end_time'])
            row.append(sensor['geometry']['coordinates'][1])
            row.append(sensor['geometry']['coordinates'][0])
            if 'huc_name' in sensor['properties']['huc']:
                row.append(sensor['properties']['huc']['huc_name'])
            else:
                row.append('')
            row.append(sensor['properties']['huc']['huc8']['code'])

            sensor_rows.append(row)

        sensors_dataframe = pd.DataFrame(sensor_rows, columns=['SENSOR_ID',
                                                               'NAME',
                                                               'NETWORK',
                                                               'DATA_START',
                                                               'DATA_END',
                                                               'LATITUDE',
                                                               'LONGITUDE',
                                                               'HUC NAME',
                                                               'HUC8'])
        return sensors_dataframe

    # Get datapoints as Pandas dataframe
    def get_datapoints(self, sensors, since=None, until=None, sources=None, format="json", only_count=None):
        if not self.datapoint_client:
            self.datapoint_client = DatapointsApi(host=self.host, username=self.username, password=self.password)

        all_datapoints = []
        sensor_parameters = []

        for sensor in sensors:
            print("Downloading datapoints for sensor_id=%s" % sensor['id'])
            r = self.datapoint_client.get_datapoints(
                sensor_id=sensor['id'], since=since, until=until, sources=sources, format=format, onlyCount=only_count)
            if r.status_code != 200:
                print("Datapoints download for sensor %s failed with status code %s" % (sensor['id'], r.status_code))
                continue
            all_datapoints += r.json()

            for parameter in sensor['parameters']:
                if parameter[-2:] == 'qc':
                    continue
                if parameter not in sensor_parameters:
                    sensor_parameters.append(parameter)

        return self.create_datapoints_dataframe(all_datapoints, sensor_parameters)

    # Convert a list of datapoints to Pandas dataframe
    def create_datapoints_dataframe(self, datapoints, sensors_parameters):
        column_names = ['sensor_id', 'datetime', 'created'] + sensors_parameters
        datapoint_rows = []
        for datapoint in datapoints:
            row = []
            row.append(datapoint['sensor_id'])
            row.append(datetime.strptime(datapoint['start_time'][:-4].replace('"T"', 'T'), '%Y-%m-%dT%H:%M'))
            row.append(datetime.strptime(datapoint['created'][:-4].replace('"T"', 'T'), '%Y-%m-%dT%H:%M'))

            for prop in sensors_parameters:
                if prop == "site":
                    continue
                if prop in datapoint['properties']:
                    row.append(datapoint['properties'][prop])
                else:
                    row.append('')
            datapoint_rows.append(row)

        datapoint_dataframe = pd.DataFrame(datapoint_rows, columns=column_names)
        for param in column_names[3:]:
            datapoint_dataframe[param] = pd.to_numeric(datapoint_dataframe[param])
        return datapoint_dataframe
