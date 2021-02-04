from pygeotemporal.sensors import SensorsApi
from pygeotemporal.datapoints import DatapointsApi
import pandas as pd
from datetime import datetime

class Analysis:
    host = None
    username = None
    password = None

    sensor_client = None
    datapoint_client = None

    def __init__(self):
        pass

    def get_sensors_by_geocode(self, geocode):
        self.sensorclient = SensorsApi(host=self.host,
                                       username=self.username,
                                       password=self.password)

        r = self.sensorclient.sensors_by_polygon(geocode)

        if r.status_code != 200:
            print("Failed to get sensors with status code %s" % r.status_code)

        sensors = r.json()['sensors']

        print("Downloaded %s sensors" % len(sensors))

        return sensors

    def get_sensors_by_sensor_ids(self, sensor_ids):

        self.sensorclient = SensorsApi(host=self.host,
                                       username=self.username,
                                       password=self.password)

        sensors = []

        for sensor_id in sensor_ids:

            sensor = self.sensorclient.sensor_get(sensor_id).json()['sensor']

            if sensor['max_end_time'] == 'N/A':
                print("Updating sensor statistics for sensor_id=%s" % sensor_id)
                self.sensorclient.sensor_statistics_post(sensor_id)

            sensors.append(sensor)

        return sensors




    def get_sensors_parameters(self, sensors):

        all_parameters = []

        for sensor in sensors:
            for parameter in sensor['parameters']:
                if parameter[-2:] == 'qc':
                    continue
                if parameter not in all_parameters:
                    all_parameters.append(parameter)

        return all_parameters

    def create_geocode(self, coordinates):

        geocode = ''
        for coord in coordinates:
            geocode += str(round(coord[1], 8)) + '%2C' + str(round(coord[0], 8)) + '%2C'
        geocode = geocode[:-3]

        return geocode

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

        print(len(datapoint_rows[0]), len( column_names))


        # datapoint_dataframe = pd.DataFrame(datapoint_rows, columns=column_names)
        # for param in column_names[3:]:
        #     datapoint_dataframe[param] = pd.to_numeric(datapoint_dataframe[param])
        #
        # return datapoint_dataframe


    def get_datapoints(self, sensors, since=None, until=None, sources=None, format="json", only_count=None):

        self.datapoint_client = DatapointsApi(host=self.host,
                                  username=self.username,
                                  password=self.password)



        all_datapoints = []

        # print(since, until, sources, format, only_count)


        for sensor in sensors:
            print("Downloading datapoints for sensor_id=%s" % sensor['id'])
            # if only_count is False:
            r = self.datapoint_client.get_datapoints(
                sensor_id=sensor['id'],
                since=since,
                until=until,
                sources=sources,
                format=format,
                onlyCount=only_count
            )

            if r.status_code != 200:
                print("Datapoints download for sensor %s failed with status code %s" % (sensor['id'], r.status_code))
                continue

            all_datapoints += r.json()
        print("Downloaded %s datapoints" % len(all_datapoints))

        return all_datapoints

    # def get_sensors_and_parameters(self, sensor_ids):
    #
    #     self.sensorclient = SensorsApi(host=self.host,
    #                                    username=self.username,
    #                                    password=self.password)
    #
    #     all_parameters = []
    #     all_sensors = []
    #
    #     for sensor_id in sensor_ids:
    #
    #         sensor = self.sensorclient.sensor_get(sensor_id).json()['sensor']
    #
    #         if sensor['max_end_time'] == 'N/A':
    #             print("Updating sensor statistics for sensor_id=%s" % sensor_id)
    #             self.sensorclient.sensor_statistics_post(sensor_id)
    #
    #         all_sensors.append(sensor)
    #
    #         for parameter in sensor['parameters']:
    #             if parameter[-2:] == 'qc':
    #                 continue
    #             if parameter not in all_parameters:
    #                 all_parameters.append(parameter)
    #
    #     print("Downloaded %s sensors" % len(all_sensors))
    #
    #     return all_parameters, all_sensors

    def create_sensor_dataframe(self, sensors):

        self.sensorclient = SensorsApi(host=self.host,
                                       username=self.username,
                                       password=self.password)

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
