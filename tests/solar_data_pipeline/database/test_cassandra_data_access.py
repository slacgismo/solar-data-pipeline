import unittest
import os
from datetime import datetime
from datetime import timedelta
import numpy as np
from cassandra.cqlengine import connection
from cassandra.cqlengine.query import BatchQuery
from solar_data_pipeline.database.cassandra import CassandraDataAccess
from solar_data_pipeline.database.models.measurements import MeasurementRaw
from solar_data_pipeline.database.models.measurements import Geopoint

class TestCassandraDataAccess(unittest.TestCase):
    """
    This test depends on Cassandra database thus cannot be run in Continuous
    Integration Server, since Cassandra database doesn't have in-memory
    database as sqlite in-memory database that can be used for testing in place
    of other relational database such as PostgreSQL or MySQL.
    """

    def setUp(self):
        input_power_signals_file_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__),
            "../../fixtures/one_year_power_signals_1.csv"))
        with open(input_power_signals_file_path) as file:
            one_year_power_signals = np.loadtxt(file, delimiter=',')

        # Use only the first 5 days in order to reduce execution time:
        self._power_signals = one_year_power_signals[:, :5]
        # Temp:
        # import matplotlib.pyplot as plt
        # plt.imshow(self._power_signals)
        # plt.show()

        self._cassandra_ip_address = '127.0.0.1'

        connection.setup([self._cassandra_ip_address], "measurements")

        start_time = datetime(2019, 1, 1, 0, 0, 0)
        # self._raw_measurements = [
        #     MeasurementRaw.create(
        #         site = "SLACA0000001",
        #         meas_name = "ac_power",
        #         ts = start_time + timedelta(minutes=5*i*j),
        #         sensor = "000001-0A00-0001_ABC-1000p-AA-1",
        #         station = "001_inverter",
        #         company = "SLAC",
        #         lat_lon = Geopoint(latitude = 35.00000, longitude = -120.00000),
        #         meas_description = None,
        #         meas_status = True,
        #         meas_unit = "kW",
        #         meas_val_b = None,
        #         meas_val_f = value,
        #         meas_val_s = None)
        #     for i, daily_signal in enumerate(self._power_signals.T)
        #     for j, value in enumerate(daily_signal)
        # ]
        self._raw_measurements = []
        timedelta_value = 0
        for i, daily_signal in enumerate(self._power_signals.T):
            for j, value in enumerate(daily_signal):
                self._raw_measurements.append(
                    MeasurementRaw.create(
                        site = "SLACA0000001",
                        meas_name = "ac_power",
                        ts = start_time + timedelta(minutes=timedelta_value),
                        sensor = "000001-0A00-0001_ABC-1000p-AA-1",
                        station = "001_inverter",
                        company = "SLAC",
                        lat_lon = Geopoint(latitude = 35.00000, longitude = -120.00000),
                        meas_description = None,
                        meas_status = True,
                        meas_unit = "kW",
                        meas_val_b = None,
                        meas_val_f = value,
                        meas_val_s = None)
                )
                timedelta_value += 5

    def tearDown(self):
        for raw_measurement in self._raw_measurements:
            raw_measurement.delete()

    @unittest.skip("This test accesses Cassandra database.")
    def test_retrieve(self):
        """
        CassandraDataAccess accesses Cassandra database. Thus, this test
        will not be a part of continuous integration.
        """

        data_access = CassandraDataAccess(self._cassandra_ip_address)
        actual_data = data_access.retrieve()

        # Temp:
        # import matplotlib.pyplot as plt
        # plt.imshow(actual_data)
        # plt.show()
        # plt.plot(actual_data)
        # plt.show()
        # import sys
        # np.set_printoptions(threshold=sys.maxsize)
        # actual_data_shape = actual_data.shape
        # print("actual_data[0]: %s" % (actual_data[0]))
        # print("actual_data[0].meas_val_f: %s" % (actual_data[0].meas_val_f))
        # print("actual_data[0].ts: %s" % (actual_data[0].ts))

        expected_data = self._power_signals[:, :2]

        # Temps;
        # plt.imshow(expected_data)
        # plt.show()
        # plt.plot(expected_data)
        # plt.show()

        np.testing.assert_almost_equal(actual_data, expected_data, decimal=5)
