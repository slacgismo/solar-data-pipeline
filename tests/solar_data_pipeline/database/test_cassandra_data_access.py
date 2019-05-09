import unittest
import os
from datetime import datetime
from datetime import timedelta
import numpy as np
from cassandra.cqlengine import connection
from cassandra.cqlengine.query import BatchQuery
from cassandra.cqlengine.management import sync_table
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

    @classmethod
    def setUpClass(self):
        input_power_signals_file_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__),
            "../../fixtures/one_year_power_signals_1.csv"))
        with open(input_power_signals_file_path) as file:
            one_year_power_signals = np.loadtxt(file, delimiter=',')

        # Use only 6 days in order to reduce execution time:
        TestCassandraDataAccess._power_signals_site_1 = one_year_power_signals[
            :, :6]
        TestCassandraDataAccess._power_signals_site_2 = one_year_power_signals[
            :, 6:12]
        # Temp:
        # import matplotlib.pyplot as plt
        # plt.imshow(TestCassandraDataAccess._power_signals_site_1)
        # plt.show()

        TestCassandraDataAccess._cassandra_ip_address = '127.0.0.1'

        connection.setup([TestCassandraDataAccess._cassandra_ip_address],
                          "measurements")

        # Note: For now, populate database and keep the data.
        #       When commit, make sure to put back the code to clean database.
        # start_time = datetime(2019, 1, 1, 0, 0, 0)
        #
        # TestCassandraDataAccess._raw_measurements = []
        # TestCassandraDataAccess._populate_database(
        #     TestCassandraDataAccess._power_signals_site_1, "SLACA0000001",
        #     start_time)
        # TestCassandraDataAccess._populate_database(
        #     TestCassandraDataAccess._power_signals_site_2, "SLACA0000002",
        #     start_time)

    @classmethod
    def tearDownClass(self):
        # Note: For now, populate database and keep the data.
        #       When commit, make sure to put back the code to clean database.
        # for raw_measurement in TestCassandraDataAccess._raw_measurements:
        #     raw_measurement.delete()
        pass

    @classmethod
    def _populate_database(self, power_signals, site_name, start_time):
        timedelta_value = 0
        for i, daily_signal in enumerate(power_signals.T):
            for j, value in enumerate(daily_signal):
                TestCassandraDataAccess._raw_measurements.append(
                    MeasurementRaw.create(
                        site = site_name,
                        meas_name = "ac_power",
                        ts = start_time + timedelta(minutes=timedelta_value),
                        sensor = "000001-0A00-0001_ABC-1000p-AA-1",
                        station = "001_inverter",
                        company = "SLAC",
                        lat_lon = Geopoint(latitude = 35.00000,
                                           longitude = -120.00000),
                        meas_description = None,
                        meas_status = True,
                        meas_unit = "kW",
                        meas_val_b = None,
                        meas_val_f = value,
                        meas_val_s = None)
                )
                timedelta_value += 5

    def _assert_number_of_matching_daily_signals(self, actual, original_data,
        expected_number_of_data_match):
        # data_match = (actual == original_data)
        data_match = np.array([np.array_equal(actual[i],
                                 original_data[i])
                                 for i in range(len(original_data))])
        actual_number_of_data_match = np.sum(data_match)
        self.assertEqual(actual_number_of_data_match,
                         expected_number_of_data_match)

    # @unittest.skip("This test accesses Cassandra database." +
    # "Thus, this test will not be a part of continuous integration.")
    def test_find_sites(self):
        data_access = CassandraDataAccess(
            TestCassandraDataAccess._cassandra_ip_address)

        actual_sites = data_access.find_sites("SLACA0000001")
        expected_sites = np.array(["SLACA0000001"])
        np.testing.assert_array_equal(actual_sites, expected_sites)

        actual_nonexisting_sites = data_access.find_sites("SLACA9999999")
        expected_nonexisting_sites = np.array([])
        np.testing.assert_array_equal(actual_nonexisting_sites,
                                      expected_nonexisting_sites)

    # @unittest.skip("This test accesses Cassandra database." +
    # "Thus, this test will not be a part of continuous integration.")
    def test_get_sites(self):
        data_access = CassandraDataAccess(
            TestCassandraDataAccess._cassandra_ip_address)

        actual_sites = data_access.get_sites()
        expected_sites = np.array(["SLACA0000001", "SLACA0000002"])

        np.testing.assert_array_equal(actual_sites, expected_sites)

    @unittest.skip("This test accesses Cassandra database." +
    "Thus, this test will not be a part of continuous integration.")
    def test_retrieve(self):

        data_access = CassandraDataAccess(
            TestCassandraDataAccess._cassandra_ip_address)
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

        # Temps;
        # plt.imshow(expected_data)
        # plt.show()
        # plt.plot(expected_data)
        # plt.show()

        self._assert_number_of_matching_daily_signals(actual_data.T,
            TestCassandraDataAccess._power_signals_site_1.T, 3)
        self._assert_number_of_matching_daily_signals(actual_data.T,
            TestCassandraDataAccess._power_signals_site_2.T, 3)

    # @unittest.skip("This test accesses Cassandra database." +
    # "Thus, this test will not be a part of continuous integration.")
    def test_retrieve_for_date_range(self):

        start_time = datetime(2019, 1, 1, 0, 0, 0)
        end_time = datetime(2019, 1, 4, 23, 55, 0)

        data_access = CassandraDataAccess(
            TestCassandraDataAccess._cassandra_ip_address)
        actual_data = data_access.retrieve(start_time=start_time,
                                           end_time=end_time)

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

        # Temps;
        # plt.imshow(expected_data)
        # plt.show()
        # plt.plot(expected_data)
        # plt.show()

        self.assertEqual(len(actual_data.T), 4)

    # @unittest.skip("This test accesses Cassandra database." +
    # "Thus, this test will not be a part of continuous integration.")
    def test_construct_random_choice_list(self):

        sites = np.array(["SLACA0000001", "SLACA0000002"])
        total_number_of_elements = 6

        data_access = CassandraDataAccess(
            TestCassandraDataAccess._cassandra_ip_address)
        actual_list = data_access._construct_random_choice_list(sites,
            total_number_of_elements)

        expected_list = ["SLACA0000001"] * 3 + ["SLACA0000002"] * 3

        np.testing.assert_array_equal(actual_list, expected_list)

    # @unittest.skip("This test accesses Cassandra database." +
    # "Thus, this test will not be a part of continuous integration.")
    def test_get_data_candidate(self):

        data_access = CassandraDataAccess(
            TestCassandraDataAccess._cassandra_ip_address)

        actual_data = data_access._get_data_candidate(["SLACA0000001",
            "SLACA0000002"])
        expected_data = {"SLACA0000001":
                         TestCassandraDataAccess._power_signals_site_1,
                         "SLACA0000002":
                         TestCassandraDataAccess._power_signals_site_2}
        np.testing.assert_array_equal(actual_data.keys(), expected_data.keys())
        np.testing.assert_almost_equal(actual_data["SLACA0000001"],
                                       expected_data["SLACA0000001"],
                                       decimal=5)
        np.testing.assert_almost_equal(actual_data["SLACA0000002"],
                                       expected_data["SLACA0000002"],
                                       decimal=5)

    # @unittest.skip("This test accesses Cassandra database." +
    # "Thus, this test will not be a part of continuous integration.")
    def test_query_power_for_given_site(self):

        data_access = CassandraDataAccess(
            TestCassandraDataAccess._cassandra_ip_address)

        actual_data_1 = data_access._query_power_for_given_site("SLACA0000001")
        expected_data_1 = TestCassandraDataAccess._power_signals_site_1
        np.testing.assert_almost_equal(actual_data_1, expected_data_1,
                                       decimal=5)

        actual_data_2 = data_access._query_power_for_given_site("SLACA0000002")
        expected_data_2 = TestCassandraDataAccess._power_signals_site_2
        np.testing.assert_almost_equal(actual_data_2, expected_data_2,
                                       decimal=5)

    # @unittest.skip("This test accesses Cassandra database." +
    # "Thus, this test will not be a part of continuous integration.")
    def test_query_power_for_given_site_with_time_range(self):

        start_time = datetime(2019, 1, 1, 0, 0, 0)
        end_time = datetime(2019, 1, 3, 23, 55, 0)

        data_access = CassandraDataAccess(
            TestCassandraDataAccess._cassandra_ip_address)

        actual_data_1 = data_access._query_power_for_given_site("SLACA0000001",
            start_time=start_time, end_time=end_time)
        expected_data_1 = TestCassandraDataAccess._power_signals_site_1[:,:3]
        np.testing.assert_almost_equal(actual_data_1, expected_data_1,
                                       decimal=5)

        actual_data_2 = data_access._query_power_for_given_site("SLACA0000002",
            start_time=start_time, end_time=end_time)
        expected_data_2 = TestCassandraDataAccess._power_signals_site_2[:,:3]
        np.testing.assert_almost_equal(actual_data_2, expected_data_2,
                                       decimal=5)
