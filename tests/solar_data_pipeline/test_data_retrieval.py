import unittest
from unittest.mock import Mock
import os
import numpy as np
from solar_data_pipeline.data_retrieval import DataRetrieval
from solar_data_pipeline.database.cassandra import CassandraDataAccess
from solar_data_pipeline.file.csv import CsvAccess

class TestDataRetrieval(unittest.TestCase):

    def setUp(self):
        input_power_signals_file_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__),
            "../fixtures/one_year_power_signals_1.csv"))
        with open(input_power_signals_file_path) as file:
            self._power_signals = np.loadtxt(file, delimiter=',')

    @unittest.skip("Fix after the other test is complete")
    def test_get_data_for_two_days_for_one_site(self):

        mock_cassandra_data_access = Mock(spec=CassandraDataAccess)
        mock_cassandra_data_access.retrieve.return_value =\
            self._power_signals.T[:2].T

        date_index_range=range(2)

        data_retrieval = DataRetrieval()

        data_retrieval._set_cassandra_data_access(mock_cassandra_data_access)

        actual_data = data_retrieval.get(date_index_range=date_index_range)

        expected_data = self._power_signals.T[:2].T

    def test_get_data_for_four_days_for_two_sites(self):

        mock_cassandra_data_access = Mock(spec=CassandraDataAccess)
        mock_cassandra_data_access.retrieve.return_value =\
            self._power_signals[:,:2]
        mock_csv_access = Mock(spec=CsvAccess)
        mock_csv_access.retrieve.return_value =\
            self._power_signals[:, 2:4]

        date_index_range=range(4)

        data_retrieval = DataRetrieval()

        data_retrieval._set_cassandra_data_access(mock_cassandra_data_access)
        data_retrieval._set_csv_access(mock_csv_access)

        actual_data = data_retrieval.get(date_index_range=date_index_range)

        expected_data = self._power_signals[:,:4]

    def test_get_random_data_for_four_days_for_two_sites(self):

        daily_data_1 = np.array([np.linspace(0, 10, 10),
                                 np.linspace(0, 10, 10) * 1.2,
                                 np.linspace(0, 10, 10) * 1.4,
                                 np.linspace(0, 10, 10) * 1.6])
        daily_data_2 = np.array([np.linspace(0, 10, 10) * 1.1,
                                 np.linspace(0, 10, 10) * 1.3,
                                 np.linspace(0, 10, 10) * 1.5,
                                 np.linspace(0, 10, 10) * 1.7])

        mock_cassandra_data_access = Mock(spec=CassandraDataAccess)
        mock_cassandra_data_access.retrieve.return_value = daily_data_1.T
        mock_csv_access = Mock(spec=CsvAccess)
        mock_csv_access.retrieve.return_value = daily_data_2.T

        date_index_range = range(4)

        data_retrieval = DataRetrieval()

        data_retrieval._set_cassandra_data_access(mock_cassandra_data_access)
        data_retrieval._set_csv_access(mock_csv_access)

        actual_data = data_retrieval.get(date_index_range=date_index_range,
            partition_ratio={"cassandra": 0.5, "file": 0.5})

        self._assert_number_of_matching_daily_signals(actual_data.T,
            daily_data_1, 2)
        self._assert_number_of_matching_daily_signals(actual_data.T,
            daily_data_2, 2)

    def _assert_number_of_matching_daily_signals(self, actual, original_data,
        expected_number_of_data_match):
        # data_match = (actual == original_data)
        data_match = np.array([np.array_equal(actual[i],
                                 original_data[i])
                                 for i in range(len(original_data))])
        actual_number_of_data_match = np.sum(data_match)
        self.assertEqual(actual_number_of_data_match,
                         expected_number_of_data_match)

    def test_construct_random_choice_list(self):

        partition_ratio = {"cassandra": 0.5, "file": 0.5}
        total_number_of_elements = 4

        data_retrieval = DataRetrieval()

        actual_list = data_retrieval._construct_random_choice_list(
            partition_ratio, total_number_of_elements)

        expected_list = ['cassandra'] * 2 + ['file'] * 2

        np.testing.assert_array_equal(actual_list, expected_list)
