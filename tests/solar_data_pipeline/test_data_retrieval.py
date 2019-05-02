import unittest
from unittest.mock import Mock
import os
import numpy as np
from solar_data_pipeline.data_retrieval import DataRetrieval
from solar_data_pipeline.database.cassandra import CassandraDataAccess

class TestDataRetrieval(unittest.TestCase):

    def setUp(self):
        input_power_signals_file_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__),
            "../fixtures/one_year_power_signals_1.csv"))
        with open(input_power_signals_file_path) as file:
            self._power_signals = np.loadtxt(file, delimiter=',')

    def test_get_data_for_two_days_for_one_site(self):

        mock_cassandra_data_access = Mock(spec=CassandraDataAccess)
        mock_cassandra_data_access.retrieve.return_value =\
            self._power_signals.T[:2].T

        date_index_range=range(2)

        data_retrieval = DataRetrieval()

        data_retrieval._set_cassandra_data_access(mock_cassandra_data_access)

        actual_data = data_retrieval.get(date_index_range=date_index_range)

        expected_data = self._power_signals.T[:2].T
