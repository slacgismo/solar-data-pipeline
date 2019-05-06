"""
This module contains the client facing class for data retrieval.
Details are hidden or delegated to other classes.
"""
import random
import functools
import numpy as np

class DataRetrieval:
    """
    User facing class for data retrieval.
    """

    def get(self, date_index_range=range(365), partition_ratio={}):
        """
        Get the uniformly distributed data from various data source of solar
        power signals data.

        Keyword arguments
        -----------------
        date_index_range : range
            Range of dates.

        Returns
        -------
        numpy array
            Representing a matrix with row for dates and colum for time of day,
            containing power signals with fixed time shift.
        """

        # Simple implementation as a start with assumption with two data sources
        # Transpose the matrix, so that in array of array format, each element
        # of outer array contains daily signal:
        data_1 = self._get_cassandra_data_access().retrieve().T
        data_2 = self._get_csv_access().retrieve().T
        #data_candidates = list(zip(data_1, data_2))
        data_candidates = {"cassandra": data_1, "file": data_2}

        random_choice_list = self._construct_random_choice_list(
            partition_ratio, len(data_1))

        daily_signal_based_data = self._random_choice(data_candidates,
            random_choice_list, len(data_1))

        # Transpose to make the matrix with row of yearly data and column of
        # daily data:
        return daily_signal_based_data.T

    def _construct_random_choice_list(self, partition_ratio,
                                      total_number_of_elements):
        """
        Arguments
        -----------------
        partition_ratio : Dictionary
            Key: Name of data source.
            Value: Between 0 ane 1. Ratio of data from that data source.
        total_number_of_elements: integer
            Total number of elements of the choice list.
        """

        if len(partition_ratio) == 0:
           # Simple implementation as a start with assumption
           # with two data sources
           return ["cassandra", "file"]
        else:
           return functools.reduce(lambda choice_list, key:
               choice_list + (
               [key] * int(partition_ratio[key] * total_number_of_elements)),
               partition_ratio, [])

    def _random_choice(self, data_candidates, random_choice_list,
        total_number_of_elements):
        sample = np.random.choice(random_choice_list, total_number_of_elements,
            replace=False)
        return np.array(
            [data_candidates[sample[i]][i]
             for i in range(total_number_of_elements)])

    def _get_cassandra_data_access(self):
        if ((not hasattr(self, '_cassandra_data_access')) or
           (self._cassandra_data_access is None)):
           from solar_data_pipeline.database.cassandra import\
               CassandraDataAccess
           # This will be read from configuration file:
           cassandra_ip_address = '127.0.0.1'
           self._cassandra_data_access = CassandraDataAccess(
               cassandra_ip_address)
        return self._cassandra_data_access

    def _set_cassandra_data_access(self, data_access):
        """
        For dependency injection for testing, i.e. for injecting mock.
        This method is set to be private, in order to indicate that it is
        not accessed from the client code.
        """
        self._cassandra_data_access = data_access

    def _get_csv_access(self):
        if ((not hasattr(self, '_csv_access')) or
           (self._csv_access is None)):
           from solar_data_pipeline.file.csv import CsvAccess
           # This will be read from configuration file:
           file_url = './test.csv'
           self._csv_access = CsvAccess(file_url)
        return self._csv_access

    def _set_csv_access(self, access):
        """
        For dependency injection for testing, i.e. for injecting mock.
        This method is set to be private, in order to indicate that it is
        not accessed from the client code.
        """
        self._csv_access = access
