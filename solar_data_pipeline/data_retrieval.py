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
        data_1 = self._cassandra_data_access.retrieve().T
        data_2 = self._csv_access.retrieve().T
        #data_candidates = list(zip(data_1, data_2))
        data_candidates = {"cassandra": data_1, "file": data_2}

        random_choice_list = self._construct_random_choice_list(partition_ratio)

        print("random_choice_list: %s" % (random_choice_list))

        daily_signal_based_data = np.array(
            [data_candidates[random.choice(random_choice_list)][i]
             for i in range(len(data_1))])

        # Transpose to make the matrix with row of yearly data and column of
        # daily data:
        return daily_signal_based_data.T

    def _construct_random_choice_list(self, partition_ratio):
        if len(partition_ratio) == 0:
           # Simple implementation as a start with assumption
           # with two data sources
           return ["cassandra", "file"]
        else:
           # Ratio is given in the number between 0 and 1.
           # Multiplying it with 100 makes the value percentage:
           return functools.reduce(lambda choice_list, key:
               choice_list + ([key] * int(partition_ratio[key] * 100)),
               partition_ratio, [])

    def _get_cassandra_data_access(self):
        return self._cassandra_data_access

    def _set_cassandra_data_access(self, data_access):
        """
        For dependency injection for testing, i.e. for injecting mock.
        This method is set to be private, in order to indicate that it is
        not accessed from the client code.
        """
        self._cassandra_data_access = data_access

    def _get_csv_access(self):
        return self._csv_access

    def _set_csv_access(self, access):
        """
        For dependency injection for testing, i.e. for injecting mock.
        This method is set to be private, in order to indicate that it is
        not accessed from the client code.
        """
        self._csv_access = access
