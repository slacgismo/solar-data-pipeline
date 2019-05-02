"""
This module contains the client facing class for data retrieval.
Details are hidden or delegated to other classes.
"""

class DataRetrieval:
    """
    User facing class for data retrieval.
    """

    def get(self, date_index_range=range(365)):
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
        return self._cassandra_data_access.retrieve()

    def _get_cassandra_data_access(self):
        return self._cassandra_data_access

    def _set_cassandra_data_access(self, data_access):
        """
        For dependency injection for testing, i.e. for injecting mock.
        This method is set to be private, in order to indicate that it is
        not accessed from the client code.
        """
        self._cassandra_data_access = data_access
