"""
This module defines options for data transformations.
"""
from abc import ABCMeta, abstractmethod

NOT_IMPLEMENTED = "You should implement this."

class AbstractDataTransformation():
    __metaclass__ = ABCMeta

    @abstractmethod
    def transform(self, data_array, datetimekey='Date-Time',
        ac_power_key='ac_power'):
        """
        Arguments
        -----------------
        data_array : numpy array
            Data from the data source

        Returns
        -------
        numpy array
            Representing a matrix with row for dates and colum for time of day,
            containing power signals.
        """
        raise NotImplementedError(NOT_IMPLEMENTED)

class SimpleDataTransformation(AbstractDataTransformation):
    def transform(self, data_array, datetimekey='Date-Time',
        ac_power_key='ac_power'):
        """
        Arguments
        -----------------
        data_array : numpy array
            Data from the data source

        Returns
        -------
        numpy array
            Representing a matrix with row for dates and colum for time of day,
            containing power signals.
        """
        value_array = np.array([site_data[ac_power_key]
            for site_data in data_array])
        return value_array.reshape(288, -1, order='F')
