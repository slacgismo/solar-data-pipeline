"""
This module defines options for data transformations.
"""
from abc import ABCMeta, abstractmethod
import pandas as pd
from statistical_clear_sky.utilities.data_conversion\
 import make_time_series
from solar_data_pipeline.utilities.data_trainsformation\
 import AbstractDataTransformation

class AllDataTransformation(AbstractDataTransformation):

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
        data_frame = pd.DataFrame(data_array.tolist())
        # data_frame.set_index(datetimekey)
        # standardize_time_axis function from solar-data-tools fails:
        time_series_data_frame = make_time_series(data_frame,
            return_keys=False).fillna(0)
        # There seems to be a problem in make_time_series function:
        time_series_size = (len(time_series_data_frame.index) // 288) * 288
        power_matrix = time_series_data_frame.iloc[
            :time_series_size].values.reshape(288, -1, order='F')
        return power_matrix
