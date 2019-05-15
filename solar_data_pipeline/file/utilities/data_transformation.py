"""
This module defines options for data transformations.
"""
from abc import ABCMeta, abstractmethod
import numpy as np
import pandas as pd
from solardatatools.data_transforms\
 import standardize_time_axis, make_2d, fix_time_shifts
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
        data_frame.replace(-999999.0, np.NaN, inplace=True)
        # data_frame.set_index(datetimekey)
        time_series_data_frame = make_time_series(data_frame,
            return_keys=False)
        standardized_data_frame = standardize_time_axis(time_series_data_frame)
        power_matrix = make_2d(standardized_data_frame, key=ac_power_key,
            zero_nighttime=True, interp_missing=True)
        return fix_time_shifts(power_matrix)
