"""
This module contains the code to retrieve data from Cassandra database
"""
import functools
import numpy as np
from cassandra.cqlengine import connection
from solar_data_pipeline.database.models.measurements import MeasurementRaw

class CassandraDataAccess:

    def __init__(self, ip_address):
        self._ip_address = ip_address

    def find_sites(self, site):
        self._set_up_connection()
        query = MeasurementRaw.objects.all()
        query = query.filter(site=site)
        raw_measurement_list = list(query.distinct(['site']))
        return np.array([raw_measurement.site
            for raw_measurement in raw_measurement_list])

    def get_sites(self):
        self._set_up_connection()
        query = MeasurementRaw.objects.all()
        raw_measurement_list = list(query.distinct(['site']))
        return np.array([raw_measurement.site
            for raw_measurement in raw_measurement_list])

    def retrieve(self, start_time=None, end_time=None):
        self._set_up_connection()

        sites = self.get_sites().tolist()

        data_candidates = self._get_data_candidate(sites,
            start_time=start_time, end_time=end_time)

        total_number_of_columns = len(data_candidates[sites[0]][0])

        random_choice_list = self._construct_random_choice_list(sites,
            total_number_of_columns)

        daily_signal_based_data = self._random_choice(data_candidates,
            random_choice_list, total_number_of_columns)

        # Transpose to make the matrix with row of yearly data and column of
        # daily data:
        return daily_signal_based_data.T

    def _set_up_connection(self):
        if ((not hasattr(self, '_connection')) or (self._connection is None)):
            self._connection = connection.setup([self._ip_address],
                                                "measurements")

    def _random_choice(self, data_candidates, random_choice_list,
        total_number_of_columns):
        sample = np.random.choice(random_choice_list, total_number_of_columns,
            replace=False)
        return np.array(
            [data_candidates[sample[i]][:,i]
             for i in range(total_number_of_columns)])

    def _construct_random_choice_list(self, sites, total_number_of_elements):
        """
        Arguments
        -----------------
        sites : Numpy array
            Name of sites.
        total_number_of_elements: integer
            Total number of elements of the choice list.
        """
        number_per_site = total_number_of_elements // len(sites)
        return functools.reduce(lambda choice_list, site:
            choice_list + ([site] * number_per_site), sites, [])

    def _get_data_candidate(self, sites, start_time=None, end_time=None):
        self._set_up_connection()

        data_dictionary = {site: self._query_power_for_given_site(
            site, start_time=start_time, end_time=end_time)
            for site in sites}
        return data_dictionary

    def _query_power_for_given_site(self, site, start_time=None, end_time=None):
        self._set_up_connection()

        query = MeasurementRaw.objects.filter(site=site)
        query = query.filter(meas_name='ac_power')
        if start_time is not None:
            query = query.filter(ts__gte=start_time)
        if end_time is not None:
            query = query.filter(ts__lte=end_time)
        values = query.values_list('meas_val_f')
        value_array = np.array(values)
        return value_array.reshape(288, -1, order='F')
