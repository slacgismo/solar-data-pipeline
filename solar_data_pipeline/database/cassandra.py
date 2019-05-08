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

    def retrieve(self):
        self._set_up_connection()
        # Simple implementation as a start:
        values = MeasurementRaw.objects().all().limit(
                2 * 288).values_list('meas_val_f')
        return np.array(values).reshape(288, -1, order='F')
        # values = MeasurementRaw.objects().all().values_list('meas_val_f')
        # return np.reshape(np.array(values), (288, -1), order='F')

    def _set_up_connection(self):
        if ((not hasattr(self, '_connection')) or (self._connection is None)):
            self._connection = connection.setup([self._ip_address],
                                                "measurements")

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
