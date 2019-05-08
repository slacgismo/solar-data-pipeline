"""
This module contains the code to retrieve data from Cassandra database
"""
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
