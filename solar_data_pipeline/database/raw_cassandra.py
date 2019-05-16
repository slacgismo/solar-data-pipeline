"""
This module contains the code to retrieve data from Cassandra database.
It uses more raw construct due to the limitation that has been encountered
when using CassandraDataAccess.
"""
from os.path import expanduser
import functools
import numpy as np
import pandas as pd
from cassandra.cluster import Cluster
from solardatatools import standardize_time_axis, make_2d
from statistical_clear_sky.utilities.data_conversion import make_time_series
from solar_data_pipeline.database.cassandra import CassandraDataAccess

class RawCassandraDataAccess(CassandraDataAccess):

    def __init__(self):
        home = expanduser("~")
        with open(home + '/.aws/cassandra_cluster') as f:
            ip_address = f.readline().strip('\n')
        super().__init__(ip_address)

    def retrieve(self, number_of_sites = 4, number_of_days_per_site = 10):
        selected_sites = self._select_sites(number_of_sites = number_of_sites)

        data_frame = self._get_data_frame_for_sites(selected_sites)
        data_frame_list = self._list_grouped_by_sites(data_frame,
            selected_sites)
        time_series_data_frame_list = self._make_time_series_list(
            data_frame_list)
        standardized_data_frame_list = self._standardize_data_frame(
            time_series_data_frame_list)
        power_matrix_list =  self._make_2d_list(
            standardized_data_frame_list)

        return self._make_selected_power_matrix(power_matrix_list,
            number_of_days_per_site)

    def _select_sites(self, number_of_sites = 4):
        sites = self.get_sites()
        return np.random.choice(sites, number_of_sites)

    def _get_data_frame_for_sites(self, selected_sites):
        cql = self._construct_cql_query(selected_sites)

        cluster = Cluster([self._ip_address])
        session = cluster.connect('measurements')

        rows = session.execute(cql)
        return pd.DataFrame(list(rows))

    def _construct_cql_query(self, selected_sites):
        cql_first_part = ("select site, meas_name, ts, sensor, meas_val_f " +
            "from measurement_raw ")
        cql_last_part = "and meas_name = 'ac_power';"

        cql_sites_string = functools.reduce(lambda result_string, site:
                   result_string + ", '" + site + "'" ,
                   selected_sites, "")[2:]
        cql_where_clause = "where site in (" + cql_sites_string + ")"

        return cql_first_part + cql_where_clause + cql_last_part

    def _list_grouped_by_sites(self, data_frame, selected_sites):
        return [data_frame.loc[data_frame['site'] == site]
            for site in selected_sites]

    def _make_time_series_list(self, data_frame_list):
        return [make_time_series(data_frame, return_keys=False) for data_frame
            in data_frame_list if data_frame.shape[0] > 0]

    def _standardize_data_frame(self, time_series_data_frame_list):
        return [standardize_time_axis(time_series_data_frame) for
            time_series_data_frame in time_series_data_frame_list]

    def _make_2d_list(self, standardized_data_frame_list):
        return [make_2d(standardized_data_frame, key='ac_power_01',
            zero_nighttime=True, interp_missing=True) for
            standardized_data_frame in standardized_data_frame_list]

    def _make_selected_power_matrix(self, power_matrix_list,
        number_of_days_per_site):
        selected_power_list = []

        for power_matrix in power_matrix_list:
            day_candidates = power_matrix.shape[1]
            selected_days = np.random.choice(day_candidates,
                number_of_days_per_site)

            for selected_day in selected_days:
                selected_power_list.append(power_matrix[:,selected_day])

        selected_power_array = np.array(selected_power_list)

        return selected_power_array.T
