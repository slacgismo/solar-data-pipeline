import unittest
import numpy as np
from solar_data_pipeline.database.raw_cassandra import RawCassandraDataAccess

class TestRawCassandraDataAccess(unittest.TestCase):
    """
    This test depends on Cassandra database thus cannot be run in Continuous
    Integration Server, since Cassandra database doesn't have in-memory
    database as sqlite in-memory database that can be used for testing in place
    of other relational database such as PostgreSQL or MySQL.

    Also this test accesses data in the server.
    Thus, it acts as acceptance test.
    """

    @unittest.skip("This test accesses Cassandra database." +
    "Thus, this test will not be a part of continuous integration.")
    def test_retrieve(self):

        number_of_sites = 4
        number_of_days_per_site = 10

        data_access = RawCassandraDataAccess()
        actual_data = data_access.retrieve(number_of_sites=number_of_sites,
            number_of_days_per_site=number_of_days_per_site)

        self.assertEqual(actual_data.shape[1], number_of_sites *
            number_of_days_per_site)
