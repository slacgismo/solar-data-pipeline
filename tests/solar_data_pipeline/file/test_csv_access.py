import unittest
import os
import numpy as np
from solar_data_pipeline.file.csv import CsvAccess

class TestCsvAccess(unittest.TestCase):

    def setUp(self):
        input_power_signals_file_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__),
            "../../fixtures/one_year_power_signals_1.csv"))
        with open(input_power_signals_file_path) as file:
            self._power_signals = np.loadtxt(file, delimiter=',')

    def test_retrieve(self):

        # Note: In the production environment, this URL would start with "s3://"
        file_url = "file://" + os.path.abspath(
            os.path.join(os.path.dirname(__file__),
            "../../fixtures/one_year_pvo_style_test_data.csv"))

        access = CsvAccess(file_url)
        actual_data = access.retrieve()

        expected_data = self._power_signals[:, :2]

        np.testing.assert_array_equal(actual_data, expected_data)
