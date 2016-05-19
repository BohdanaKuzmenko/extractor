from unittest import TestCase
from app.services.check_bios.statistics import *


class TestStatistics(TestCase):
    def test_count_rows(self):
        data_list = [1, 2, 3, 4, 5, 6]
        test_df = DataFrame(data_list, columns=['test_row'])
        self.assertEqual(Statistics.count_rows(test_df, 'test_row'), len(data_list))

    def test_get_differs(self):
        test_df1 = DataFrame([1, 2, 3, 4, 5, 6], columns=['test_row'])
        test_df2 = DataFrame([3, 4, 5, 6, 7, 8], columns=['test_row'])
        self.assertEqual(Statistics.get_differs(test_df1, test_df2, 'test_row'), {1, 2})

    def test_get_equals(self):
        test_df1 = DataFrame([1, 2, 3, 4, 5, 6], columns=['test_row'])
        test_df2 = DataFrame([3, 4, 5, 6, 7, 8], columns=['test_row'])
        self.assertEqual(Statistics.get_equals(test_df1, test_df2, 'test_row'), {3, 4, 5, 6})
