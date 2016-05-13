from pandas import DataFrame


class Statistics(object):
    @staticmethod
    def count_rows(data_frame, row):
        return data_frame[row].count()

    @staticmethod
    def get_differs(data_frame_current, data_frame_compare_with, row):
        original = data_frame_current[row].values.tolist()
        compare_with = data_frame_compare_with[row].values.tolist()
        return [value for value in original if value not in compare_with]

    @staticmethod
    def get_equals(data_frame_current, data_frame_compare_with, row):
        original = data_frame_current[row].values.tolist()
        compare_with = data_frame_compare_with[row].values.tolist()
        return [value for value in original if value in compare_with]

    @staticmethod
    def get_all_statistics(data_frame_current, data_frame_compare_with, row):
        equals = len(Statistics.get_equals(data_frame_current, data_frame_compare_with, "profileUrl"))
        ai_only = len(Statistics.get_differs(data_frame_current, data_frame_compare_with, "profileUrl"))
        ldb_only = Statistics.get_differs(data_frame_compare_with, data_frame_current, "profileUrl")
        ldb_only_table = DataFrame()
        if ldb_only:
            ldb_only_table = data_frame_compare_with[data_frame_compare_with['profileUrl'].isin(ldb_only)]
        return(equals, ai_only, len(ldb_only), ldb_only_table)
