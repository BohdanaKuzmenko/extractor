class Statistics(object):
    @staticmethod
    def count_rows(data_frame, row):
        return data_frame[row].count()

    @staticmethod
    def get_differs(data_frame_current, data_frame_compare_with, row):
        original = data_frame_current[row].values.tolist()
        compare_with = data_frame_compare_with[row].values.tolist()
        return[value for value in original if value not in compare_with]

    @staticmethod
    def get_equals(data_frame_current, data_frame_compare_with, row):
        original = data_frame_current[row].values.tolist()
        compare_with = data_frame_compare_with[row].values.tolist()
        return[value for value in original if value in compare_with]
