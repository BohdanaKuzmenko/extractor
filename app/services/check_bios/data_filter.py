import multiprocessing
from pandas import concat, DataFrame, merge
from app.services.check_bios.handlers.io_data_handler import DataHandler

SPREADSHEET_ID = '1dGKAXcZze3n6ypzdHUVrsULx5e-8sfkmYO2Ow3jagHE'
SUB_REGEXES_LIST_NAME = "ContextREGEX"
SUB_REGEX_ID_COL = "SR ID"
SUB_REGEX_VALUE_COL = "SubREGEX"

CONTENT_REGEX_LIST_NAME = "ContentREGEX"
CONTENT_REGEX_ID_COL = "Content REG ID"
CONTENT_REGEX_VALUE_COL = "KeyWord"
CONTENT_NARROW_REGEX = "Narrow REGs"

# JOINED_REGEX_LIST_NAME = 'JoinedREGEX(ready)'
# JOINED_REGEX_LIST_NAME = 'JoinedREGEX(CN)'
JOINED_REGEX_LIST_NAME = 'JoinedREGEX'
JOINED_REGEX_ID_COL = "JOIN REG ID"
JOINED_REGEX_VALUE_COL = "JOINED REGEX"

FINAL_REGEX_DF_ID = "reg_id"
FINAL_REGEX_DF_VALUE = "regex_value"
FINAL_NARROW_REGEX_DF_VALUE = "narrow_regex"

SUPPORT_WORDS_LIST_NAME = "SupportWords"
PA_COL = "PA"
SUPPORT_WORD_COL = "Support Word"
SCORE_COL = "AddScore"
STOP_WORDS_COL = "StopREGEX"




class DataFilter(object):
    def get_bios(self, source, source_text):
        if source == "from_file":
            return next(DataHandler.get_csv_values('app/data/full_data.csv')).fillna('')
        if source == "from_text":
            return DataFrame([['No url', source_text, '', '']],
                             columns=['profileUrl', 'attorneyBio', 'practice_areas', 'specialty'])

    def get_regexes_frames(self, raw_regex):
        print("Regex processing started")
        regexes = []
        if raw_regex:
            regexes = [(r.replace('\r', '')) for r in raw_regex.split('\n')]

        sub_regexes_df = DataHandler.get_spread_sheet_values(SPREADSHEET_ID, SUB_REGEXES_LIST_NAME) \
            .fillna('')[[SUB_REGEX_ID_COL, SUB_REGEX_VALUE_COL]]
        sub_regexes_df.columns = [FINAL_REGEX_DF_ID, FINAL_REGEX_DF_VALUE]

        content_regexes_df = DataHandler.get_spread_sheet_values(SPREADSHEET_ID, CONTENT_REGEX_LIST_NAME) \
            .fillna('')[[CONTENT_REGEX_ID_COL, CONTENT_REGEX_VALUE_COL, CONTENT_NARROW_REGEX]]
        content_regexes_df.columns = [FINAL_REGEX_DF_ID, FINAL_REGEX_DF_VALUE, FINAL_NARROW_REGEX_DF_VALUE]

        self.merged_regexes = concat([sub_regexes_df, content_regexes_df[[FINAL_REGEX_DF_ID, FINAL_REGEX_DF_VALUE]]],
                                     ignore_index=True)
        self.merged_regexes = self.merged_regexes.set_index([FINAL_REGEX_DF_ID])

        self.joined_regexes_df = DataHandler.get_spread_sheet_values(SPREADSHEET_ID, JOINED_REGEX_LIST_NAME).fillna('')

        support_words_df = DataHandler.get_spread_sheet_values(SPREADSHEET_ID, SUPPORT_WORDS_LIST_NAME) \
            .fillna('')[[PA_COL, SUPPORT_WORD_COL, SCORE_COL]]

        stop_words_df = DataHandler.get_spread_sheet_values(SPREADSHEET_ID, SUPPORT_WORDS_LIST_NAME) \
            .fillna('')[[STOP_WORDS_COL]]

        p = multiprocessing.Pool(4)
        result_regex_list = p.map(self.process_regexes, regexes)
        p.close()
        p.join()
        p.terminate()

        print("Regex processing finished")
        return (concat(result_regex_list), content_regexes_df, support_words_df, stop_words_df)

    def process_regexes(self, regex):
        df = self.joined_regexes_df[self.joined_regexes_df[JOINED_REGEX_ID_COL] == regex]
        regexes_keys = df[JOINED_REGEX_VALUE_COL].str.split("@").values.tolist()
        merged_regex = ''.join([self.merged_regexes.at[value, FINAL_REGEX_DF_VALUE]
                                if value and value in self.merged_regexes.index.tolist() else value
                                for value in regexes_keys[0]])
        df['regex'] = DataFrame([merged_regex]).values
        return df
