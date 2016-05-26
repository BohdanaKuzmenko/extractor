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
JOINED_REGEX_LIST_NAME = 'JoinedREGEX'
JOINED_REGEX_ID_COL = "JOIN REG ID"
JOINED_REGEX_VALUE_COL = "JOINED REGEX"
FINAL_REGEX_DF_ID = "reg_id"
FINAL_REGEX_DF_VALUE = "regex_value"
FINAL_NARROW_REGEX_DF_VALUE = "narrow_regex"


def get_bios(source, source_text):
    if source == "from_file":
        return next(DataHandler.get_csv_values('app/data/full_data.csv')).fillna('')
    if source == "from_text":
        return DataFrame([['No url', source_text, '', '']],
                         columns=['profileUrl', 'attorneyBio', 'practice_areas', 'specialty'])


def get_regexes_frames(raw_regex):
    regexes = []
    if raw_regex:
        regexes = [(r.replace('\r', '')) for r in raw_regex.split('\n')]

    sub_regexes_df = DataHandler.get_spread_sheet_values(SPREADSHEET_ID, SUB_REGEXES_LIST_NAME)\
        .fillna('')[[SUB_REGEX_ID_COL, SUB_REGEX_VALUE_COL]]
    sub_regexes_df.columns = [FINAL_REGEX_DF_ID, FINAL_REGEX_DF_VALUE]

    content_regexes_df = DataHandler.get_spread_sheet_values(SPREADSHEET_ID, CONTENT_REGEX_LIST_NAME) \
        .fillna('')[[CONTENT_REGEX_ID_COL, CONTENT_REGEX_VALUE_COL, CONTENT_NARROW_REGEX]]
    content_regexes_df.columns = [FINAL_REGEX_DF_ID, FINAL_REGEX_DF_VALUE, FINAL_NARROW_REGEX_DF_VALUE]

    merged_regexes = concat([sub_regexes_df, content_regexes_df[[FINAL_REGEX_DF_ID, FINAL_REGEX_DF_VALUE]]], ignore_index=True)
    merged_regexes_dict = DataHandler.df_to_dict(merged_regexes, FINAL_REGEX_DF_ID, FINAL_REGEX_DF_VALUE)
    # merged_regexes = merged_regexes.set_index([FINAL_REGEX_DF_ID])

    joined_regexes_df = DataHandler.get_spread_sheet_values(SPREADSHEET_ID, JOINED_REGEX_LIST_NAME).fillna('')
    result_regex_list = []

    for regex in regexes:
        df = joined_regexes_df[joined_regexes_df[JOINED_REGEX_ID_COL] == regex]
        regexes_keys = df[JOINED_REGEX_VALUE_COL].str.split("@").values.tolist()
        # merged_regex = ''.join(
        #     [merged_regexes.at[value, FINAL_REGEX_DF_VALUE] if value and value in merged_regexes.index.tolist() else value for
        #      value in regexes_keys[0]])\
        merged_regex = ''.join(
            [merged_regexes_dict.get(value) if value and value in merged_regexes_dict.keys() else value for value in
             regexes_keys[0]])
        df['regex'] = DataFrame([merged_regex]).values
        result_regex_list.append(df)

    return (concat(result_regex_list), content_regexes_df)
