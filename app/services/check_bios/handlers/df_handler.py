from pandas import Series, DataFrame


def split_data_frame_col(df, new_cols, column_to_split):
    """
    :param df: DataFrame
    :param new_cols: list of rows, which previous column will be splitted to.
    :param column_to_split: str, column name, containing iterable structure
    :return: DataFrame with separated columns
    """
    df[new_cols] = df[column_to_split].apply(Series)
    df.drop([column_to_split], inplace=True, axis=1)
    return df


def split_data_frame_rows(df, col_to_split):
    """
    :param df: DataFrame
    :param col_to_split: str, column name, that contains rows with iterable data, needs to be divided into different rows
    :return: DataFrame with splitted rows
    """
    splitted_df = df[col_to_split].apply(Series, 1).stack()
    splitted_df.index = splitted_df.index.droplevel(-1)
    splitted_df.name = col_to_split
    df.drop([col_to_split], inplace=True, axis=1)
    return df.join(splitted_df)


def join_df_cols(df, cols_to_join):
    """
    :param df: DataFrame
    :param cols_to_join: list of DataFrame columns, that need to be joined
    :return:
    """
    return df[cols_to_join].apply(tuple, axis=1)


def filer_bios_with_not_contain_regex(bio_df, col_name, regexes):
    """
    :param bio_df: DataFrame
    :param col_name: str
    :param regexes: list
    :return: Data frame, with removing sentences which contain given regex
    """
    df_for_filtering = bio_df.copy()
    for regex in regexes:
        df_for_filtering = df_for_filtering[~df_for_filtering[col_name].str.contains(regex)]
    return df_for_filtering


def filter_bios_with_contain_regex(bio_df, col_name, regex_for_filtering):
    """
    :param bio_df: DataFrame
    :param col_name: str
    :param regex_for_filtering: str
    :return: DataFrame, filtered with input regular expression
    """
    try:
        df_to_filter_copy = bio_df.copy()
        filtered_bios = df_to_filter_copy[df_to_filter_copy[col_name].str.contains(regex_for_filtering)]
        return filtered_bios
    except:
        return DataFrame()
