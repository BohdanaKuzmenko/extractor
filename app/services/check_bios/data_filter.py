from pandas import concat, DataFrame

from app.services.check_bios.handlers.io_data_handler import DataHandler


def get_all_specialities():
    all_spec = []
    [all_spec.extend(value.split(',')) for value in
     next(DataHandler.get_csv_values('app/data/full_data.csv'))['specialty'].fillna('').values.tolist()]
    distinct_spec = sorted(list(set(all_spec)))
    return ' '.join(['<li id="{}", name="spec">'.format(str(index)) + distinct_spec[index] + '</li>' for index in
                     range(len(distinct_spec)) if distinct_spec[index]])


def get_bios():
    return next(DataHandler.get_csv_values('app/data/full_data.csv')).fillna('')[:50]


def filter_bios(df, regex):
    return df[df.attorneyBio.str.contains(regex)][['profileUrl', 'attorneyBio']]


def get_bios_per_spec(specialities_regex_filter):
    all_bios = next(DataHandler.get_csv_values('app/data/full_data.csv')).fillna('')
    filtered = all_bios[all_bios['specialty'].str.contains(specialities_regex_filter)]
    filtered['profileUrl'] = filtered['profileUrl'].apply(lambda x: '<p class = "link"><a href="{}">{}</a></p>'.format(x, x))
    filtered['attorneyBio'] = filtered['attorneyBio'].apply(lambda x: '<p class ="test">{}</p>'.format(x))
    return filtered[['profileUrl', 'attorneyBio', 'practice_areas', 'specialty']]


def get_regexes_frames(raw_regex):
    regexes = []
    if raw_regex:
        regexes = [(r.replace('\r', '')) for r in raw_regex.split('\n')]

    sub_regexes_df = DataHandler.get_spread_sheet_values('1dGKAXcZze3n6ypzdHUVrsULx5e-8sfkmYO2Ow3jagHE', 'ContextREGEX')
    sub_regexes = DataHandler.df_to_dict(sub_regexes_df, "SR ID", "SubREGEX")

    content_regexes_df = DataHandler.get_spread_sheet_values('1dGKAXcZze3n6ypzdHUVrsULx5e-8sfkmYO2Ow3jagHE',
                                                             'ContentREGEX').fillna('')
    content_regexes = DataHandler.df_to_dict(content_regexes_df, "Content REG ID", "KeyWord")

    merged_regexes = sub_regexes.copy()
    merged_regexes.update(content_regexes)

    joined_regexes_df = DataHandler.get_spread_sheet_values('1dGKAXcZze3n6ypzdHUVrsULx5e-8sfkmYO2Ow3jagHE',
                                                            'JoinedREGEX').fillna('')
    joined_regexes = DataHandler.df_to_dict(joined_regexes_df, "JOIN REG ID",
                                            ["JOINED REGEX", "REG score", "PA", "SP", "CN ID", "CX ID"])

    result_regex_list = []

    for regex in regexes:
        joined_regex = joined_regexes['JOINED REGEX'].get(regex)

        df = DataFrame([''.join([merged_regexes.get(value) if value in merged_regexes.keys() else value for value in
                                 joined_regex.split("@")])], columns=["regex"])
        df['regex_id'] = DataFrame([regex]).values
        df['score'] = DataFrame([joined_regexes['REG score'].get(regex)]).values
        df['pract_areas'] = DataFrame([joined_regexes['PA'].get(regex)]).values
        df['specialties'] = DataFrame([joined_regexes['SP'].get(regex)]).values
        df['content_regex'] = DataFrame([joined_regexes['CN ID'].get(regex)]).values
        df['context_regex'] = DataFrame([joined_regexes['CX ID'].get(regex)]).values
        result_regex_list.append(df)

    return (concat(result_regex_list), content_regexes_df)
