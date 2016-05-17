from app.services.check_bios.data_handler import DataHandler


def get_all_specialities():
    all_spec = []
    [all_spec.extend(value.split(',')) for value in
     next(DataHandler.get_csv_values('app/data/full_data.csv'))['specialty'].fillna('').values.tolist()]
    distinct_spec = sorted(list(set(all_spec)))
    return ' '.join(['<li id="{}", name="spec">'.format(str(index)) + distinct_spec[index] + '</li>' for index in
                     range(len(distinct_spec)) if distinct_spec[index]])


def get_bios(source, source_text):
    if source == "from_file":
        all_data = next(DataHandler.get_csv_values('app/data/full_data.csv')).fillna('')
        return all_data[['profileUrl', 'attorneyBio']]
    if source == "from_url":
        urls = ([url.replace('\r', '') for url in source_text.split('\n')])
        df = next(DataHandler.get_csv_values('app/data/full_data.csv'))
        return df[df['profileUrl'].isin(urls)].fillna('').values.tolist()
    if source == "from_text":
        return [('no_link', source_text, 'no_pract_area', 'no_specialities')]


def get_bios_per_spec(specialities_regex_filter):
    all_bios = next(DataHandler.get_csv_values('app/data/full_data.csv')).fillna('')
    filtered = all_bios[all_bios['specialty'].str.contains(specialities_regex_filter)]
    filtered['profileUrl'] = filtered['profileUrl'].apply(lambda x: '<a href="{}">{}</a>'.format(x, x))
    filtered['attorneyBio'] = filtered['attorneyBio'].apply(lambda x: '<p title = "{}">{}</p>'.format(x, x[:100]))
    return filtered[['profileUrl', 'attorneyBio', 'practice_areas', 'specialty']]


def get_regexes(raw_regex):
    regexes = []
    if raw_regex:
        regexes = [(r.replace('\r', '')) for r in raw_regex.split('\n')]

    sub_regexes_df = DataHandler.get_spread_sheet_values('1dGKAXcZze3n6ypzdHUVrsULx5e-8sfkmYO2Ow3jagHE', 'ContextREGEX')
    sub_regexes = DataHandler.df_to_dict(sub_regexes_df, "SR ID", "SubREGEX")
    content_regexes_df = DataHandler.get_spread_sheet_values('1dGKAXcZze3n6ypzdHUVrsULx5e-8sfkmYO2Ow3jagHE',
                                                             'ContentREGEX')
    content_regexes = DataHandler.df_to_dict(content_regexes_df, "Content REG ID", "KeyWord")
    merged_regexes = sub_regexes.copy()
    merged_regexes.update(content_regexes)

    result_regex_list = []
    for regex in regexes:
        result_regex_list.append(''.join(
            [merged_regexes.get(value) if value in merged_regexes.keys() else value for value in regex.split("@")]))

    return result_regex_list
