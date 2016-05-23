#!/usr/bin/python
# -*- coding: utf-8 -*

import multiprocessing
from pandas import concat, DataFrame, Series, set_option, merge, to_numeric
from app.services.check_bios.data_handler import DataHandler
from app.services.check_bios.text_normalizer import sentences_splitter
import numpy as np


class Extractor(object):
    def __init__(self, joined_regexes, content_regexes):
        self.joined_regexes = joined_regexes
        self.content_regexes = content_regexes

    def get_ai_results(self, bios):
        set_option('display.max_colwidth', -1)
        p = multiprocessing.Pool(4)
        pool_results = p.map(self.filter_with_regex, np.array_split(bios, 4))
        p.close()
        p.join()
        p.terminate()
        concatenated = concat(pool_results)

        # concatenated['profileUrl'] = concatenated['profileUrl'].apply(
        #     lambda x: '<p class = "link"><a href="{}">{}</a></p>'.format(x, x))
        # concatenated['sentence'] = concatenated['sentence'].apply(lambda x: '<p class = "test">{}</p>'.format(x))
        # return concatenated[
        #     ['profileUrl', 'sentence', 'sent_num', 'content', 'context', 'joined', 'practice_areas',
        #      'specialties', 'score']]
        return concatenated

    def filter_with_regex(self, bio_df):
        splitted_bios = self.split_bio_df(bio_df)
        print("sentences splitted")
        unique_content_regexes_keys = set(self.joined_regexes.content_regex.values)
        all_content_regexes_dict = DataHandler.df_to_dict(self.content_regexes, "Content REG ID", "KeyWord")

        result = []
        for content_regex_key in unique_content_regexes_keys:
            print(content_regex_key)
            content_regex_value = all_content_regexes_dict.get(content_regex_key)
            content_filtered_bios = self.filter_bios(splitted_bios, content_regex_value)

            joined_regexes = self.joined_regexes[self.joined_regexes["content_regex"] == content_regex_key][[
                "regex", "regex_id"]].values.tolist()
            for regex in joined_regexes:
                joined_regex_value, joined_regex_index = regex
                bio_df = self.filter_bios(content_filtered_bios, joined_regex_value)

                if not bio_df.empty:
                    pr_areas, specialties, score, context_regex = self.get_regex_info(joined_regex_index)
                    bio_df['content'] = DataFrame(
                        [content_regex_key] * len(bio_df.attorneyBio.values)).values
                    bio_df['context'] = DataFrame([context_regex] * len(bio_df.attorneyBio.values)).values
                    bio_df['joined'] = DataFrame([joined_regex_index] * len(bio_df.attorneyBio.values)).values
                    bio_df['practice_areas'] = DataFrame(pr_areas * len(bio_df.attorneyBio.values)).values
                    bio_df['specialties'] = DataFrame(specialties * len(bio_df.attorneyBio.values)).values
                    bio_df['score'] = DataFrame(score * len(bio_df.attorneyBio.values)).values
                    result.append(bio_df)
        return (self.group_data(concat(result)))

    def get_regex_info(self, regex_index):
        current_regex_info_df = self.joined_regexes[self.joined_regexes["regex_id"] == regex_index]
        pr_areas = list(set(current_regex_info_df["pract_areas"].values.tolist()))
        specialties = list(set(current_regex_info_df["specialties"].values.tolist()))
        score = list(set(current_regex_info_df["score"].values.tolist()))
        context_regex = list(set(current_regex_info_df["context_regex"].values.tolist()))
        regex_info = (pr_areas, specialties, score, context_regex)
        return regex_info

    def split_bio_df(self, df):
        splitted_bios = concat([Series(row['profileUrl'], sentences_splitter(row['attorneyBio']))
                                for _, row in df.iterrows()]).reset_index()
        splitted_bios.columns = ["attorneyBio", "profileUrl"]
        splitted_bios[['sent_num', 'sentence']] = splitted_bios['attorneyBio'].apply(Series)
        return splitted_bios

    def filter_bios(self, df_to_filter, regex_for_filtering):
        df_to_filter_copy = df_to_filter.copy()
        filtered_bios = df_to_filter_copy[df_to_filter_copy['sentence'].str.contains(regex_for_filtering)]
        return filtered_bios

    def split_data_frame_col(self, df, new_cols, previous):
        df[new_cols] = df[previous].apply(Series)
        df.drop([previous], inplace=True, axis=1)
        return df

    def split_data_frame_rows(self, df, col_to_split):
        splitted_df = df[col_to_split].apply(Series, 1).stack()
        splitted_df.index = splitted_df.index.droplevel(-1)
        splitted_df.name = col_to_split
        df.drop([col_to_split], inplace=True, axis=1)
        return df.join(splitted_df)

    def join_cols(self, df, cols_to_join):
        return df[cols_to_join].apply(tuple, axis=1)

    def group_data(self, filtered_bios_df):
        print("result_processing started")
        filtered_bios_df['sentence_info'] = self.join_cols(filtered_bios_df,
                                                           ['specialties', 'context', 'joined', 'score'])

        grouped_bios = filtered_bios_df.groupby(['profileUrl', 'sent_num', 'content', 'practice_areas'])[
            'sentence_info'].agg(
            {'result': lambda x: tuple([i for i in x if i[3] == max([i[3] for i in x])][0])}).reset_index()

        grouped_bios = self.split_data_frame_col(grouped_bios, ['specialties', 'context_regex', 'joined', 'score'],
                                                 'result')

        grouped_bios['specialty_info'] = self.join_cols(grouped_bios, ['practice_areas', 'score'])
        grouped_bios.drop(['practice_areas', 'score'], inplace=True, axis=1)

        test = grouped_bios.groupby(['profileUrl', 'sent_num', 'context_regex', 'specialties', 'joined'])[
            'specialty_info'].agg(
            {'main_info': lambda x: tuple(set(x, ))[0]}).reset_index()

        result = self.split_data_frame_col(test, ['practice_areas', 'specialty_score'], 'main_info')

        return self.count_result(result)

    def remove_conflicts(self, sentence_info):
        specialties = [value[0] for value in sentence_info]

        conflict_groups = {key: [] for key in set(specialties)}
        unconflict_groups = []
        for data in sentence_info:
            specialty, pract_area, score = data
            if specialties.count(specialty) > 1:
                conflict_groups[specialty].append(data)
            elif pract_area in conflict_groups.keys():
                conflict_groups[pract_area].append([pract_area, score])
            else:
                unconflict_groups.append(data)

        for key in conflict_groups.keys():
            if conflict_groups.get(key):
                [unconflict_groups.append(i) for i in conflict_groups.get(key) if
                 i[2] == max([i[2] for i in conflict_groups.get(key)])]
        return unconflict_groups

    def count_result(self, grouped_df):
        grouped_df = grouped_df.convert_objects(convert_numeric=True)
        grouped_df['practice_area_score'] = grouped_df.groupby(['profileUrl', 'practice_areas'])[
            'specialty_score'].transform('sum')
        grouped_df['practice_area_info'] = self.join_cols(grouped_df,
                                                          ['specialties', 'practice_areas', 'practice_area_score'])

        grouped_bios = grouped_df.groupby(['profileUrl', 'sent_num'])[
            'practice_area_info'].agg(
            {'result': lambda x: tuple(self.remove_conflicts(x, ))})
        grouped_bios = self.split_data_frame_rows(grouped_bios, 'result')
        grouped_bios = self.split_data_frame_col(grouped_bios, ['specialty', 'pract_area', 'score'], 'result').reset_index()

        grouped_bios = grouped_bios.groupby(['profileUrl'])['specialty', 'pract_area'].agg(
            {lambda x: ', '.join(set(x, ))})
        print(grouped_bios)
        return grouped_bios


if __name__ == "__main__":
    pass
