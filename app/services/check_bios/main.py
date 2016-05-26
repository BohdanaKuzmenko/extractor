#!/usr/bin/python
# -*- coding: utf-8 -*

import multiprocessing
import numpy as np
from pandas import concat, DataFrame, set_option
from app.services.check_bios.text_normalizer import sentences_splitter
from app.services.check_bios.handlers.io_data_handler import DataHandler
from app.services.check_bios.handlers.df_handler import *

PROFILE_URL_COL = "profileUrl"
FULL_BIO_COL = "attorneyBio"
SENTENCE_INDEX_COL = "sent_index"
SENTENCE_CONTENT_COL = "sentence"
SPECIALTIES_COL = 'specialties'
PRACTICE_AREAS_COL = 'practice_areas'
CONTEXT_REG_COL = 'context'
CONTENT_REG_COL = 'content'
JOINED_REG_COL = 'joined'
REG_SCORE_COL = 'score'
PRACTICE_AREAS_SCORE = 'practice_area_score'


class Extractor(object):
    def __init__(self, joined_regexes, content_regexes):
        self.joined_regexes = joined_regexes
        self.content_regexes = content_regexes

    def get_ai_results(self, bios):
        set_option('display.max_colwidth', -1)
        # if bios.profileUrl.count() >= 4:
        #     p = multiprocessing.Pool(4)
        #     pool_results = p.map(self.filter_with_regex, np.array_split(bios, 4))
        #     p.close()
        #     p.join()
        #     p.terminate()
        #     concatenated = concat(pool_results)
        # else:
        #     concatenated = self.filter_with_regex(bios)
        concatenated = self.filter_with_regex(bios)
        return concatenated

    def filter_with_regex(self, bio_df):
        sentence_tokenized_bios = self.bio_df_sentence_tokenizing(bio_df)
        unique_content_regexes_keys = set(self.joined_regexes['CN ID'].values)
        self.content_regexes = self.content_regexes.set_index(["reg_id"])
        # all_content_regexes_dict = DataHandler.df_to_dict(self.content_regexes, "reg_id", "regex_value")

        result = []
        # Filter bios with content regex
        for content_regex_key in unique_content_regexes_keys:
            print(content_regex_key)
            if content_regex_key in self.content_regexes.index.values.tolist():
                content_regex_value = self.content_regexes.at[content_regex_key, 'regex_value']
                # all_content_regexes_dict.get(content_regex_key)
                content_filtered_bios = self.filter_bios_with_contain_regex(sentence_tokenized_bios,
                                                                            SENTENCE_CONTENT_COL,
                                                                            content_regex_value)
                narrow = self.content_regexes.at[content_regex_key, 'narrow_regex']
                if narrow:
                    content_filtered_bios = self.filer_bios_with_not_contain_regex(content_filtered_bios,
                                                                                   SENTENCE_CONTENT_COL, narrow)

                joined_regexes = self.joined_regexes[self.joined_regexes['CN ID'] == content_regex_key]

                for _, reg in joined_regexes.iterrows():

                    bio_df = self.filter_bios_with_contain_regex(content_filtered_bios, SENTENCE_CONTENT_COL,
                                                                 reg['regex'])

                    if not bio_df.empty:
                        bio_df[CONTENT_REG_COL] = DataFrame([reg['CN ID']] * len(bio_df.attorneyBio.values)).values
                        bio_df[CONTEXT_REG_COL] = DataFrame([reg['CX ID']] * len(bio_df.attorneyBio.values)).values
                        bio_df[JOINED_REG_COL] = DataFrame([reg['JOIN REG ID']] * len(bio_df.attorneyBio.values)).values
                        bio_df[PRACTICE_AREAS_COL] = DataFrame([reg['PA']] * len(bio_df.attorneyBio.values)).values
                        bio_df[SPECIALTIES_COL] = DataFrame([reg['SP']] * len(bio_df.attorneyBio.values)).values
                        bio_df[REG_SCORE_COL] = DataFrame([reg['REG score']] * len(bio_df.attorneyBio.values)).values
                        result.append(bio_df)

        if result:
            return self.group_data(concat(result))
        return DataFrame()

    def bio_df_sentence_tokenizing(self, df):
        '''
        :param df: DataFrame
        :return: DataFrame containing sentence splitted biographies. Each row contains bio url,
        separated sentence and its index.
        '''
        splitted_bios = concat([Series(row[PROFILE_URL_COL], sentences_splitter(row[FULL_BIO_COL]))
                                for _, row in df.iterrows()]).reset_index()
        splitted_bios.columns = [FULL_BIO_COL, PROFILE_URL_COL]
        splitted_bios[[SENTENCE_INDEX_COL, SENTENCE_CONTENT_COL]] = splitted_bios[FULL_BIO_COL].apply(Series)
        return splitted_bios

    def filer_bios_with_not_contain_regex(self, bio_df, col_name, regexes):
        regexes_list = regexes.split(";")
        df_for_filtering = bio_df.copy()
        for regex in regexes_list:
            df_for_filtering = df_for_filtering[~df_for_filtering[col_name].str.contains(regex)]
        return df_for_filtering

    def filter_bios_with_contain_regex(self, bio_df, col_name, regex_for_filtering):
        '''
        :param bio_df: DataFrame
        :param regex_for_filtering: str
        :return: DataFrame, filtered with input regular expression
        '''
        try:
            df_to_filter_copy = bio_df.copy()
            filtered_bios = df_to_filter_copy[df_to_filter_copy[col_name].str.contains(regex_for_filtering)]
            return filtered_bios
        except:
            return DataFrame()

    def group_data(self, filtered_bios_df):
        '''
        :param filtered_bios_df: DataFrame
        Method filters rows with the same practice areas and specialties according to max score value.
        '''

        cols_to_join = [SPECIALTIES_COL, CONTEXT_REG_COL, JOINED_REG_COL, REG_SCORE_COL]
        filtered_bios_df['sentence_info'] = join_df_cols(filtered_bios_df, cols_to_join)
        filtered_bios_df.drop(cols_to_join, inplace=True, axis=1)

        grouped_bios = \
            filtered_bios_df.groupby([PROFILE_URL_COL, SENTENCE_INDEX_COL, CONTENT_REG_COL, PRACTICE_AREAS_COL]) \
                ['sentence_info'].agg({'result': lambda x: tuple(
                [sent_info for sent_info in x if sent_info[3] == max([sent_info[3] for sent_info in x])][
                    0])}).reset_index()

        grouped_bios = split_data_frame_col(grouped_bios,
                                            [SPECIALTIES_COL, CONTEXT_REG_COL, JOINED_REG_COL, REG_SCORE_COL],
                                            'result')

        cols_to_join = [PRACTICE_AREAS_COL, REG_SCORE_COL]
        grouped_bios['specialty_info'] = join_df_cols(grouped_bios, cols_to_join)
        grouped_bios.drop(cols_to_join, inplace=True, axis=1)

        test = \
            grouped_bios.groupby(
                [PROFILE_URL_COL, SENTENCE_INDEX_COL, CONTEXT_REG_COL, SPECIALTIES_COL, JOINED_REG_COL])[
                'specialty_info'].agg(
                {'main_info': lambda x: tuple(set(x, ))[0]}).reset_index()

        result = split_data_frame_col(test, [PRACTICE_AREAS_COL, REG_SCORE_COL], 'main_info')
        return self.count_result(result)

    def count_result(self, grouped_df):
        '''
        :param grouped_df: DataFrame
        Method counts sum score for every practice area and removes unappropriate practice areas
        '''
        grouped_df = grouped_df.convert_objects(convert_numeric=True)

        grouped_df[PRACTICE_AREAS_SCORE] = grouped_df.groupby([PROFILE_URL_COL, PRACTICE_AREAS_COL])[
            REG_SCORE_COL].transform('sum')

        cols_to_join = [SPECIALTIES_COL, PRACTICE_AREAS_COL, PRACTICE_AREAS_SCORE]
        grouped_df['practice_area_info'] = join_df_cols(grouped_df, cols_to_join)
        grouped_df.drop(cols_to_join, inplace=True, axis=1)

        grouped_bios = grouped_df.groupby([PROFILE_URL_COL, SENTENCE_INDEX_COL])[
            'practice_area_info'].agg(
            {'result': lambda x: tuple(self.remove_conflicts(x, ))})

        grouped_bios = split_data_frame_rows(grouped_bios, 'result')

        grouped_bios = split_data_frame_col(grouped_bios,
                                            [SPECIALTIES_COL, PRACTICE_AREAS_COL, REG_SCORE_COL],
                                            'result').reset_index()
        cols_to_join = [SPECIALTIES_COL, PRACTICE_AREAS_COL, REG_SCORE_COL, SENTENCE_INDEX_COL]
        grouped_bios['bio_full_info'] = join_df_cols(grouped_bios, cols_to_join)
        # grouped_bios.drop(cols_to_join, inplace=True, axis=1)

        grouped_bios = grouped_bios.groupby([PROFILE_URL_COL])['bio_full_info'].agg(
            {'test': lambda x: tuple(self.filter_by_practice_area_score(x, ))})

        grouped_bios = split_data_frame_rows(grouped_bios, 'test')
        grouped_bios = split_data_frame_col(grouped_bios,
                                            [SPECIALTIES_COL, PRACTICE_AREAS_COL, REG_SCORE_COL, SENTENCE_INDEX_COL],
                                            'test').reset_index()

        grouped_bios = grouped_bios.groupby([PROFILE_URL_COL])[SPECIALTIES_COL, PRACTICE_AREAS_COL].agg(
            {"Predictions": lambda x: ', '.join([i for i in set(x, ) if i])}).reset_index()
        return grouped_bios

    def remove_conflicts(self, sentence_info):
        '''
        :param sentence_info: tuple, that contains information gotten for certain sentence: sets of speciality, practice
         area and practice area score
        :return: filtered tuple of tuples
        method removes potential practice areas that are incompatible with other selections.
        '''
        specialties = [value[0] for value in sentence_info]

        conflict_groups = {key: [] for key in set(specialties) if key}
        unconflict_groups = []
        for data in sentence_info:
            specialty, pract_area, score = data
            if specialty and specialties.count(specialty) > 1:
                conflict_groups[specialty].append(data)
            elif pract_area in conflict_groups.keys() and not specialty:
                conflict_groups[pract_area].append(data)
            else:
                unconflict_groups.append(data)
        for key in conflict_groups.keys():
            if conflict_groups.get(key):
                group_max_score = max([i[2] for i in conflict_groups.get(key)])
                max_scored = [i for i in conflict_groups.get(key) if i[2] == group_max_score]
                practice_only = [i for i in max_scored if not i[0]]
                unconflict_groups.extend(practice_only) if practice_only else unconflict_groups.extend(max_scored)
        return unconflict_groups

    def filter_by_practice_area_score(self, data_frame):
        '''
        :param data_frame:
        :return:
        '''
        max_scored_limit = 2
        sorted_data = sorted(data_frame, key=lambda x: int(x[2]), reverse=True)
        appropriate_data = []
        for bio_info in sorted_data:
            if len(appropriate_data) < max_scored_limit or bio_info[2] == appropriate_data[-1][2]:
                appropriate_data.append(bio_info)
        return appropriate_data


if __name__ == "__main__":
    pass
