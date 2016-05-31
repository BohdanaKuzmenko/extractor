#!/usr/bin/python
# -*- coding: utf-8 -*

import multiprocessing
import numpy as np
import re
from pandas import concat, DataFrame, set_option
from app.services.check_bios.text_normalizer import sentences_splitter
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
PRACTICE_AREAS_SCORE_COL = 'practice_area_score'
SUPPORT_WORDS_SCORE = 'words_score'


class Extractor(object):
    def __init__(self, joined_regexes, content_regexes, support_words, stop_words):
        self.joined_regexes = joined_regexes
        self.content_regexes = content_regexes
        self.support_words = support_words
        self.stop_words = stop_words

    def get_ai_results(self, bios):
        set_option('display.max_colwidth', -1)
        if bios.profileUrl.count() >= 4:
            p = multiprocessing.Pool(4)
            pool_results = p.map(self.filter_with_regex, np.array_split(bios, 4))
            p.close()
            p.join()
            p.terminate()
            concatenated = concat(pool_results)
        else:
            concatenated = self.filter_with_regex(bios)
        return concatenated

    def filter_with_regex(self, bio_df):
        sentence_tokenized_bios = self.bio_df_sentence_tokenizing(bio_df)
        stop_words = [regex for regex in self.stop_words['StopREGEX'].values.tolist() if regex]
        sentence_tokenized_bios = filer_bios_with_not_contain_regex(sentence_tokenized_bios, SENTENCE_CONTENT_COL,
                                                                    stop_words)
        unique_content_regexes_keys = sorted(set(self.joined_regexes['CN ID'].values))
        self.content_regexes = self.content_regexes.set_index(["reg_id"])

        result = []
        # Filter bios with content regex
        for content_regex_key in unique_content_regexes_keys:
            print(content_regex_key)
            if content_regex_key in self.content_regexes.index.values.tolist():
                content_regex_value = self.content_regexes.at[content_regex_key, 'regex_value']
                content_filtered_bios = filter_bios_with_contain_regex(sentence_tokenized_bios,
                                                                       SENTENCE_CONTENT_COL,
                                                                       content_regex_value)
                narrow = self.content_regexes.at[content_regex_key, 'narrow_regex']
                if narrow:
                    narrow_regexes_values = [self.content_regexes.at[key, 'regex_value'] for key in narrow.split(";")]
                    content_filtered_bios = filer_bios_with_not_contain_regex(content_filtered_bios,
                                                                              SENTENCE_CONTENT_COL,
                                                                              narrow_regexes_values)
                joined_regexes = self.joined_regexes[self.joined_regexes['CN ID'] == content_regex_key]

                for _, reg in joined_regexes.iterrows():

                    bio_df = filter_bios_with_contain_regex(content_filtered_bios, SENTENCE_CONTENT_COL,
                                                            reg['regex'])
                    if not bio_df.empty:
                        bio_df[SUPPORT_WORDS_SCORE] = bio_df[SENTENCE_CONTENT_COL].apply(
                            lambda x: self.count_score(x, reg['PA']))
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

    def count_score(self, sentence, practice_area):
        pa_support_words_df = self.support_words[self.support_words['PA'] == practice_area] \
            .convert_objects(convert_numeric=True)
        pa_support_words_df = pa_support_words_df.set_index(["Support Word"])
        score = sum([pa_support_words_df.loc[pattern, 'AddScore'] * len(re.findall(pattern, sentence)) \
                     for pattern in pa_support_words_df.index.values.tolist()])
        return score

    def bio_df_sentence_tokenizing(self, df):
        """
        :param df: DataFrame
        :return: DataFrame containing sentence splitted biographies. Each row contains bio url,
        separated sentence and its index.
        """
        splitted_bios = concat([Series(row[PROFILE_URL_COL], sentences_splitter(row[FULL_BIO_COL]))
                                for _, row in df.iterrows()]).reset_index()
        splitted_bios.columns = [FULL_BIO_COL, PROFILE_URL_COL]
        splitted_bios[[SENTENCE_INDEX_COL, SENTENCE_CONTENT_COL]] = splitted_bios[FULL_BIO_COL].apply(Series)
        return splitted_bios

    def group_data(self, filtered_bios_df):
        """
        :param filtered_bios_df: DataFrame
        Method filters rows with the same practice areas and specialties according to max score value.
        """
        filtered_bios_df = filtered_bios_df.convert_objects(convert_numeric=True)
        filtered_bios_df[REG_SCORE_COL] = filtered_bios_df[REG_SCORE_COL] + filtered_bios_df[SUPPORT_WORDS_SCORE] + \
                                          (filtered_bios_df[REG_SCORE_COL] / filtered_bios_df[SENTENCE_INDEX_COL])

        cols_to_join = [SPECIALTIES_COL, CONTEXT_REG_COL, JOINED_REG_COL, REG_SCORE_COL]
        filtered_bios_df['sentence_info'] = join_df_cols(filtered_bios_df, cols_to_join)
        filtered_bios_df.drop(cols_to_join, inplace=True, axis=1)
        group_by_cols = [PROFILE_URL_COL, SENTENCE_INDEX_COL, CONTENT_REG_COL, PRACTICE_AREAS_COL]
        grouped_bios = filtered_bios_df.groupby(group_by_cols)['sentence_info'] \
            .agg({'result': lambda x: tuple(
            [sent_info for sent_info in x if sent_info[3] == max([sent_info[3] for sent_info in x])][0])}) \
            .reset_index()

        split_cols = [SPECIALTIES_COL, CONTEXT_REG_COL, JOINED_REG_COL, REG_SCORE_COL]
        grouped_bios = split_data_frame_col(grouped_bios, split_cols, 'result')

        grouped_bios = grouped_bios.convert_objects(convert_numeric=True)
        grouped_bios[PRACTICE_AREAS_SCORE_COL] = grouped_bios.groupby([PROFILE_URL_COL, PRACTICE_AREAS_COL])[
            REG_SCORE_COL].transform('sum')
        return self.count_result(grouped_bios)

    def count_result(self, grouped_df):
        """
        :param grouped_df: DataFrame
        Method counts sum score for every practice area and removes unappropriate practice areas
        """

        cols_to_join = [SPECIALTIES_COL, PRACTICE_AREAS_COL, PRACTICE_AREAS_SCORE_COL]
        grouped_df['practice_area_info'] = join_df_cols(grouped_df, cols_to_join)
        grouped_df.drop(cols_to_join, inplace=True, axis=1)
        grouped_bios = grouped_df.groupby([PROFILE_URL_COL, SENTENCE_INDEX_COL])['practice_area_info'] \
            .agg({'result': lambda x: tuple(self.remove_conflicts(x, ))})
        grouped_bios = grouped_bios.fillna('')
        grouped_bios = split_data_frame_rows(grouped_bios, 'result')

        split_cols = [SPECIALTIES_COL, PRACTICE_AREAS_COL, REG_SCORE_COL]
        grouped_bios = split_data_frame_col(grouped_bios, split_cols, 'result').reset_index()

        cols_to_join = [SPECIALTIES_COL, PRACTICE_AREAS_COL, REG_SCORE_COL]
        grouped_bios['bio_full_info'] = join_df_cols(grouped_bios, cols_to_join)

        grouped_bios = grouped_bios.groupby([PROFILE_URL_COL])['bio_full_info'] \
            .agg({'test': lambda x: tuple(self.filter_by_practice_area_score(x, ))})

        grouped_bios = split_data_frame_rows(grouped_bios, 'test')

        split_cols = [SPECIALTIES_COL, PRACTICE_AREAS_COL, REG_SCORE_COL]
        grouped_bios = split_data_frame_col(grouped_bios, split_cols, 'test').reset_index()

        grouped_bios = grouped_bios.groupby([PROFILE_URL_COL])[SPECIALTIES_COL, PRACTICE_AREAS_COL].agg(
            {"Predictions": lambda x: ', '.join([i for i in set(x, ) if i])}).reset_index()
        return grouped_bios

    def remove_conflicts(self, sentence_info):
        """
        :param sentence_info: tuple, that contains information gotten for certain sentence: sets of speciality, practice
         area and practice area score
        :return: filtered tuple of tuples
        method removes potential practice areas that are incompatible with other selections.
        """
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
        """
        :param data_frame:
        :return:
        """
        max_scored_limit = 2
        sorted_data = sorted(set(data_frame), key=lambda x: int(x[2]), reverse=True)
        # print(sorted_data)
        appropriate_data = []
        for bio_info in sorted_data:
            if len(appropriate_data) < max_scored_limit or bio_info[2] == appropriate_data[-1][2]:
                appropriate_data.append(bio_info)
        return appropriate_data


if __name__ == "__main__":
    pass
