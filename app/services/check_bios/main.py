#!/usr/bin/python
# -*- coding: utf-8 -*

import multiprocessing

import numpy as np
from pandas import concat, DataFrame, set_option

from app.services.check_bios.handlers.io_data_handler import DataHandler
from app.services.check_bios.text_normalizer import sentences_splitter
from app.services.check_bios.handlers.df_handler import *


class Extractor(object):
    def __init__(self, joined_regexes, content_regexes):
        self.joined_regexes = joined_regexes
        self.content_regexes = content_regexes

    def get_ai_results(self, bios):
        set_option('display.max_colwidth', -1)
        # concatenated = self.filter_with_regex(bios)
        p = multiprocessing.Pool(4)
        pool_results = p.map(self.filter_with_regex, np.array_split(bios, 4))
        p.close()
        p.join()
        p.terminate()
        concatenated = concat(pool_results)
        return concatenated

    def filter_with_regex(self, bio_df):
        sentence_tokenized_bios = self.bio_df_sentence_tokenizing(bio_df)
        print("sentences splitted")
        unique_content_regexes_keys = set(self.joined_regexes.content_regex.values)
        all_content_regexes_dict = DataHandler.df_to_dict(self.content_regexes, "Content REG ID", "KeyWord")

        result = []
        for content_regex_key in unique_content_regexes_keys:
            print(content_regex_key)
            content_regex_value = all_content_regexes_dict.get(content_regex_key)
            content_filtered_bios = self.filter_bios(sentence_tokenized_bios, content_regex_value)

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
        return self.group_data(concat(result))

    def get_regex_info(self, regex_index):
        '''
        :param regex_index: str
        :return: tuple of practice area, specialty, score and context regex according to input regular expression index
        '''
        current_regex_info_df = self.joined_regexes[self.joined_regexes["regex_id"] == regex_index]
        pr_areas = list(set(current_regex_info_df["pract_areas"].values.tolist()))
        specialties = list(set(current_regex_info_df["specialties"].values.tolist()))
        score = list(set(current_regex_info_df["score"].values.tolist()))
        context_regex = list(set(current_regex_info_df["context_regex"].values.tolist()))
        regex_info = (pr_areas, specialties, score, context_regex)
        return regex_info

    def bio_df_sentence_tokenizing(self, df):
        '''
        :param df: DataFrame
        :return: DataFrame containing sentence splitted biographies. Each row contains bio url,
        separated sentence and its index.
        '''
        splitted_bios = concat([Series(row['profileUrl'], sentences_splitter(row['attorneyBio']))
                                for _, row in df.iterrows()]).reset_index()
        splitted_bios.columns = ["attorneyBio", "profileUrl"]
        splitted_bios[['sent_num', 'sentence']] = splitted_bios['attorneyBio'].apply(Series)
        return splitted_bios

    def filter_bios(self, df_to_filter, regex_for_filtering):
        '''
        :param df_to_filter: DataFrame
        :param regex_for_filtering: str
        :return: DataFrame, filtered with input regular expression
        '''
        try:
            df_to_filter_copy = df_to_filter.copy()
            filtered_bios = df_to_filter_copy[df_to_filter_copy['sentence'].str.contains(regex_for_filtering)]
            return filtered_bios
        except:
            print("wrong regex: " + regex_for_filtering)
            return DataFrame()

    def group_data(self, filtered_bios_df):
        print("result_processing started")
        filtered_bios_df['sentence_info'] = join_df_cols(filtered_bios_df,
                                                         ['specialties', 'context', 'joined', 'score'])

        grouped_bios = filtered_bios_df.groupby(['profileUrl', 'sent_num', 'content', 'practice_areas'])[
            'sentence_info'].agg(
            {'result': lambda x: tuple([i for i in x if i[3] == max([i[3] for i in x])][0])}).reset_index()

        grouped_bios = split_data_frame_col(grouped_bios, ['specialties', 'context_regex', 'joined', 'score'],
                                            'result')

        grouped_bios['specialty_info'] = join_df_cols(grouped_bios, ['practice_areas', 'score'])
        grouped_bios.drop(['practice_areas', 'score'], inplace=True, axis=1)

        test = grouped_bios.groupby(['profileUrl', 'sent_num', 'context_regex', 'specialties', 'joined'])[
            'specialty_info'].agg(
            {'main_info': lambda x: tuple(set(x, ))[0]}).reset_index()

        result = split_data_frame_col(test, ['practice_areas', 'specialty_score'], 'main_info')

        return self.count_result(result)

    def count_result(self, grouped_df):
        grouped_df = grouped_df.convert_objects(convert_numeric=True)
        grouped_df['practice_area_score'] = grouped_df.groupby(['profileUrl', 'practice_areas'])[
            'specialty_score'].transform('sum')
        grouped_df['practice_area_info'] = join_df_cols(grouped_df,
                                                        ['specialties', 'practice_areas', 'practice_area_score', 'sent_num', 'profileUrl'])

        grouped_bios = grouped_df.groupby(['profileUrl', 'sent_num'])[
            'practice_area_info'].agg(
            {'result': lambda x: tuple(self.remove_conflicts(x, ))})
        grouped_bios = split_data_frame_rows(grouped_bios, 'result')
        grouped_bios = split_data_frame_col(grouped_bios, ['specialty', 'pract_area', 'score', 's_num', 'pr_url'], 'result').reset_index()

        grouped_bios = grouped_bios.groupby(['profileUrl'])['specialty', 'pract_area'].agg(
            {"Predictions":lambda x: ', '.join([i for i in set(x, ) if i])}).reset_index()
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
            specialty, pract_area, score, sent_n, url = data
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
                unconflict_groups.extend(practice_only)if practice_only else unconflict_groups.extend(max_scored)

        return unconflict_groups


if __name__ == "__main__":
    pass
