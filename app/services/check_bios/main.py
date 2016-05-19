#!/usr/bin/python
# -*- coding: utf-8 -*

import multiprocessing
from pandas import concat, DataFrame, Series, set_option
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
        concatenated['profileUrl'] = concatenated['profileUrl'].apply(
            lambda x: '<p class = "link"><a href="{}">{}</a></p>'.format(x, x))
        concatenated['sentence'] = concatenated['sentence'].apply(lambda x: '<p class = "test">{}</p>'.format(x))
        return concatenated[
            ['profileUrl', 'sentence', 'sent_num', 'content', 'context', 'joined', 'practice_areas',
             'specialties', 'score']]

    def filter_with_regex(self, bio_df):
        splitted_bios = self.split_bio_df(bio_df)

        unique_content_regexes_keys = set(self.joined_regexes.content_regex.values)
        all_content_regexes_dict = DataHandler.df_to_dict(self.content_regexes, "Content REG ID", "KeyWord")

        result = []
        for content_regex_key in unique_content_regexes_keys:
            content_regex_value = all_content_regexes_dict.get(content_regex_key)
            content_filtered_bios = self.filter_bios(splitted_bios, content_regex_value)

            joined_regexes = self.joined_regexes[self.joined_regexes["content_regex"] == content_regex_key][[
                "regex", "regex_id"]].values.tolist()
            for regex in joined_regexes:
                joined_regex_value, joined_regex_index = regex
                bio_df = self.filter_bios(content_filtered_bios, joined_regex_value)

                if not bio_df.empty:
                    pr_areas, specialties, score, context_regex = self.get_regex_info(joined_regex_value)

                    bio_df['content'] = DataFrame(
                        [content_regex_key] * len(bio_df.attorneyBio.values)).values
                    bio_df['context'] = DataFrame([context_regex] * len(bio_df.attorneyBio.values)).values
                    bio_df['joined'] = DataFrame([joined_regex_index] * len(bio_df.attorneyBio.values)).values
                    bio_df['practice_areas'] = DataFrame(pr_areas * len(bio_df.attorneyBio.values)).values
                    bio_df['specialties'] = DataFrame(specialties * len(bio_df.attorneyBio.values)).values
                    bio_df['score'] = DataFrame(score * len(bio_df.attorneyBio.values)).values
                    result.append(bio_df)
        return concat(result)

    def get_regex_info(self, regex_value):
        current_regex_info_df = self.joined_regexes[self.joined_regexes["regex"] == regex_value]
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


if __name__ == "__main__":
    pass
