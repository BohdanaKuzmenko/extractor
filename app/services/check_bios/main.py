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
        concatenated['profileUrl'] = concatenated['profileUrl'].apply(lambda x: '<p class = "link"><a href="{}">{}</a></p>'.format(x, x))
        concatenated['sentence'] = concatenated['sentence'].apply(lambda x: '<p class = "test">{}</p>'.format(x))
        return concatenated[['profileUrl', 'sentence', 'sent_num',  'practice_areas', 'specialties', 'score']]

    def filter_with_regex(self, bio_df):
        splitted_bios = concat([Series(row['profileUrl'], sentences_splitter(row['attorneyBio']))
                                for _, row in bio_df.iterrows()]).reset_index()
        splitted_bios.columns = ["attorneyBio", "profileUrl"]
        splitted_bios[['sent_num', 'sentence']] = splitted_bios['attorneyBio'].apply(Series)

        unique_content_regexes_keys = set(self.joined_regexes.content_regex.values)
        content_regexes_dict = DataHandler.df_to_dict(self.content_regexes, "Content REG ID", "KeyWord")

        result = []

        for content_regex_key in unique_content_regexes_keys:
            content_regex = content_regexes_dict.get(content_regex_key)
            df_for_content_filtering = splitted_bios.copy()
            cn_filtered_bios = df_for_content_filtering[
                df_for_content_filtering['sentence'].str.contains(content_regex)]

            joined_regexes = self.joined_regexes[self.joined_regexes["content_regex"] == content_regex_key][
                "regex"].values.tolist()

            for regex in joined_regexes:
                print(regex)
                df_for_foined_filetering = cn_filtered_bios.copy()
                regex_df = df_for_foined_filetering[df_for_foined_filetering['sentence'].str.contains(regex)]
                if not regex_df.empty:
                    current_regex_info_df = self.joined_regexes[self.joined_regexes["regex"] == regex]
                    pr_areas = list(set(current_regex_info_df["pract_areas"].values.tolist()))
                    score = list(set(current_regex_info_df["score"].values.tolist()))
                    specialties = list(set(current_regex_info_df["specialties"].values.tolist()))

                    regex_df['practice_areas'] = DataFrame(pr_areas * len(regex_df.attorneyBio.values)).values
                    regex_df['specialties'] = DataFrame(specialties * len(regex_df.attorneyBio.values)).values
                    regex_df['score'] = DataFrame(score * len(regex_df.attorneyBio.values)).values
                    result.append(regex_df)

        return concat(result)



if __name__ == "__main__":
    pass
