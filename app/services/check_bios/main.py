#!/usr/bin/python
# -*- coding: utf-8 -*
import pandas as pd
from app.services.check_bios.data_handler import DataHandler
from app.services.check_bios.predictor import Predictor
from app.services.check_bios.pr_areas_spec_handler import *


class BiosExtractor(object):
    def __init__(self, regexes):
        self.regexes = regexes

    def predict(self, bio):
        pd.set_option('display.max_colwidth', -1)
        link, full_bio = bio
        print(link)
        full_bio = ' '.join(full_bio.split())
        sentences_with_regexes = extract_sentences(link, full_bio, self.regexes)
        print(sentences_with_regexes)
        return (sentences_with_regexes)


if __name__ == "__main__":
    pass
