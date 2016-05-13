from pandas import set_option
from app.services.check_bios.pr_areas_spec_handler import *

class BiosExtractor(object):
    def __init__(self, regexes):
        self.regexes = regexes

    def predict(self, bio):
        set_option('display.max_colwidth', -1)
        link, full_bio = bio
        print(link)
        full_bio = ' '.join(full_bio.split())
        sentences_with_regexes = extract_sentences(link, full_bio, self.regexes)
        print(sentences_with_regexes)
        return (sentences_with_regexes)
