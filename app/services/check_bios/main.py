#!/usr/bin/python
# -*- coding: utf-8 -*

from app.services.check_bios.bios_extractor import BiosExtractor
from app.services.check_bios.pr_areas_spec_handler import *
from app.services.check_bios.data_filter import *
import multiprocessing
from pandas import concat


def get_results(bios, regexes, specialities_regex_filter):
    ldb_result = get_bios_per_spec(specialities_regex_filter)

    ai_result = DataFrame()
    if bios and regexes:
        predictor = BiosExtractor(regexes)
        pool = multiprocessing.Pool(1)
        table = pool.map(predictor.predict, bios)
        ai_result = concat(table, ignore_index=True)
    return ai_result, ldb_result




if __name__ == "__main__":
    pass
