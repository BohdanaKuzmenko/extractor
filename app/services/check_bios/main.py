#!/usr/bin/python
# -*- coding: utf-8 -*
import pandas as pd
from app.services.check_bios.data_handler import DataHandler
from app.services.check_bios.predictor import Predictor
from app.services.check_bios.pr_areas_spec_handler import *


def predict(bios, regexes):
    pd.set_option('display.max_colwidth', -1)
    df_predictions = pd.DataFrame()
    df_no_extractions = pd.DataFrame()
    for bio in bios:
        link, full_bio, db_practice_areas, db_specialties = bio
        print(link)
        db_practice_areas, db_specialties, full_bio = db_practice_areas.lower().split(
            ","), db_specialties.lower().split(","), ' '.join(full_bio.split())
        sentences_with_regexes = extract_sentences(full_bio, regexes, limit=None)
        if not sentences_with_regexes:
            data_frame = pd.DataFrame([link], columns=["link"])
            data_frame["full_bio"] = pd.DataFrame([full_bio]).values
            data_frame["practice_areas"] = pd.DataFrame([', '.join(db_practice_areas)]).values
            data_frame["specialties"] = pd.DataFrame([', '.join(db_specialties)]).values
            df_no_extractions = pd.concat([df_no_extractions, data_frame], ignore_index=True)
        else:
            gotten_pr_areas = []
            gotten_spec = []
            sentences_frames = []
            for value in sentences_with_regexes:
                index, sentence, regexes, indexes = value
                predicted_areas, predicted_spec = Predictor.get_predictions(
                    'app/services/check_bios/Master_list_for_PA.csv', sentence)
                gotten_pr_areas.extend(predicted_areas)
                gotten_spec.extend(predicted_spec)
                data_frame = pd.DataFrame([link], columns=["link"])
                data_frame["full_bio"] = pd.DataFrame([full_bio]).values
                data_frame["sentences_num"] = pd.DataFrame([len(sentences_with_regexes)]).values
                data_frame["sentence_index"] = pd.DataFrame([index]).values
                data_frame["sentence"] = pd.DataFrame([sentence]).values
                data_frame["regexes"] = pd.DataFrame([regexes])
                data_frame["regexes_index"] = pd.DataFrame([indexes])
                data_frame["practice_areas"] = pd.DataFrame([', '.join(db_practice_areas)]).values
                data_frame["predicted_pract_area"] = pd.DataFrame([', '.join(set(predicted_areas))]).values
                data_frame["specialties"] = pd.DataFrame([', '.join(db_specialties)]).values
                data_frame["predicted_specialities"] = pd.DataFrame([', '.join(set(predicted_spec))]).values
                sentences_frames.append(data_frame)
                df_predictions = pd.concat([df_predictions, data_frame], ignore_index=True)
    DataHandler.chunk_to_csv(df_predictions, "app/static/test.csv", header=True, mode='w')
    DataHandler.chunk_to_csv(df_no_extractions, "app/static/test1.csv", header=True, mode='w')
    return df_predictions


def get_bios(source, source_text):
    if source == "from_file":
        return next(DataHandler.get_csv_values('app/models/full_data.xlsx')).fillna('').values.tolist()
    if source == "from_url":
        urls = ([url.replace('\r', '') for url in source_text.split('\n')])
        df = next(DataHandler.get_csv_values('app/models/full_data.csv'))
        return df[df['profileUrl'].isin(urls)].fillna('').values.tolist()
    if source == "from_text":
        return [('no_link', source_text, 'no_pract_area', 'no_specialities')]


def get_regexes(raw_regex):
    if raw_regex:
        return [(r.replace('\r', '')) for r in raw_regex.split('\n')]
