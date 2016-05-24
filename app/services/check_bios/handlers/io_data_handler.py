#!/usr/bin/python
# -*- coding: utf-8 -*
import csv

import pandas as pd

from app.services.check_bios.handlers.drive_handler import *


class DataHandler(object):
    @staticmethod
    def get_db_values(engine, query):
        '''
        :param engine: db engine
        :param query: str, sql query
        :return: DataFrame of data according to query
        '''
        return pd.read_sql_query(query, engine, chunksize=1000000)

    @staticmethod
    def chunk_to_csv(chunk, file_name, header=False, index=False, mode='a'):
        '''
        :param chunk: DataFrame of data, needed to be written to file
        :param file_name: str
        :param header: bool
        :param index: bool
        :param mode: str
        :return:None
        '''
        chunk.to_csv(file_name, mode=mode, encoding='utf-8', header=header, index=index, sep="\t", chunksize=1000000)

    @staticmethod
    def db_to_csv(engine, file_name, query, header=False, index=False):
        '''
        :param engine: sql engine
        :param file_name: str
        :param query: str, sql query for getting neede data
        :param header: bool
        :param index: bool
        :return: None
        '''
        for chunk in pd.read_sql_query(query, engine, chunksize=1000000):
            chunk.to_csv(file_name, mode='a', header=header, index=index, sep="\t")

    @staticmethod
    def get_db_values(engine, query):
        '''
        :param engine: db engine
        :param query: str, sql query for getting needed data
        :return: DataFrame
        '''
        return pd.read_sql_query(query, engine, chunksize=1000000)

    @staticmethod
    def get_csv_values(file_name):
        '''
        :param file_name: str
        :return: DataFrame of file values
        '''
        return pd.read_csv(file_name, chunksize=1000000, sep="\t", quoting=csv.QUOTE_NONE, encoding='utf-8')

    @staticmethod
    def db_to_db(engine_from, engine_to, query, table_name):
        '''
        :param engine_from: db engine
        :param engine_to: db engine
        :param query: str
        :param table_name: str
        :return: None
        '''
        for chunk in pd.read_sql_query(query, engine_from, chunksize=1000000):
            chunk.to_sql(name=table_name, con=engine_to)

    @staticmethod
    def chunk_to_db(engine, chunk, table_name):
        '''
        :param engine: db engine
        :param chunk: DataFrame
        :param table_name: str
        :return: None
        '''
        chunk.to_sql(name=table_name, con=engine)

    @staticmethod
    def chunk_to_exel(chunk, file_name, header=False, index=False, mode='a'):
        '''
        :param chunk: DataFrame
        :param file_name: str
        :param header: bool
        :param index: bool
        :param mode: str
        :return: None
        '''
        writer = pd.ExcelWriter(file_name)
        chunk.to_excel(writer, 'Sheet1', engine='xlsxwriter', header=header, index=index)
        writer.save()


    @staticmethod
    def get_spread_sheet_values(spread_sheet_id, sheet_name):
        '''
        :param spread_sheet_id: str
        :param sheet_name: str
        :return: DataFrame of spreadsheet values
        '''
        spread_sheet = get_spread_sheet_data(spread_sheet_id)
        sheet = spread_sheet.worksheet(sheet_name).get_all_values()
        sub_regex_df = pd.DataFrame(sheet[1:], columns=sheet[0])
        return sub_regex_df

    @staticmethod
    def df_to_dict(data_frame, key_col, value_col):
        '''
        :param data_frame: DataFrame
        :param key_col: str, key column
        :param value_col: list of values columns
        :return: dict
        '''
        return data_frame.set_index(key_col)[value_col].to_dict()



if __name__ == '__main__':
    pass
