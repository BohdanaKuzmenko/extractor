#!/usr/bin/python
# -*- coding: utf-8 -*
import pandas as pd
import csv


class DataHandler(object):
    @staticmethod
    def get_db_values(engine, query):
        return pd.read_sql_query(query, engine, chunksize=1000000)

    @staticmethod
    def chunk_to_csv(chunk, file_name, header=False, index=False, mode = 'a'):
        chunk.to_csv(file_name, mode=mode, encoding='utf-8', header=header, index=index, sep="\t", chunksize=1000000)

    @staticmethod
    def db_to_csv(engine, file_name, query, header=False, index=False):
        for chunk in pd.read_sql_query(query, engine, chunksize=1000000):
            chunk.to_csv(file_name, mode='a', header=header, index=index, sep="\t")

    @staticmethod
    def get_db_values(engine, query):
        return pd.read_sql_query(query, engine, chunksize=1000000)

    @staticmethod
    def get_csv_values(file_name):
        return pd.read_csv(file_name, chunksize=1000000, sep="\t", quoting=csv.QUOTE_NONE, encoding='utf-8')

    @staticmethod
    def db_to_db(engine_from, engine_to, query, table_name):
        for chunk in pd.read_sql_query(query, engine_from, chunksize=1000000):
            chunk.to_sql(name=table_name, con=engine_to)

    @staticmethod
    def chunk_to_db(engine_to, chunk, table_name):
        chunk.to_sql(name=table_name, con=engine_to)

    @staticmethod
    def chunk_to_exel(chunk, file_name, header=False, index=False, mode='a'):
        writer = pd.ExcelWriter(file_name)
        chunk.to_excel(writer, 'Sheet1', engine='xlsxwriter', header=header, index=index)
        writer.save()

if __name__ == '__main__':
    print(next(DataHandler.get_csv_values('full_data.csv')))
