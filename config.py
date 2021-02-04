# coding: utf-8

import os

class Config(object):
    APP_TITLE = 'Dados do Siconv'
    DATA_ENDPOINT = 'tables'
    COMPRESSION_METHOD = 'gzip'
    FILE_EXTENTION = '.csv.gz'
    TABLE_LIST = ['emendas', 'emendas_convenios', 'convenios', 'proponentes', 'movimento']

    DATA_FOLDER = os.getenv('DATA_FOLDER', default=os.path.join(os.path.realpath('..'), 'csv_files'))
    API_KEY_DISABLED = os.getenv('API_KEY_DISABLED', default=True)
    API_KEY = os.getenv('API_KEY')

config = Config()