# coding: utf-8

import os

class Config(object):
    APP_TITLE = 'Dados do Siconv'
    DATA_FOLDER = os.getenv('DATA_FOLDER', default=os.path.join(os.path.realpath('..'), 'csv_files'))
    STATIC_FOLDER = os.path.join(os.path.realpath(os.path.dirname(__file__)), 'app', 'static')
    API_KEY = os.getenv('API_KEY')
    DATA_ENDPOINT = 'tables'
    COMPRESSION_METHOD = 'gzip'
    FILE_EXTENTION = '.csv.gz'
    TABLE_LIST = ['emendas', 'emendas_convenios', 'convenios', 'proponentes', 'movimento']


config = Config()