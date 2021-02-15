# coding: utf-8

import os

class Config(object):
    APP_TITLE = 'Dados do Siconv'
    DATA_MODEL_PAGE_TITLE = 'Diagrama da Base de Dados'
    DATA_ENDPOINT = 'tables'
    COMPRESSION_METHOD = 'gzip'
    FILE_EXTENTION = '.csv.gz'
    CURRENT_DATE_FILENAME = 'data_atual.txt'
    TABLE_LIST = ['emendas', 'emendas_convenios', 'convenios', 'proponentes', 'movimento', 'municipios']

    DATA_FOLDER = os.getenv('DATA_FOLDER', default=os.path.join(os.path.realpath('..'), 'csv_files'))
    API_KEY_ENABLED = os.getenv('API_KEY_ENABLED', default=False)
    API_KEY = os.getenv('API_KEY')

    GRAPHQL_DEFAULT_PAGE_LENGTH = 50
    GRAPHQL_COMMAND_DELIMITER = '$'
    GRAPHQL_LIST_DELIMITER = '|'
    GRAPHQL_NEGATION_MARK = '!'


