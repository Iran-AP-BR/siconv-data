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

    DATA_FOLDER = os.getenv('DATA_FOLDER')
    API_KEY_ENABLED = True if os.getenv('API_KEY_ENABLED', default='false').lower()=='true' else False
    API_KEY = os.getenv('API_KEY')
    
    SQLALCHEMY_DATABASE_URI = os.getenv('SQLALCHEMY_DATABASE_URI')
    SQLALCHEMY_TRACK_MODIFICATIONS = True if os.getenv('SQLALCHEMY_TRACK_MODIFICATIONS', default='false').lower()=='true' else False
    SQLALCHEMY_POOL_RECYCLE = int(os.getenv('SQLALCHEMY_POOL_RECYCLE')) if os.getenv('SQLALCHEMY_POOL_RECYCLE') else None
    SQLALCHEMY_POOL_TIMEOUT = int(os.getenv('SQLALCHEMY_POOL_TIMEOUT')) if os.getenv('SQLALCHEMY_POOL_TIMEOUT') else None

    GRAPHQL_DEFAULT_PAGE_LENGTH = 50

    CURRENT_DATE_URI = os.getenv('CURRENT_DATE_URI')
    DOWNLOAD_URI = os.getenv('DOWNLOAD_URI')
    CHUNK_SIZE=int(os.getenv('CHUNK_SIZE', default=10000))