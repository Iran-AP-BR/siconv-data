# coding: utf-8

import os

class Config(object):
    
    APP_TITLE = os.getenv('APP_TITLE', default='...')
    DATA_MODEL_PAGE_TITLE = os.getenv('DATA_MODEL_PAGE_TITLE', default='...')
    DATA_ENDPOINT = os.getenv('DATA_ENDPOINT', default='tables')
    
    COMPRESSION_METHOD = os.getenv('COMPRESSION_METHOD', default='gzip')
    FILE_EXTENTION = os.getenv('FILE_EXTENTION', default='.csv.gz')

    TABLE_LIST = ['emendas', 'emendas_convenios', 'convenios', 'proponentes', 'movimento', 'municipios']

    DATA_FOLDER = os.getenv('DATA_FOLDER')
    STAGE_FOLDER = os.getenv('STAGE_FOLDER')
    MUNICIPIOS_BACKUP_FOLDER = os.getenv('MUNICIPIOS_BACKUP_FOLDER')
    
    API_KEY_ENABLED = True if os.getenv('API_KEY_ENABLED', default='false').lower()=='true' else False
    API_KEY = os.getenv('API_KEY')
    
    DATABASE_REQUIRED = True if os.getenv('DATABASE_REQUIRED', default='false').lower() == 'true' else False
    SQLALCHEMY_DATABASE_URI = os.getenv('SQLALCHEMY_DATABASE_URI')
    SQLALCHEMY_TRACK_MODIFICATIONS = True if os.getenv('SQLALCHEMY_TRACK_MODIFICATIONS', default='false').lower()=='true' else False
    SQLALCHEMY_POOL_RECYCLE = int(os.getenv('SQLALCHEMY_POOL_RECYCLE')) if os.getenv('SQLALCHEMY_POOL_RECYCLE') else None
    SQLALCHEMY_POOL_TIMEOUT = int(os.getenv('SQLALCHEMY_POOL_TIMEOUT')) if os.getenv('SQLALCHEMY_POOL_TIMEOUT') else None

    GRAPHQL_DEFAULT_PAGE_LENGTH = int(os.getenv('GRAPHQL_DEFAULT_PAGE_LENGTH', default=50))

    CURRENT_DATE_URI = os.getenv('CURRENT_DATE_URI')
    DOWNLOAD_URI = os.getenv('DOWNLOAD_URI')
    CHUNK_SIZE=int(os.getenv('CHUNK_SIZE', default=10000))

    TIMEZONE=os.getenv('TIMEZONE', default="UTC")