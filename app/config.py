# coding: utf-8

import os

DATA_FOLDER = os.getenv('DATA_FOLDER')
STATIC_FOLDER = os.path.join(os.path.realpath(os.path.dirname(__file__)), 'static')
API_KEY = os.getenv('API_KEY')
DATA_ENDPOINT = 'data'

COMPRESSION_METHOD = 'gzip'
FILE_EXTENTION = '.csv.gz'