# coding: utf-8

import os

SITE_ROOT = os.path.realpath(os.path.dirname(__file__))
DATA_FOLDER = os.path.join(SITE_ROOT, "files")
API_KEY = os.getenv('API_KEY')
DATA_ENDPOINT = 'data'

COMPRESSION_METHOD = 'gzip'
FILE_EXTENTION = '.csv.gz'