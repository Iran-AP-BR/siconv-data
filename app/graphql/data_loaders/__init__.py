# coding: utf-8
"""data_loaders.
   """

from flask import current_app as app
import pandas as pd
import os
from math import ceil

def pagination(page, page_length, page_count, items_count):
    return {
        "page": page,
        "page_length": page_length,
        "page_count": page_count,
        "items_count": items_count
    }

def load_data(table_name):
    data = pd.read_csv(os.path.join(app.config['DATA_FOLDER'], f'{table_name}{app.config["FILE_EXTENTION"]}'),
        compression=app.config['COMPRESSION_METHOD'], sep=';', dtype=str).fillna('')

    return data

def page_settings(obj, page, page_length):
    items_count = len(obj)
    page_count = ceil(items_count / page_length)
    idx_first = (page-1)*page_length
    idx_last = idx_first + page_length

    return items_count, page_count, idx_first, idx_last
