# coding: utf-8
"""data_loaders.
   """

from flask import current_app as app
import pandas as pd
import os
from math import ceil


def pagination(page_specs, page_count, items_count):
    return {
        "page": page_specs.get('page'),
        "page_length": page_specs.get('page_length'),
        "page_count": page_count,
        "items_count": items_count
    }

def load_data(table_name, dtypes='object', parse_dates=False):
    data = pd.read_csv(os.path.join(app.config['DATA_FOLDER'], f'{table_name}{app.config["FILE_EXTENTION"]}'),
        compression=app.config['COMPRESSION_METHOD'], sep=';', decimal=',', dayfirst=True, dtype=dtypes,
        parse_dates=parse_dates)

    return data

def page_settings(obj, page_specs):
    page = page_specs.get('page') if page_specs and page_specs.get('page') else 1
    page_length = page_specs.get('page_length') if page_specs and page_specs.get('page_length') else 50
    items_count = len(obj)
    page_count = ceil(items_count / page_length)
    idx_first = (page - 1) * page_length
    idx_last = idx_first + page_length

    page_specs = {'page': page, 'page_length': page_length}

    return items_count, page_count, idx_first, idx_last, page_specs


