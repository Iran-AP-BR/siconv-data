# coding: utf-8
"""data_loaders.
   """

from flask import current_app as app
import pandas as pd
import os
from math import ceil


def get_current_tables_date():
    with open(os.path.join(app.config.get('DATA_FOLDER'), app.config.get('CURRENT_DATE_FILENAME')), 'r') as fd:
        return fd.read()


def load_data(table_name, dtypes='object', parse_dates=False):
    current_date_var = f'{table_name.upper()}_CURRENT_DATE'
    table_name_var = table_name.upper()
    current_date = get_current_tables_date()

    if app.config.get(current_date_var) != current_date or app.config.get(table_name_var) is None:
        app.config[table_name_var] = pd.read_csv(os.path.join(app.config['DATA_FOLDER'], f'{table_name}{app.config["FILE_EXTENTION"]}'),
            compression=app.config['COMPRESSION_METHOD'], sep=';', decimal=',', dayfirst=True, dtype=dtypes,
            parse_dates=parse_dates)
        
        app.config[current_date_var] = current_date

    return app.config.get(table_name_var)

def page_settings(obj, page_specs):
    page = page_specs.get('page') if page_specs and page_specs.get('page') else 1
    page_length = page_specs.get('page_length') if page_specs and page_specs.get('page_length') else app.config.get('GRAPHQL_DEFAULT_PAGE_LENGTH')
    items_count = len(obj)
    page_count = ceil(items_count / page_length)
    idx_first = (page - 1) * page_length
    idx_last = idx_first + page_length

    page_specs = {'page': page, 'page_length': page_length}

    return items_count, page_count, idx_first, idx_last, page_specs

def pagination(page_specs, page_count, items_count):
    return {
        "page": page_specs.get('page'),
        "page_length": page_specs.get('page_length'),
        "page_count": page_count,
        "items_count": items_count
    }
