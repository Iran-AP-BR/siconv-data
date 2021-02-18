# coding: utf-8
"""loaders.
   """

import pandas as pd
from .dtypes import *
from flask import current_app as app
from app.graphql.data_loaders.filtering import filter_constructor
import os
from math import ceil


class DataLoader(object):
    def __init__(self, table_name, dtypes, parse_dates=[]):
        self.dtypes = dtypes
        self.parse_dates = parse_dates
        self.table_name = table_name
        self.__current_date_var__ = f'{table_name.upper()}_CURRENT_DATE'
        self.__table_name_var__ = table_name.upper()


    def __pagination_(self, data_frame, page_specs):
        page = page_specs.get('page') if page_specs and page_specs.get('page') else 1
        page_length = page_specs.get('page_length') if page_specs and page_specs.get('page_length') else app.config.get('GRAPHQL_DEFAULT_PAGE_LENGTH')
        items_count = len(data_frame)
        page_count = ceil(items_count / page_length)

        if page > page_count:
            page = page_count
            
        if page == 0:
            page_length = 0

        idx_first = (page - 1) * page_length if page > 0 else 0
        idx_last = idx_first + page_length

        page_specs = {'page': page, 'page_length': page_length}

        return items_count, page_count, idx_first, idx_last, page_specs


    def __load__(self):
        # Get tables' current date
        with open(os.path.join(app.config.get('DATA_FOLDER'), app.config.get('CURRENT_DATE_FILENAME')), 'r') as fd:
            current_date = fd.read()

        if app.config.get(self.__current_date_var__) != current_date or app.config.get(self.__table_name_var__) is None:
            
            app.config[self.__table_name_var__] = pd.read_csv(os.path.join(app.config['DATA_FOLDER'], f'{self.table_name}{app.config["FILE_EXTENTION"]}'),
                compression=app.config['COMPRESSION_METHOD'], sep=';', decimal=',', dayfirst=True, dtype=self.dtypes,
                parse_dates=self.parse_dates)
            
            app.config[self.__current_date_var__] = current_date

        return app.config.get(self.__table_name_var__)


    def load(self, page_specs=None, use_pagination=True, parameters=None, order_by=None):
        conditions = filter_constructor(parameters=parameters, dtypes=self.dtypes, parse_dates=self.parse_dates)
        data_frame = self.__load__()

        if conditions:
            data_frame = data_frame.query(conditions)

        if order_by:
            by = order_by.get('field')
            ascending = order_by.get('ascending')
            data_frame = data_frame.sort_values(by=by, ascending=ascending)
            
        if use_pagination:
            items_count, page_count, idx_first, idx_last, page_specs = self.__pagination_(data_frame, page_specs)
            data_frame = data_frame[idx_first:idx_last]

        data_frame_dict = data_frame.to_dict('records')

        pagination = None
        if page_specs:
            pagination = {
                          "page": page_specs.get('page'),
                          "page_length": page_specs.get('page_length'),
                          "page_count": page_count,
                          "items_count": items_count
                          }

        return data_frame_dict, pagination



##############################################

def load_convenios(page_specs=None, use_pagination=True, parameters=None, obj=None, order_by=None):
    
    convenios_loader = DataLoader(table_name='convenios', dtypes=dtypes_convenios, parse_dates=parse_dates_convenios)
    emendas_convenios_loader = DataLoader(table_name='emendas_convenios', dtypes=dtypes_emendas_convenios)

    if obj:
        if obj.get('IDENTIF_PROPONENTE'):
            convenios, pagination = convenios_loader.load(page_specs=page_specs, 
                                  parameters={'IDENTIF_PROPONENTE': obj.get('IDENTIF_PROPONENTE')}, order_by=order_by)
        else:
            ec_d = emendas_convenios_loader.load(parameters={'NR_EMENDA': obj.get('NR_EMENDA')})
            p = 'in$' + '|'.join([em['NR_CONVENIO'] for em in ec_d])
            convenios, pagination = convenios_loader.load(page_specs=page_specs, parameters={'NR_CONVENIO': p}, order_by=order_by)
    else:
        convenios, pagination = convenios_loader.load(page_specs=page_specs, parameters=parameters, order_by=order_by)
    
    return convenios, pagination

def load_emendas(page_specs=None, use_pagination=True, parameters=None, obj=None, order_by=None):
    
    emendas_loader = DataLoader(table_name='emendas', dtypes=dtypes_emendas)
    emendas_convenios_loader = DataLoader(table_name='emendas_convenios', dtypes=dtypes_emendas_convenios)

    if obj:
        emendas_convenios_didct = emendas_convenios_loader.load(parameters={'NR_CONVENIO': obj.get('NR_CONVENIO')})
        emendas, pagination = emendas_loader.load(page_specs=page_specs, 
                                               parameters={'NR_EMENDA': 'in$' + '|'.join([emc['NR_EMENDA']
                                                for emc in emendas_convenios_didct])}, order_by=order_by)
    else:
        emendas, pagination = emendas_loader.load(page_specs=page_specs, parameters=parameters, order_by=order_by)
    
    return emendas, pagination


def load_proponentes(page_specs=None, use_pagination=True, parameters=None, order_by=None):
    
    proponentes_loader = DataLoader(table_name='proponentes', dtypes=dtypes_proponentes)

    proponentes, pagination = proponentes_loader.load(page_specs=page_specs, parameters=parameters, order_by=order_by)

    return proponentes, pagination


def load_movimento(page_specs=None, use_pagination=True, parameters=None, obj=None, order_by=None):
    
    movimento_loader = DataLoader(table_name='movimento', dtypes=dtypes_movimento, parse_dates=parse_dates_movimento)

    if obj:
        movimento, pagination = movimento_loader.load(page_specs=page_specs, parameters={'NR_CONVENIO': obj['NR_CONVENIO']}, order_by=order_by)
    else:
        movimento, pagination = movimento_loader.load(page_specs=page_specs, parameters=parameters, order_by=order_by)

    return movimento, pagination
