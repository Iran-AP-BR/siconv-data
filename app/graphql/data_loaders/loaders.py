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
    def __init__(self, table_name, dtypes, parse_dates=[], decimal=','):
        self.dtypes = dtypes
        self.parse_dates = parse_dates
        self.table_name = table_name
        self.decimal = decimal
        self.__current_date_var__ = f'{table_name.upper()}_CURRENT_DATE'
        self.__table_name_var__ = table_name.upper()

    def __pagination_(self, data_frame, page_specs):
        page = page_specs.get(
            'page') if page_specs and page_specs.get('page') else 1
        page_length = page_specs.get('page_length') if page_specs and page_specs.get(
            'page_length') else app.config.get('GRAPHQL_DEFAULT_PAGE_LENGTH')
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
                                                              compression=app.config['COMPRESSION_METHOD'], sep=';', decimal=self.decimal, dayfirst=True, dtype=self.dtypes,
                                                              parse_dates=self.parse_dates)

            app.config[self.__current_date_var__] = current_date

        return app.config.get(self.__table_name_var__)

    def load(self, page_specs=None, use_pagination=True, parameters=None, order_by=None):
        conditions = filter_constructor(
            parameters=parameters, dtypes=self.dtypes, parse_dates=self.parse_dates)
        data_frame = self.__load__()

        if conditions:
            data_frame = data_frame.query(conditions)

        if order_by:
            by = order_by.get('field')
            ascending = order_by.get('ascending')
            data_frame = data_frame.sort_values(by=by, ascending=ascending)

        if use_pagination:
            items_count, page_count, idx_first, idx_last, page_specs = self.__pagination_(
                data_frame, page_specs)
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

def join_dict(*d0):
    d0 = d0 and list(filter(lambda x: x, d0))
    if d0:
        d3 = d0[0].copy()
        for d1 in d0[1:]:
            if d1:
                for d in d1:           
                    if d in d3:
                        d3[d] = d3[d] if type(d3[d]) is list else [d3[d]]
                        d3[d] += d1[d] if type(d1[d]) is list else [d1[d]]
                    else:
                        d3[d] = d1[d]

        if d3:
            for d in d3:
                d3[d] = list(map(lambda x: f'({x})', d3[d])) if type(d3[d]) is list else d3[d]
                d3[d] = ' && '.join(d3[d]) if type(d3[d]) is list else d3[d]

         
            return d3

    return None


def load_convenios(page_specs=None, use_pagination=True, parameters=None, parent=None, order_by=None):

    params1 = None
    params2 = None
    params3 = None
    params4 = None

    convenios_loader = DataLoader(table_name='convenios', dtypes=dtypes_convenios, parse_dates=parse_dates_convenios)
    emendas_convenios_loader = DataLoader(table_name='emendas_convenios', dtypes=dtypes_emendas_convenios)

    if parent:

        if parent.get('NR_EMENDA'):

            params1 = {'NR_EMENDA': parent['NR_EMENDA']}
            emendas_convenios_didct, _ = emendas_convenios_loader.load(parameters=params1, use_pagination=False)
            params1 = {'NR_CONVENIO': 'in$' + '|'.join([emc['NR_CONVENIO'] for emc in emendas_convenios_didct])}

        else:

            params1 = {'IDENTIF_PROPONENTE': parent['IDENTIF_PROPONENTE']}


    if parameters:

        if parameters.get('MOVIMENTO'):

            params2 = parameters.pop('MOVIMENTO')
            movimentos_dict, _ = load_movimento(parameters=params2, use_pagination=False)
            params2 = {'NR_CONVENIO': 'in$' + '|'.join([mov['NR_CONVENIO'] for mov in movimentos_dict])}
    
        if parameters.get('EMENDAS'):

            params3 = parameters.pop('EMENDAS')
            emendas_dict, _ = load_emendas(parameters=params3, use_pagination=False)
            params3 = {'NR_EMENDA': 'in$' + '|'.join([emd['NR_EMENDA'] for emd in emendas_dict])}
            emendas_convenios_didct, _ = emendas_convenios_loader.load(parameters=params3, use_pagination=False)
            params3 = {'NR_CONVENIO': 'in$' + '|'.join([emc['NR_CONVENIO'] for emc in emendas_convenios_didct])}
  
        if parameters.get('PROPONENTE'):

            params4 = parameters.pop('PROPONENTE')
            proponentes_dict, _ = load_proponentes(parameters=params4, use_pagination=False)
            params4 = {'IDENTIF_PROPONENTE': 'in$' + '|'.join([prop['IDENTIF_PROPONENTE'] for prop in proponentes_dict])}
    

    parameters = join_dict(parameters, params1, params2, params3, params4)

    convenios, pagination = convenios_loader.load(page_specs=page_specs, parameters=parameters, order_by=order_by, use_pagination=use_pagination)

    return convenios, pagination


def load_emendas(page_specs=None, use_pagination=True, parameters=None, parent=None, order_by=None):

    params = None
    emendas_loader = DataLoader(table_name='emendas', dtypes=dtypes_emendas)
    emendas_convenios_loader = DataLoader(table_name='emendas_convenios', dtypes=dtypes_emendas_convenios)

    if parent:

        emendas_convenios_didct, _ = emendas_convenios_loader.load(parameters={'NR_CONVENIO': parent['NR_CONVENIO']}, use_pagination=False)
        params = {'NR_EMENDA': 'in$' + '|'.join([emc['NR_EMENDA'] for emc in emendas_convenios_didct])}
        

    parameters = join_dict(parameters, params)
    
    emendas, pagination = emendas_loader.load(page_specs=page_specs, parameters=parameters, order_by=order_by, use_pagination=use_pagination)

    return emendas, pagination


def load_proponentes(page_specs=None, use_pagination=True, parameters=None, parent=None, order_by=None):

    params = None
    proponentes_loader = DataLoader(table_name='proponentes', dtypes=dtypes_proponentes)

    if parent:

        if parent.get('IDENTIF_PROPONENTE'):

            params = {'IDENTIF_PROPONENTE': parent['IDENTIF_PROPONENTE']}

        else:

            params = {'COD_MUNIC_IBGE': parent['codigo_ibge']}

    parameters = join_dict(parameters, params)

    proponentes, pagination = proponentes_loader.load(page_specs=page_specs, parameters=parameters, order_by=order_by, use_pagination=use_pagination)

    return proponentes, pagination


def load_movimento(page_specs=None, use_pagination=True, parameters=None, parent=None, order_by=None):

    params = None
    movimento_loader = DataLoader(table_name='movimento', dtypes=dtypes_movimento, parse_dates=parse_dates_movimento)

    if parent:

        params={'NR_CONVENIO': parent['NR_CONVENIO']}


    parameters = join_dict(parameters, params)
  
    movimento, pagination = movimento_loader.load(page_specs=page_specs, parameters=parameters, order_by=order_by, use_pagination=use_pagination)

    return movimento, pagination


def load_municipios(page_specs=None, use_pagination=True, parameters=None, parent=None, order_by=None):

    params = None
    municipios_loader = DataLoader(table_name='municipios', dtypes=dtypes_municipios, decimal='.')

    if parent:

        params = {'codigo_ibge': parent['COD_MUNIC_IBGE']}


    parameters = join_dict(parameters, params)

    municipios, pagination = municipios_loader.load(page_specs=page_specs, parameters=parameters, order_by=order_by, use_pagination=use_pagination)

    return municipios, pagination
