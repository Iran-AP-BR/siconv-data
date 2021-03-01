# coding: utf-8
"""loaders.
   """

import pandas as pd
from .dtypes import *
from flask import current_app as app
from app.graphql.data_loaders.filtering import filter_constructor
import os
from math import ceil


convenios_settings = {
    'table_name': 'convenios',
    'dtypes': dtypes_convenios, 
    'parse_dates': parse_dates_convenios,
    'auxiliary_table': {
                        'name': 'emendas_convenios',
                        'dtypes': dtypes_emendas_convenios,
                        'pivot_column': 'NR_CONVENIO',
                        'data_column': 'NR_EMENDA',
                        'aggregated_column': 'EMENDAS'
                        }
    }

emendas_settings = {
    'table_name': 'emendas',
    'dtypes': dtypes_emendas, 
    'parse_dates': [],
    'auxiliary_table': {
                        'name': 'emendas_convenios',
                        'dtypes': dtypes_emendas_convenios,
                        'pivot_column': 'NR_EMENDA',
                        'data_column': 'NR_CONVENIO',
                        'aggregated_column': 'CONVENIOS'
                        }
    }

proponentes_settings = {
    'table_name': 'proponentes',
    'dtypes': dtypes_proponentes, 
    'parse_dates': [],
    'auxiliary_table': None
    }

movimento_settings = {
    'table_name': 'movimento',
    'dtypes': dtypes_movimento, 
    'parse_dates': parse_dates_movimento,
    'auxiliary_table': None,
    'decimal': ','
    }

municipios_settings = {
    'table_name': 'municipios',
    'dtypes': dtypes_municipios, 
    'parse_dates': [],
    'auxiliary_table': None,
    'decimal': '.'
    }



def get_current_date():
    with open(os.path.join(app.config.get('DATA_FOLDER'), app.config.get('CURRENT_DATE_FILENAME')), 'r') as fd:
        return fd.read().strip()


class DataLoader(object):
    def __init__(self, table_name, dtypes, parse_dates=[], decimal=',', auxiliary_table=None):
        self.dtypes = dtypes
        self.parse_dates = parse_dates
        self.table_name = table_name
        self.auxiliary_table = auxiliary_table
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

    def __read_data__(self, tbl_name=None, dtypes=None, parse_dates=[]):
        tbl = pd.read_csv(os.path.join(app.config['DATA_FOLDER'], f'{tbl_name}{app.config["FILE_EXTENTION"]}'),
                                                            compression=app.config['COMPRESSION_METHOD'], sep=';',
                                                            decimal=self.decimal, dayfirst=True, dtype=dtypes,
                                                            parse_dates=parse_dates)
        
        return tbl

    def __aggregate_auxiliary_table__(self, table):

        tbl = self.__read_data__(self.auxiliary_table.get('name'), self.auxiliary_table.get('dtypes'))

        tbl[self.auxiliary_table.get('aggregated_column')] = tbl[[self.auxiliary_table.get('pivot_column'), self.auxiliary_table.get('data_column')]].groupby(by=self.auxiliary_table.get('pivot_column')).transform(lambda x: ','.join(x))
        tbl = tbl[[self.auxiliary_table.get('pivot_column'), self.auxiliary_table.get('aggregated_column')]].drop_duplicates()

        table = table.merge(tbl, how='left', on=self.auxiliary_table.get('pivot_column'))

        return table

    def __load__(self):
        current_date = get_current_date()

        if app.config.get(self.__current_date_var__) != current_date or app.config.get(self.__table_name_var__) is None:

            tbl = self.__read_data__(self.table_name, self.dtypes, self.parse_dates)

            if self.auxiliary_table:
                tbl = self.__aggregate_auxiliary_table__(table=tbl)

            app.config[self.__table_name_var__] = tbl.fillna('')

            app.config[self.__current_date_var__] = current_date

        return app.config.get(self.__table_name_var__)

    def load(self, page_specs=None, use_pagination=True, filters=None, sort=None):
        conditions = filter_constructor(filters=filters)
        data_frame = self.__load__()
        pagination = None

        if conditions:
            data_frame = data_frame.query(conditions)
        
        if sort:
            by = sort.get('fields')
            order = sort.get('order')
            
            if not by:
                raise Exception('Sort: fields must not be None.')

            if type(by) is not list:
                by = [by]

            if type(order) is not list:
                order = [order]
            
            if order == []:
                order = len(by)*['ASC']

            ascending = list(map(lambda x: True if x.upper()=='ASC' else (False if x.upper()=='DESC' else None), order))
            
            if None in ascending:
                raise Exception('Sort order must be "ASC" or "DESC".')
            if len(by) != len(order):
                raise Exception('Sort: fields and order length must be the same.')
            data_frame = data_frame.sort_values(by=by, ascending=ascending, key=lambda col: col.str.lower() if type(col) is str else col)
        
        if use_pagination and len(data_frame) > 0:
            items_count, page_count, idx_first, idx_last, page_specs = self.__pagination_(
                data_frame, page_specs)
            data_frame = data_frame[idx_first:idx_last]

            pagination = {
                "page": page_specs.get('page'),
                "page_length": page_specs.get('page_length'),
                "page_count": page_count,
                "items_count": items_count
            }

        data_frame_dict = data_frame.to_dict('records')

        return data_frame_dict, pagination


##############################################


def load_convenios(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):

    params = {'AND': []}

    convenios_loader = DataLoader(**convenios_settings)

    if parent:

        if parent.get('NR_EMENDA'):
            params['AND'] += [{'EMENDAS': {'ctx': parent['NR_EMENDA']}}]
        elif parent.get('IDENTIF_PROPONENTE'):
            params['AND'] += [{'IDENTIF_PROPONENTE': {'eq': parent['IDENTIF_PROPONENTE']}}]
        else:
            params['AND'] += [{'NR_CONVENIO': {'eq': parent['NR_CONVENIO']}}]


    if filters:

        if filters.get('MOVIMENTO'):

            movimentos_dict, _ = load_movimento(filters=filters.pop('MOVIMENTO'), use_pagination=False)
            params['AND'] += [{'NR_CONVENIO': {'in': [mov['NR_CONVENIO'] for mov in movimentos_dict]}}]
    
        if filters.get('EMENDAS'):

            emendas_dict, _ = load_emendas(filters=filters.pop('EMENDAS'), use_pagination=False)
            d = []
            for emd in emendas_dict:
                d += emd['CONVENIOS'].split(',')
                
            params['AND'] += [{'NR_CONVENIO': {'in': d}}]

        if filters.get('PROPONENTE'):

            proponentes_dict, _ = load_proponentes(filters=filters.pop('PROPONENTE'), use_pagination=False)
            params['AND'] += [{'IDENTIF_PROPONENTE': {'in': [prop['IDENTIF_PROPONENTE'] for prop in proponentes_dict]}}]



    if params is not None:   
        if filters:
            params['AND'] += [filters]

        if params['AND']:
            filters = params
        
        convenios, pagination = convenios_loader.load(page_specs=page_specs, filters=filters, 
                        sort=sort, use_pagination=use_pagination)
    else:
        emendas = []
        pagination = None

    return convenios, pagination


def load_emendas(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):

    params = {'AND': []}

    emendas_loader = DataLoader(**emendas_settings)

    if parent:
        params['AND'] += [{'CONVENIOS': {'ctx': parent['NR_CONVENIO']}}]

    if filters:
    
        if filters.get('CONVENIOS'):

            convenios_dict, _ = load_convenios(filters=filters.pop('CONVENIOS'), use_pagination=False)
            d = []
            for conv in convenios_dict:
                d += conv['EMENDAS'].split(',')
                
            params['AND'] += [{'NR_EMENDA': {'in': d}}]


    if params is not None:   
        if filters:
            params['AND'] += [filters]

        if params['AND']:
            filters = params

        emendas, pagination = emendas_loader.load(page_specs=page_specs, filters=filters, 
                                                  sort=sort, use_pagination=use_pagination)
    else:
        emendas = []
        pagination = None

    return emendas, pagination


def load_proponentes(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):

    params = {'AND': []}

    proponentes_loader = DataLoader(**proponentes_settings)

    if parent:

        if parent.get('IDENTIF_PROPONENTE'):

            params['AND'] += [{'IDENTIF_PROPONENTE': {'eq': parent['IDENTIF_PROPONENTE']}}]

        else:

            params['AND'] += [{'COD_MUNIC_IBGE': {'eq': parent['codigo_ibge']}}]


    if filters:

        if filters.get('CONVENIOS'):

            convenios_dict, _ = load_convenios(filters=filters.pop('CONVENIOS'), use_pagination=False)
            params['AND'] += [{'IDENTIF_PROPONENTE': {'in': [conv['IDENTIF_PROPONENTE'] for conv in convenios_dict]}}]

        if filters.get('MUNICIPIOS'):

            municipios_dict, _ = load_municipios(filters=filters.pop('MUNICIPIOS'), use_pagination=False)
            params['AND'] += [{'COD_MUNIC_IBGE': {'in': [mun['codigo_ibge'] for mun in municipios_dict]}}]


    if filters:
        params['AND'] += [filters]

    if params['AND']:
        filters = params

    proponentes, pagination = proponentes_loader.load(page_specs=page_specs, filters=filters, sort=sort, use_pagination=use_pagination)

    return proponentes, pagination


def load_movimento(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):

    params = {'AND': []}

    movimento_loader = DataLoader(**movimento_settings)

    if parent:
        params['AND'] += [{'NR_CONVENIO': {'eq': parent['NR_CONVENIO']}}]

    if filters:

        if filters.get('CONVENIOS'):

            convenios_dict, _ = load_convenios(filters=filters.pop('CONVENIOS'), use_pagination=False)
            params['AND'] += [{'NR_CONVENIO': {'in': [conv['NR_CONVENIO'] for conv in convenios_dict]}}]

    if filters:
        params['AND'] += [filters]

    if params['AND']:
        filters = params
  
    movimento, pagination = movimento_loader.load(page_specs=page_specs, filters=filters, sort=sort, 
                                                  use_pagination=use_pagination)

    return movimento, pagination


def load_municipios(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):

    params = {'AND': []}

    municipios_loader = DataLoader(**municipios_settings)

    if parent:
        params['AND'] += [{'codigo_ibge': {'eq': parent['COD_MUNIC_IBGE']}}]

    if filters:

        if filters.get('PROPONENTE'):

            proponentes_dict, _ = load_proponentes(filters=filters.pop('PROPONENTE'), use_pagination=False)
            params['AND'] += [{'codigo_ibge': {'in': [prop['COD_MUNIC_IBGE'] for prop in proponentes_dict]}}]
    

    if filters:
        params['AND'] += [filters]

    if params['AND']:
        filters = params

    municipios, pagination = municipios_loader.load(page_specs=page_specs, filters=filters, sort=sort, use_pagination=use_pagination)

    return municipios, pagination


def load_atributos(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):

    convenios, _ = load_convenios(use_pagination=False)
    
    atributos = {'DATA_ATUAL': get_current_date(), 'SIT_CONVENIO': set(), 'NATUREZA_JURIDICA': set(), 'MODALIDADE': set()}
    
    for conv in convenios:
        atributos['SIT_CONVENIO'].add(conv.get('SIT_CONVENIO') if conv.get('SIT_CONVENIO') else '#indefinido')
        atributos['NATUREZA_JURIDICA'].add(conv.get('NATUREZA_JURIDICA') if conv.get('NATUREZA_JURIDICA') else '#indefinido')
        atributos['MODALIDADE'].add(conv.get('MODALIDADE') if conv.get('MODALIDADE') else '#indefinido')

    atributos = {key: sorted(list(atributos[key])) if key != 'DATA_ATUAL' else atributos[key] for key in atributos}

    return atributos, None

