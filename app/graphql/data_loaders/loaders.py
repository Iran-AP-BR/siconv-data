# coding: utf-8
"""loaders.
    """

from flask import current_app as app
from app.graphql.data_loaders.filtering import filter_constructor
import os
from math import ceil

from app.database import db, Convenio, Emenda, Movimento, Proponente, Municipio, DataAtual

from sqlalchemy import text, desc, func
from sqlalchemy.orm import joinedload

tables = {
          'convenios': Convenio,
          'emendas': Emenda,
          'proponentes': Proponente,
          'movimento': Movimento,
          'municipios': Municipio
          }

def sort_constructor(sort):
    if sort:
        sort_fields = [text(field) for field in sort.get('fields')]
        sort_fields = [(desc(field) if sort.get('order')[p].upper()=='DESC' else field) for p, field in enumerate(sort_fields)]
    else:
        sort_fields = [text('')]

    return sort_fields

def pagination_constructor(table_name=None, conditions=None, page_specs=None, items_count=None):
    assert table_name is not None and conditions is not None and items_count is None or \
           table_name is None and conditions is None and items_count is not None, \
           'Assertion error in pagination_constructor.'

    page = page_specs.get(
        'page') if page_specs and page_specs.get('page') else 1
    page_length = page_specs.get('page_length') if page_specs and page_specs.get(
        'page_length') else app.config.get('GRAPHQL_DEFAULT_PAGE_LENGTH')
    
    if not items_count:
        sql = f'select count(*) from {table_name}'
        if conditions:
            sql = f'{sql} where {conditions}'

        items_count = db.engine.execute(text(sql)).scalar()

    page_count = ceil(items_count / page_length)

    if page > page_count:
        page = page_count

    if page == 0:
        page_length = 0

    offset = (page - 1) * page_length if page > 0 else 0

    page_specs = {'page': page, 'page_length': page_length}

    pagination = {
        "page": page,
        "page_length": page_length,
        "page_count": page_count,
        "items_count": items_count
    }

    return pagination, offset, page_specs

class DataLoader(object):
    
    def __init__(self, table_name, db):
        self.table_name = table_name
        self.table = tables[table_name]
        self.db = db

    def load(self, page_specs=None, filters=None, sort=None, use_pagination=True):
        
        conditions = filter_constructor(filters=filters)
        pagination = None

        sort_fields = sort_constructor(sort)

        query = self.db.session.query(self.table).filter(text(conditions)).order_by(*sort_fields)

        if use_pagination:
            pagination, offset, page_specs = pagination_constructor(table_name=self.table_name, 
                                                        conditions=conditions, page_specs=page_specs)

            query = query.offset(offset).limit(page_specs.get('page_length'))
        

        data = query.all()
        
        return data, pagination


##############################################

def rows2dict(rows, attr=None):
    data_dict = []
    
    for row in rows:
        d = {}
        if attr:
            d = { mtm: getattr(row, mtm) for mtm in attr }

        for column in row.__table__.columns:
            d[column.name] = str(getattr(row, column.name))

        data_dict += [d]

    return data_dict

def load_data(table_name=None, query=None, filters=None, sort=None, page_specs=None, use_pagination=True):
    assert table_name is not None and query is None or \
           table_name is None and query is not None, 'Assertion error in load_data.'

    if table_name:
        query = db.session.query(tables[table_name])

    fltr = filter_constructor(filters=filters)
    srt = sort_constructor(sort=sort)
    query = query.filter(text(fltr)).order_by(*srt)
    items_count = query.count()

    offset = 0
    if use_pagination:
        pagination, offset, page_specs = pagination_constructor(page_specs=page_specs, items_count=items_count)
        query = query.offset(offset).limit(page_specs.get('page_length'))

    return query.all(), pagination


def load_convenios(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):

    params = {'AND': []}

    convenios_loader = DataLoader(table_name='convenios', db=db)

    if parent:
        if parent.get('CONVENIOS'): #CONVENIOS
            dta, pagination = load_data(query=parent.get('CONVENIOS'), filters=filters, sort=sort, page_specs=page_specs)
            data_dict = rows2dict(dta, attr=['EMENDAS', 'PROPONENTE'])

            return data_dict, pagination

        elif parent.get('IDENTIF_PROPONENTE'): #CONVENIO
            params['AND'] += [{'IDENTIF_PROPONENTE': {'eq': parent['IDENTIF_PROPONENTE']}}]

        elif parent.get('NR_CONVENIO'): #CONVENIO
            params['AND'] += [{'NR_CONVENIO': {'eq': parent['NR_CONVENIO']}}]
    
        else:
            raise Exception('load_convenios: Unknown parent')

    if filters:
        params['AND'] += [filters]

    if params['AND']:
        filters = params

    data, pagination = convenios_loader.load(page_specs=page_specs, filters=filters, 
                        sort=sort, use_pagination=use_pagination)
    
    data_dict = rows2dict(data, attr=['EMENDAS', 'PROPONENTE'])
    return data_dict, pagination


def load_emendas(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):

    params = {'AND': []}

    emendas_loader = DataLoader(table_name='emendas', db=db)
    if parent:
        if parent.get('NR_CONVENIO'): #EMENDAS
            emds = [emd.NR_EMENDA for emd in parent.get('EMENDAS')]

            params['AND'] += [{'NR_EMENDA': {'in': emds}}]
        else:
            raise Exception('load_emendas: Unknown parent')

    if filters:
        params['AND'] += [filters]

    if params['AND']:
        filters = params


    data, pagination = emendas_loader.load(page_specs=page_specs, filters=filters, 
                                                  sort=sort, use_pagination=use_pagination)

    data_dict = rows2dict(data, attr=['CONVENIOS'])
    
    return data_dict, pagination


def load_proponentes(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):

    params = {'AND': []}

    proponentes_loader = DataLoader(table_name='proponentes', db=db)

    if parent:
        if parent.get('IDENTIF_PROPONENTE'): #PROPONENTE
            params['AND'] += [{'IDENTIF_PROPONENTE': {'eq': parent['IDENTIF_PROPONENTE']}}]

        elif parent.get('codigo_ibge'): #PROPONENTES
            params['AND'] += [{'COD_MUNIC_IBGE': {'eq': parent['codigo_ibge']}}]
        
        else:
            raise Exception('load_proponentes: Unknown parent')

    if filters:
        params['AND'] += [filters]

    if params['AND']:
        filters = params

    data, pagination = proponentes_loader.load(page_specs=page_specs, filters=filters,
                                                  sort=sort, use_pagination=use_pagination)
    data_dict = rows2dict(data, attr=['CONVENIOS', 'MUNICIPIO'])
    return data_dict, pagination


def load_movimento(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):

    params = {'AND': []}

    movimento_loader = DataLoader(table_name='movimento', db=db)

    if parent:
        if parent.get('NR_CONVENIO'): #CONVENIO
            params['AND'] += [{'NR_CONVENIO': {'eq': parent['NR_CONVENIO']}}]

        else:
            raise Exception('load_movimento: Unknown parent')

    if filters:
        params['AND'] += [filters]

    if params['AND']:
        filters = params

    data, pagination = movimento_loader.load(page_specs=page_specs, filters=filters, sort=sort, 
                                                  use_pagination=use_pagination)

    data_dict = rows2dict(data)
    return data_dict, pagination


def load_municipios(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):

    params = {'AND': []}

    municipios_loader = DataLoader(table_name='municipios', db=db)

    if parent:
        if parent.get('COD_MUNIC_IBGE'):
            params['AND'] += [{'codigo_ibge': {'eq': parent['COD_MUNIC_IBGE']}}]

        else:
            raise Exception('load_municipio: Unknown parent')

    if filters:
        params['AND'] += [filters]

    if params['AND']:
        filters = params

    data, pagination = municipios_loader.load(page_specs=page_specs, filters=filters,
                                                sort=sort, use_pagination=use_pagination)
    data_dict = rows2dict(data, attr=['PROPONENTES'])
    return data_dict, pagination


def load_atributos(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):


    return {}, None

