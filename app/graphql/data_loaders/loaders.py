# coding: utf-8
"""loaders.
    """

from flask import current_app as app
from app.graphql.data_loaders.filtering import filter_constructor
from math import ceil
from app.database import db, Convenio, Emenda, Movimento, Proponente, Municipio, DataAtual, Situacao, Natureza, Modalidade
from sqlalchemy import text, desc, func

tables = {
          'convenios': Convenio,
          'emendas': Emenda,
          'proponentes': Proponente,
          'movimento': Movimento,
          'municipios': Municipio
          }

def sort_constructor(sort):
    if sort is not None:
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
    
    if items_count is None:
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

def rows2dict(rows, attr=None):
    data_dict = []
    
    for row in rows:
        d = {}
        if attr is not None:
            d = { mtm: getattr(row, mtm) for mtm in attr }

        for column in row.__table__.columns:
            d[column.name] = str(getattr(row, column.name))

        data_dict += [d]

    return data_dict

def load_data(table_name=None, query=None, filters=None, sort=None, page_specs=None, use_pagination=True):
    assert table_name is not None and query is None or \
           table_name is None and query is not None, 'Assertion error in load_data.'


    if table_name is not None:
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

    pagination = None
    
    if parent is not None:
        if parent.get('CONVENIOS') is not None:
            data, pagination = load_data(query=parent.get('CONVENIOS'), filters=filters, sort=sort,
                                        page_specs=page_specs, use_pagination=use_pagination)

        elif parent.get('CONVENIO') is not None:
            data = [parent.get('CONVENIO')]
    
        else:
            raise Exception('load_convenios: Unknown parent')
    
    else:
        data, pagination = load_data(table_name='convenios', filters=filters, sort=sort,
                                    page_specs=page_specs, use_pagination=use_pagination)


    data_dict = rows2dict(data, attr=['EMENDAS', 'PROPONENTE', 'MOVIMENTOS'])

    return data_dict, pagination


def load_emendas(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):

    pagination = None
    
    if parent is not None:
        if parent.get('EMENDAS') is not None:
            data, pagination = load_data(query=parent.get('EMENDAS'), filters=filters, sort=sort,
                                        page_specs=page_specs, use_pagination=use_pagination)

        elif parent.get('EMENDA') is not None:
            data = [parent.get('EMENDA')]
    
        else:
            raise Exception('load_emendas: Unknown parent')
    
    else:
        data, pagination = load_data(table_name='emendas', filters=filters, sort=sort,
                                    page_specs=page_specs, use_pagination=use_pagination)


    data_dict = rows2dict(data, attr=['CONVENIOS'])

    return data_dict, pagination

def load_proponentes(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):

    pagination = None
    
    if parent is not None:
        if parent.get('PROPONENTES') is not None:
            data, pagination = load_data(query=parent.get('PROPONENTES'), filters=filters, sort=sort,
                                        page_specs=page_specs, use_pagination=use_pagination)

        elif parent.get('PROPONENTE') is not None:
            data = [parent.get('PROPONENTE')]
    
        else:
            raise Exception('load_proponentes: Unknown parent')
    
    else:
        data, pagination = load_data(table_name='proponentes', filters=filters, sort=sort,
                                    page_specs=page_specs, use_pagination=use_pagination)


    data_dict = rows2dict(data, attr=['CONVENIOS', 'MUNICIPIO'])

    return data_dict, pagination


def load_movimento(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):

    pagination = None
    
    if parent is not None:
        if parent.get('MOVIMENTOS') is not None:
            data, pagination = load_data(query=parent.get('MOVIMENTOS'), filters=filters, sort=sort,
                                        page_specs=page_specs, use_pagination=use_pagination)

        else:
            raise Exception('load_movimento: Unknown parent')
    
    else:
        data, pagination = load_data(table_name='movimento', filters=filters, sort=sort,
                                    page_specs=page_specs, use_pagination=use_pagination)

    data_dict = rows2dict(data, attr=['CONVENIO'])
    
    return data_dict, pagination

def load_municipios(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):

    pagination = None
    
    if parent is not None:
        if parent.get('MUNICIPIO') is not None:
            data = [parent.get('MUNICIPIO')]

        else:
            raise Exception('load_municipios: Unknown parent')
    
    else:
        data, pagination = load_data(table_name='municipios', filters=filters, sort=sort,
                                    page_specs=page_specs, use_pagination=use_pagination)

    data_dict = rows2dict(data, attr=['PROPONENTES'])
    
    return data_dict, pagination


def load_atributos(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):
    
    atributos = {
                 'DATA_ATUAL': db.session.query(DataAtual).first().DATA_ATUAL,
                 'SIT_CONVENIO': [
                                  situacao.SIT_CONVENIO 
                                  for situacao in 
                                  db.session.query(Situacao).order_by(text('SIT_CONVENIO')).all()
                                  ],
                 'NATUREZA_JURIDICA': [
                                       natureza.NATUREZA_JURIDICA
                                       for natureza in 
                                       db.session.query(Natureza).order_by(text('NATUREZA_JURIDICA')).all()
                                       ],
                 'MODALIDADE': [
                                modalidade.MODALIDADE
                                for modalidade in
                                db.session.query(Modalidade).order_by(text('MODALIDADE')).all()
                                ]
                 }

    return atributos, None

