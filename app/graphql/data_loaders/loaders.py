# coding: utf-8
"""loaders.
    """

from flask import current_app as app
from app.graphql.data_loaders.filtering import filter_constructor
from math import ceil
from app import db
from sqlalchemy import text, desc, func


def sort_constructor(sort):
    if sort is not None:
        return 'order by ' + ','.join([f"{field} {sort.get('order')[p]}" for p, field in enumerate(sort.get('fields'))])

    return ''

def pagination_constructor(conditions=None, page_specs=None, items_count=None):

    page = page_specs.get(
        'page') if page_specs and page_specs.get('page') else 1
    page_length = page_specs.get('page_length') if page_specs and page_specs.get(
        'page_length') else app.config.get('GRAPHQL_DEFAULT_PAGE_LENGTH')
    
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


def load_data(table_expression=None, selected_fields=None, groupby_fields=[], filters=None, sort=None,
               page_specs=None, use_pagination=True, extra_filter=''):
    assert selected_fields is not None
    assert table_expression is not None

    pagination = None
    limit = ''

    where = filter_constructor(filters=filters) if filter else ''
    if extra_filter:
        where = f"({where}) and ({extra_filter})" if where else extra_filter
    
    where = f"where {where}" if where else ''

    order_by = sort_constructor(sort)
    group_by = f"group by {','.join(groupby_fields)}" if groupby_fields else ''

    sql = f"select {', '.join(selected_fields.values())} from {table_expression} {where} {group_by}"
    if use_pagination:
        items_count = db.engine.execute(text(f"select count(*) from ({sql}) a")).scalar()
        pagination, offset, page_specs = pagination_constructor(page_specs=page_specs, items_count=items_count)
        limit = f"limit {offset}, {page_specs.get('page_length')}"
    
    sql = f"{sql} {order_by} {limit}"
    result = db.engine.execute(text(sql))

    data = [{list(selected_fields.keys())[p]: r for p, r in enumerate(row)} for row in result]

    return data, pagination

def load_atributos(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):
    
    atributos = {
                 'DATA_ATUAL': db.engine.execute(text("select DATA_ATUAL from data_atual")).scalar(),
                 'SIT_CONVENIO': [
                                  situacao[0]
                                  for situacao in 
                                  db.engine.execute(text("select SIT_CONVENIO from situacoes order by SIT_CONVENIO"))
                                  ],
                 'NATUREZA_JURIDICA': [
                                       natureza[0]
                                       for natureza in 
                                       db.engine.execute(text("select NATUREZA_JURIDICA from naturezas order by NATUREZA_JURIDICA"))
                                       ],
                 'MODALIDADE': [
                                modalidade[0]
                                for modalidade in
                                db.engine.execute(text("select MODALIDADE from modalidades order by MODALIDADE"))
                                ]
                 }

    return atributos, None


def load_fornecedores(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):
    table_expression = 'movimento'
    selected_fields = {'IDENTIF_FORNECEDOR': 'IDENTIF_FORNECEDOR', 
                       'NOME_FORNECEDOR': 'NOME_FORNECEDOR',
                       'PAGAMENTOS': 'round(sum(VALOR), 2) as PAGAMENTOS'}
    
    groupby_fields = ['IDENTIF_FORNECEDOR', 'NOME_FORNECEDOR']


    extra_filter = ''
    if parent is not None:
        if parent.get('NR_CONVENIO') is not None:
            extra_filter = f"NR_CONVENIO = {parent.get('NR_CONVENIO')}"            

        else:
            raise Exception('load_fornecedores: Unknown parent')

    extra_filter =  f"({extra_filter}) and TIPO = 'P'" if extra_filter else "TIPO = 'P'"

    data, pagination = load_data(table_expression=table_expression, selected_fields=selected_fields,
                                  groupby_fields=groupby_fields, filters=filters, sort=sort,
                                  page_specs=page_specs, use_pagination=use_pagination,
                                  extra_filter=extra_filter)

    return data, pagination


def load_convenios(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):
    table_expression = 'convenios'
    selected_fields = {'NR_CONVENIO': 'NR_CONVENIO',
                       'DIA_ASSIN_CONV': 'DIA_ASSIN_CONV',
                       'SIT_CONVENIO': 'SIT_CONVENIO',
                       'INSTRUMENTO_ATIVO': 'INSTRUMENTO_ATIVO',
                       'DIA_PUBL_CONV': 'DIA_PUBL_CONV',
                       'DIA_INIC_VIGENC_CONV': 'DIA_INIC_VIGENC_CONV',
                       'DIA_FIM_VIGENC_CONV': 'DIA_FIM_VIGENC_CONV',
                       'DIA_LIMITE_PREST_CONTAS': 'DIA_LIMITE_PREST_CONTAS',
                       'VL_GLOBAL_CONV': 'VL_GLOBAL_CONV',
                       'VL_REPASSE_CONV': 'VL_REPASSE_CONV',
                       'VL_CONTRAPARTIDA_CONV': 'VL_CONTRAPARTIDA_CONV',
                       'COD_ORGAO_SUP': 'COD_ORGAO_SUP',
                       'DESC_ORGAO_SUP': 'DESC_ORGAO_SUP',
                       'NATUREZA_JURIDICA': 'NATUREZA_JURIDICA',
                       'COD_ORGAO': 'COD_ORGAO',
                       'DESC_ORGAO':'DESC_ORGAO',
                       'MODALIDADE': 'MODALIDADE',
                       'IDENTIF_PROPONENTE': 'IDENTIF_PROPONENTE',
                       'OBJETO_PROPOSTA': 'OBJETO_PROPOSTA',
                       'VALOR_REPASSE_EMENDA': 'VALOR_REPASSE_EMENDA',
                       'COM_EMENDAS': 'COM_EMENDAS'}

    groupby_fields = []

    extra_filter = ''
    if parent is not None:
        if parent.get('IDENTIF_FORNECEDOR') is not None:
            identif_fornecedor = parent.get('IDENTIF_FORNECEDOR')
            nome_fornecedor = parent.get('NOME_FORNECEDOR')
            id_fornecedor = f"('{identif_fornecedor}', '{nome_fornecedor}')"
            sql_convs = f"select distinct NR_CONVENIO from movimento " \
                        f"where (IDENTIF_FORNECEDOR, NOME_FORNECEDOR)={id_fornecedor}"
            convs = db.engine.execute(text(sql_convs))
            conv_list = [conv[0] for conv in convs]
            extra_filter = f"NR_CONVENIO in ({','.join(conv_list)})"

        elif parent.get('IDENTIF_PROPONENTE') is not None:
            identif_proponente = parent.get('IDENTIF_PROPONENTE')
            sql_convs = f"select distinct NR_CONVENIO from convenios " \
                        f"where IDENTIF_PROPONENTE='{identif_proponente}'"
            convs = db.engine.execute(text(sql_convs))
            conv_list = [f"'{conv[0]}'" for conv in convs]
            extra_filter = f"NR_CONVENIO in ({','.join(conv_list)})"

        elif parent.get('NR_EMENDA') is not None:
            nr_emenda = parent.get('NR_EMENDA')
            sql_convs = f"select distinct NR_CONVENIO from emendas_convenios " \
                        f"where NR_EMENDA='{nr_emenda}'"
            convs = db.engine.execute(text(sql_convs))
            conv_list = [f"'{conv[0]}'" for conv in convs]
            extra_filter = f"NR_CONVENIO in ({','.join(conv_list)})"

        else:
            raise Exception('load_convenios: Unknown parent')

    data, pagination = load_data(table_expression=table_expression, selected_fields=selected_fields,
                                  groupby_fields=groupby_fields, filters=filters, sort=sort,
                                  page_specs=page_specs, use_pagination=use_pagination,
                                  extra_filter=extra_filter)
    
    return data, pagination

def load_municipios(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):

    table_expression = 'municipios'
    selected_fields = {'codigo_ibge': 'codigo_ibge',
                       'nome_municipio': 'nome_municipio',
                       'codigo_uf': 'codigo_uf',
                       'uf': 'uf',
                       'estado': 'estado',
                       'latitude': 'latitude',
                       'longitude': 'longitude'}

    groupby_fields = []

    extra_filter = ''
    if parent is not None:
        if parent.get('COD_MUNIC_IBGE') is not None:
            extra_filter = f"codigo_ibge = {parent.get('COD_MUNIC_IBGE')}"            

        else:
            raise Exception('load_municipios: Unknown parent')


    data, pagination = load_data(table_expression=table_expression, selected_fields=selected_fields,
                                  groupby_fields=groupby_fields, filters=filters, sort=sort,
                                  page_specs=page_specs, use_pagination=use_pagination,
                                  extra_filter=extra_filter)
    
    return data, pagination

def load_proponentes(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):

    table_expression = 'proponentes'
    selected_fields = {'IDENTIF_PROPONENTE': 'IDENTIF_PROPONENTE',
                       'NM_PROPONENTE': 'NM_PROPONENTE',
                       'UF_PROPONENTE': 'UF_PROPONENTE',
                       'MUNIC_PROPONENTE': 'MUNIC_PROPONENTE',
                       'COD_MUNIC_IBGE': 'COD_MUNIC_IBGE'}

    groupby_fields = []

    extra_filter = ''
    if parent is not None:
        if parent.get('codigo_ibge') is not None:
            extra_filter = f"COD_MUNIC_IBGE = {parent.get('codigo_ibge')}"            

        elif parent.get('IDENTIF_PROPONENTE') is not None:
            extra_filter = f"IDENTIF_PROPONENTE = {parent.get('IDENTIF_PROPONENTE')}"            

        else:
            raise Exception('load_proponentes: Unknown parent')

    data, pagination = load_data(table_expression=table_expression, selected_fields=selected_fields,
                                  groupby_fields=groupby_fields, filters=filters, sort=sort,
                                  page_specs=page_specs, use_pagination=use_pagination,
                                  extra_filter=extra_filter)
    
    return data, pagination

def load_emendas(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):

    table_expression = 'emendas'
    selected_fields = {'NR_EMENDA': 'NR_EMENDA',
                       'NOME_PARLAMENTAR': 'NOME_PARLAMENTAR',
                       'TIPO_PARLAMENTAR': 'TIPO_PARLAMENTAR',
                       'VALOR_REPASSE_EMENDA': 'VALOR_REPASSE_EMENDA'}

    groupby_fields = []

    extra_filter = ''
    if parent is not None:
        if parent.get('NR_CONVENIO') is not None:
            nr_convenio = parent.get('NR_CONVENIO')
            sql_emds = f"select distinct NR_EMENDA from emendas_convenios " \
                        f"where NR_CONVENIO='{nr_convenio}'"
            emds = db.engine.execute(text(sql_emds))
            emd_list = [f"'{emd[0]}'" for emd in emds]
            extra_filter = f"NR_EMENDA in ({','.join(emd_list)})"

        else:
            raise Exception('load_emendas: Unknown parent')

    data, pagination = load_data(table_expression=table_expression, selected_fields=selected_fields,
                                  groupby_fields=groupby_fields, filters=filters, sort=sort,
                                  page_specs=page_specs, use_pagination=use_pagination,
                                  extra_filter=extra_filter)
    
    return data, pagination


def load_movimento(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):

    table_expression = 'movimento'
    selected_fields = {'MOV_ID': 'MOV_ID',
                       'NR_CONVENIO': 'NR_CONVENIO',
                       'DATA': 'DATA',
                       'VALOR': 'VALOR',
                       'TIPO': 'TIPO',
                       'IDENTIF_FORNECEDOR': 'IDENTIF_FORNECEDOR',
                       'NOME_FORNECEDOR': 'NOME_FORNECEDOR'}

    groupby_fields = []

    extra_filter = ''
    if parent is not None:
        if parent.get('NR_CONVENIO') is not None:
            extra_filter = f"NR_CONVENIO = {parent.get('NR_CONVENIO')}"            

        elif parent.get('IDENTIF_FORNECEDOR') is not None:
            identif_fornecedor = parent.get('IDENTIF_FORNECEDOR')
            nome_fornecedor = parent.get('NOME_FORNECEDOR')
            id_fornecedor = f"('{identif_fornecedor}', '{nome_fornecedor}')"
            extra_filter = f"(IDENTIF_FORNECEDOR, NOME_FORNECEDOR) = {id_fornecedor}"

        else:
            raise Exception('load_movimento: Unknown parent')

    data, pagination = load_data(table_expression=table_expression, selected_fields=selected_fields,
                                  groupby_fields=groupby_fields, filters=filters, sort=sort,
                                  page_specs=page_specs, use_pagination=use_pagination,
                                  extra_filter=extra_filter)
    
    return data, pagination
