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
        return 'order by ' + ','.join([f"{field} {sort.get('order')[p]}" 
                                       for p, field in enumerate(sort.get('fields'))])

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


def load_data(table_expression=None, selected_fields=None, filters=None, sort=None,
               page_specs=None, use_pagination=True, distinct_clause=False):
    assert selected_fields is not None
    assert table_expression is not None

    pagination = None
    distinct = 'distinct' if distinct_clause else ''
    limit = ''

    where = f"where {filter_constructor(filters=filters)}" if filters else ''
    
    order_by = sort_constructor(sort)

    for k in selected_fields:
        where = where.replace(k, selected_fields[k])
        order_by = order_by.replace(k, selected_fields[k])


    sql = f"select {distinct} {', '.join(selected_fields.values())} from {table_expression} {where}"
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
                                  valor[0]
                                  for valor in 
                                  db.engine.execute(text("select VALOR from atributos " \
                                                         "where atributo='SIT_CONVENIO'"\
                                                         "order by VALOR"))
                                  ],
                 'NATUREZA_JURIDICA': [
                                       valor[0]
                                       for valor in 
                                       db.engine.execute(text("select VALOR from atributos " \
                                                                "where atributo='NATUREZA_JURIDICA'"\
                                                                "order by VALOR"))

                                       ],
                 'MODALIDADE_TRANSFERENCIA': [
                                valor[0]
                                for valor in
                                db.engine.execute(text("select VALOR from atributos " \
                                                        "where atributo='MODALIDADE'"\
                                                        "order by VALOR"))
                                ],
                 'TIPO_PARLAMENTAR': [
                                valor[0]
                                for valor in
                                db.engine.execute(text("select VALOR from atributos " \
                                                        "where atributo='TIPO_PARLAMENTAR'"\
                                                        "order by VALOR"))                              
                                ],
                 'MODALIDADE_COMPRA': [
                                valor[0]
                                for valor in
                                db.engine.execute(text("select VALOR from atributos " \
                                                        "where atributo='MODALIDADE_COMPRA'"\
                                                        "order by VALOR"))                                
                                ],
                 'TIPO_LICITACAO': [
                                valor[0]
                                for valor in
                                db.engine.execute(text("select VALOR from atributos " \
                                                        "where atributo='TIPO_LICITACAO'"\
                                                        "order by VALOR"))  
                                ],
                 'FORMA_LICITACAO': [
                                valor[0]
                                for valor in
                                db.engine.execute(text("select VALOR from atributos " \
                                                        "where atributo='FORMA_LICITACAO'"\
                                                        "order by VALOR"))  
                                ],
                 'STATUS_LICITACAO': [
                                valor[0]
                                for valor in
                                db.engine.execute(text("select VALOR from atributos " \
                                                        "where atributo='STATUS_LICITACAO'"\
                                                        "order by VALOR"))  
                                ]
                 }


    return atributos, None


def load_fornecedores(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):

    table_expression = f"fornecedores"
    
    selected_fields = {'FORNECEDOR_ID': 'FORNECEDOR_ID', 
                       'IDENTIF_FORNECEDOR': 'IDENTIF_FORNECEDOR', 
                       'NOME_FORNECEDOR': 'NOME_FORNECEDOR'}
    if parent is not None:
        if parent.get('query') == 'convenios':
            sql = f"select distinct FORNECEDOR_ID from movimento where \
                    FORNECEDOR_ID<>-1 and NR_CONVENIO = {parent.get('NR_CONVENIO')}"
            table_expression = f"(select b.* from {table_expression} b \
                                inner join ({sql}) c on b.FORNECEDOR_ID = c.FORNECEDOR_ID)"

        elif parent.get('query') == 'movimento':
            sql = f"select FORNECEDOR_ID from movimento \
                    where FORNECEDOR_ID<>-1 and MOV_ID = {parent.get('MOV_ID')}"
            table_expression = f"(select a.* from {table_expression} a \
                                inner join ({sql}) b on a.FORNECEDOR_ID = b.FORNECEDOR_ID)"

        elif parent.get('query') == 'proponentes':
            sql = f"select distinct NR_CONVENIO from convenios \
                    where IDENTIF_PROPONENTE = '{parent.get('IDENTIFICACAO')}'"
            sql = f"select a.FORNECEDOR_ID from movimento \
                    a inner join ({sql}) b on a.NR_CONVENIO = b.NR_CONVENIO \
                    where FORNECEDOR_ID<>-1"
            table_expression = f"(select c.* from {table_expression} c \
                                inner join ({sql}) d on c.FORNECEDOR_ID = d.FORNECEDOR_ID)"

        elif parent.get('query') == 'emendas':
            sql = f"select distinct NR_CONVENIO from emendas_convenios \
                    where NR_EMENDA = {parent.get('NR_EMENDA')}"
            sql = f"select a.FORNECEDOR_ID from movimento a \
                    inner join ({sql}) b on a.NR_CONVENIO = b.NR_CONVENIO \
                    where FORNECEDOR_ID<>-1"
            table_expression = f"(select c.* from {table_expression} c \
                                  inner join ({sql}) d on c.FORNECEDOR_ID = d.FORNECEDOR_ID)"

        elif parent.get('query') == 'municipios':
            sql = f"select distinct IDENTIF_PROPONENTE from proponentes \
                    where CODIGO_IBGE = {parent.get('CODIGO_IBGE')}"
            sql = f"select distinct a.NR_CONVENIO from convenios a inner join ({sql}) b \
                    on a.IDENTIF_PROPONENTE = b.IDENTIF_PROPONENTE"
            sql = f"select c.FORNECEDOR_ID from movimento c \
                    inner join ({sql}) d on c.NR_CONVENIO = d.NR_CONVENIO \
                    where FORNECEDOR_ID<>-1"
            table_expression = f"(select e.* from {table_expression} e \
                                  inner join ({sql}) f on e.FORNECEDOR_ID = f.FORNECEDOR_ID)"

        elif parent.get('query') == 'estados':
            sql = f"select distinct codigo_ibge from municipios \
                    where UF = '{parent.get('SIGLA')}'"
            sql = f"select a.IDENTIF_PROPONENTE from proponentes a \
                    inner join ({sql}) b on a.CODIGO_IBGE = b.CODIGO_IBGE"
            sql = f"select c.NR_CONVENIO from convenios c \
                    inner join ({sql}) d on c.IDENTIF_PROPONENTE = d.IDENTIF_PROPONENTE"
            sql = f"select e.FORNECEDOR_ID from movimento e \
                    inner join ({sql}) f on e.NR_CONVENIO = f.NR_CONVENIO \
                    where FORNECEDOR_ID<>-1"
            table_expression = f"(select g.* from {table_expression} g \
                                  inner join ({sql}) h on g.FORNECEDOR_ID = h.FORNECEDOR_ID)"
        else:
            raise Exception('load_fornecedores: Unknown parent')

    
    table_expression = f"{table_expression} table_expression"
    
    data, pagination = load_data(table_expression=table_expression, selected_fields=selected_fields,
                                  filters=filters, sort=sort, page_specs=page_specs,
                                  use_pagination=use_pagination, distinct_clause=True)

    for d, _ in enumerate(data):
        data[d]['query'] = 'fornecedores'
        data[d]['summary_table'] = table_expression
        data[d]['filters'] = filters

    return data, pagination

def load_fornecedores_summary(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):
    summary_fields = {
                'DATA_PRIMEIRO_PAGAMENTO': 'min(DATA_MOV) as DATA_PRIMEIRO_PAGAMENTO',
                'DATA_ULTIMO_PAGAMENTO': 'max(DATA_MOV) as DATA_ULTIMO_PAGAMENTO',
                'PAGAMENTOS': 'round(sum(VALOR_MOV), 2) as PAGAMENTOS',
                'MENOR_PAGAMENTO': 'min(VALOR_MOV) as MENOR_PAGAMENTO',
                'MAIOR_PAGAMENTO': 'max(VALOR_MOV) as MAIOR_PAGAMENTO',
                'MEDIA_PAGAMENTO': 'round(avg(VALOR_MOV), 2) as MEDIA_PAGAMENTO',
                'DESVPAD_PAGAMENTO': 'round(std(VALOR_MOV), 2) as DESVPAD_PAGAMENTO',
                'QUANTIDADE_PAGAMENTOS': 'count(*) as QUANTIDADE_PAGAMENTOS'
                }
    
    filters = parent.get('filters')
    where = f"where FORNECEDOR_ID = {parent.get('FORNECEDOR_ID')}"
    if filters:
        where = f"{where} and {filter_constructor(filters=filters)}"

    result = db.engine.execute(text(f"select {', '.join(summary_fields.values())} \
                                    from {parent.get('summary_table')} join movimento \
                                    using(FORNECEDOR_ID) {where} group by FORNECEDOR_ID"))

    data = {list(summary_fields.keys())[p]: r for p, r in enumerate(list(result)[0])}

    data['query'] = 'resumo_fornecedor'

    return data, None


def load_estados(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):
    
    table_expression = 'municipios'
    selected_fields = {'SIGLA': 'UF',
                       'NOME': 'NOME_ESTADO'}

    if parent is not None:            
        if parent.get('query') == 'fornecedores':
            id_fornecedor = f"('{parent.get('IDENTIF_FORNECEDOR')}', \
                               '{parent.get('NOME_FORNECEDOR')}')"
            sql = f"select distinct FORNECEDOR_ID from fornecedores  \
                  where (IDENTIF_FORNECEDOR, NOME_FORNECEDOR)={id_fornecedor}"
            sql = f"select distinct a.NR_CONVENIO from movimento a inner join ({sql}) b  \
                  on a.FORNECEDOR_ID = b.FORNECEDOR_ID"
            sql = f"select c.IDENTIF_PROPONENTE from convenios c inner join ({sql}) d  \
                  on c.NR_CONVENIO = d.NR_CONVENIO"
            sql = f"select e.CODIGO_IBGE from proponentes e inner join ({sql}) f \
                  on e.IDENTIF_PROPONENTE = f.IDENTIF_PROPONENTE"
            table_expression = f"(select * from {table_expression} g inner join ({sql}) h \
                                  on g.CODIGO_IBGE = h.CODIGO_IBGE)"

        elif parent.get('query') == 'convenios':
            sql = f"select IDENTIF_PROPONENTE from convenios where \
                    NR_CONVENIO = {parent.get('NR_CONVENIO')}"
            sql = f"select a.CODIGO_IBGE from proponentes a inner join ({sql}) b \
                  on a.IDENTIF_PROPONENTE = b.IDENTIF_PROPONENTE"
            table_expression = f"(select c.UF, c.NOME_ESTADO from {table_expression} c \
                                inner join ({sql}) d on c.CODIGO_IBGE=d.CODIGO_IBGE)"

        elif parent.get('query') == 'proponentes':
            sql = f"select CODIGO_IBGE from proponentes \
                    where IDENTIF_PROPONENTE='{parent.get('IDENTIFICACAO')}'"
            table_expression = f"(select a.UF, a.NOME_ESTADO from {table_expression} a \
                                 inner join ({sql}) b on a.CODIGO_IBGE = b.CODIGO_IBGE)"

        else:
            raise Exception('load_estados: Unknown parent')

    table_expression = f"{table_expression} table_expression"

    data, pagination = load_data(table_expression=table_expression, selected_fields=selected_fields,
                                  filters=filters, sort=sort, page_specs=page_specs,
                                  use_pagination=use_pagination, distinct_clause=True)

    for d, _ in enumerate(data):
        data[d]['query'] = 'estados'

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
                       'VALOR_EMENDA_CONVENIO': 'VALOR_EMENDA_CONVENIO',
                       'COM_EMENDAS': 'COM_EMENDAS',
                       'INSUCESSO': 'INSUCESSO'}

    if parent is not None:
        if parent.get('query') == 'fornecedores':
            id_fornecedor = f"('{parent.get('IDENTIF_FORNECEDOR')}', \
                               '{parent.get('NOME_FORNECEDOR')}')"
            sql = f"select distinct FORNECEDOR_ID from fornecedores  \
                  where (IDENTIF_FORNECEDOR, NOME_FORNECEDOR)={id_fornecedor}"
            sql = f"select distinct a.NR_CONVENIO from movimento a inner join ({sql}) b  \
                  on a.FORNECEDOR_ID = b.FORNECEDOR_ID"
            table_expression = f"(select c.* from {table_expression} c inner join ({sql}) d  \
                  on c.NR_CONVENIO = d.NR_CONVENIO)"

        elif parent.get('query') == 'proponentes':
            table_expression = f"(select * from {table_expression} \
                                  where IDENTIF_PROPONENTE='{parent.get('IDENTIFICACAO')}')"

        elif parent.get('query') == 'emendas':
            sql = f"select distinct NR_CONVENIO from emendas_convenios \
                    where NR_EMENDA={parent.get('NR_EMENDA')}"
            table_expression = f"(select a.* from {table_expression} a inner join ({sql}) b \
                                  on a.NR_CONVENIO = b.NR_CONVENIO)"

        elif parent.get('query') == 'municipios':
            sql = f"select distinct IDENTIF_PROPONENTE from proponentes \
                    where CODIGO_IBGE = {parent.get('CODIGO_IBGE')}"
            table_expression = f"(select a.* from {table_expression} a inner join ({sql}) b \
                                  on a.IDENTIF_PROPONENTE = b.IDENTIF_PROPONENTE)"

        elif parent.get('query') == 'estados':
            sql = f"select distinct CODIGO_IBGE from municipios \
                    where UF = '{parent.get('SIGLA')}'"
            sql = f"select a.IDENTIF_PROPONENTE from proponentes a \
                    inner join ({sql}) b on a.CODIGO_IBGE = b.CODIGO_IBGE"
            table_expression = f"(select c.* from {table_expression} c \
                    inner join ({sql}) d on c.IDENTIF_PROPONENTE = d.IDENTIF_PROPONENTE)"

        elif parent.get('query') == 'movimento':
            table_expression = f"(select * from {table_expression} \
                    where NR_CONVENIO = {parent.get('NR_CONVENIO')})"
        
        elif parent.get('query') == 'licitacoes':
            table_expression = f"(select * from {table_expression} \
                    where NR_CONVENIO = {parent.get('NR_CONVENIO')})"

        else:
            raise Exception('load_convenios: Unknown parent')

    table_expression = f"{table_expression} table_expression"

    data, pagination = load_data(table_expression=table_expression, selected_fields=selected_fields,
                                  filters=filters, sort=sort,
                                  page_specs=page_specs, use_pagination=use_pagination)
    
    for d, _ in enumerate(data):
        data[d]['query'] = 'convenios'

    return data, pagination


def load_municipios(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):

    table_expression = 'municipios'
    selected_fields = {'CODIGO_IBGE': 'CODIGO_IBGE',
                       'NOME_MUNICIPIO': 'NOME_MUNICIPIO',
                       'UF': 'UF',
                       'REGIAO': 'REGIAO',
                       'REGIAO_ABREVIADA': 'REGIAO_ABREVIADA',
                       'LATITUDE': 'LATITUDE',
                       'LONGITUDE': 'LONGITUDE',
                       'CAPITAL': 'CAPITAL'
                       }

    if parent is not None:
        if parent.get('query') == 'proponentes':
            table_expression = f"(select * from {table_expression} " \
                               f"where CODIGO_IBGE = {parent.get('CODIGO_IBGE')})"            

        elif parent.get('query') == 'estados':
            table_expression = f"(select * from {table_expression} where UF = '{parent.get('SIGLA')}')"     

        elif parent.get('query') == 'fornecedores':
            id_fornecedor = f"('{parent.get('IDENTIF_FORNECEDOR')}', \
                               '{parent.get('NOME_FORNECEDOR')}')"
            sql = f"select distinct FORNECEDOR_ID from fornecedores  \
                  where (IDENTIF_FORNECEDOR, NOME_FORNECEDOR)={id_fornecedor}"
            sql = f"select distinct a.NR_CONVENIO from movimento a inner join ({sql}) b  \
                  on a.FORNECEDOR_ID = b.FORNECEDOR_ID"
            sql = f"select c.IDENTIF_PROPONENTE from convenios c inner join ({sql}) d  \
                  on c.NR_CONVENIO = d.NR_CONVENIO"
            sql = f"select e.CODIGO_IBGE from proponentes e inner join ({sql}) f \
                  on e.IDENTIF_PROPONENTE = f.IDENTIF_PROPONENTE"
            table_expression = f"(select g.* from {table_expression} g \
                                inner join ({sql}) h on g.CODIGO_IBGE = h.CODIGO_IBGE)"

        elif parent.get('query') == 'convenios':
            sql = f"select distinct NR_CONVENIO, IDENTIF_PROPONENTE from convenios  \
                  where NR_CONVENIO={parent.get('NR_CONVENIO')}"
            sql = f"select a.CODIGO_IBGE from proponentes a inner join ({sql}) b \
                  on a.IDENTIF_PROPONENTE = b.IDENTIF_PROPONENTE"
            table_expression = f"(select c.* from {table_expression} c \
                                inner join ({sql}) d on c.CODIGO_IBGE = d.CODIGO_IBGE)"

        else:
            raise Exception('load_municipios: Unknown parent')

    table_expression = f"{table_expression} table_expression"

    data, pagination = load_data(table_expression=table_expression, selected_fields=selected_fields,
                                  filters=filters, sort=sort,
                                  page_specs=page_specs, use_pagination=use_pagination)

    for d, _ in enumerate(data):
        data[d]['query'] = 'municipios'

    return data, pagination


def load_proponentes(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):

    table_expression = 'proponentes'
    selected_fields = {'IDENTIFICACAO': 'IDENTIF_PROPONENTE',
                       'NOME_PROPONENTE': 'NM_PROPONENTE',
                       'CODIGO_IBGE': 'CODIGO_IBGE'}

    if parent is not None:
        if parent.get('query') == 'municipios':
            table_expression = f"(select * from {table_expression} " \
                               f"where CODIGO_IBGE = '{parent.get('CODIGO_IBGE')}')"
        
        elif parent.get('query') == 'estados':
            sql = f"select distinct CODIGO_IBGE from municipios \
                    where UF = '{parent.get('SIGLA')}'"
            table_expression = f"(select a.* from {table_expression} a inner join ({sql}) b \
                                  on a.CODIGO_IBGE = b.CODIGO_IBGE)"
            
        elif parent.get('query') == 'convenios':
            table_expression = f"(select * from {table_expression} \
                                  where IDENTIF_PROPONENTE = '{parent.get('IDENTIF_PROPONENTE')}')"
            
        elif parent.get('query') == 'licitacoes':
            sql = f"select distinct IDENTIF_PROPONENTE from convenios \
                    where NR_CONVENIO = {parent.get('NR_CONVENIO')}"
            table_expression = f"(select a.* from {table_expression} a inner join ({sql}) b \
                                  on a.IDENTIF_PROPONENTE = b.IDENTIF_PROPONENTE)"

        else:
            raise Exception('load_proponentes: Unknown parent')

    table_expression = f"{table_expression} table_expression"

    data, pagination = load_data(table_expression=table_expression, selected_fields=selected_fields,
                                  filters=filters, sort=sort,
                                  page_specs=page_specs, use_pagination=use_pagination)

    for d, _ in enumerate(data):
        data[d]['query'] = 'proponentes'

    return data, pagination


def load_emendas(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):

    table_expression = 'emendas'
    selected_fields = {'NR_EMENDA': 'NR_EMENDA',
                       'NOME_PARLAMENTAR': 'NOME_PARLAMENTAR',
                       'TIPO_PARLAMENTAR': 'TIPO_PARLAMENTAR',
                       'VALOR_EMENDA': 'VALOR_EMENDA'}

    if parent is not None:
        if parent.get('query') == 'convenios':
            sql = f"select distinct NR_EMENDA from emendas_convenios \
                    where NR_CONVENIO={parent.get('NR_CONVENIO')}"
            table_expression = f"(select a.* from {table_expression} a inner join ({sql}) b \
                                  on a.NR_EMENDA = b.NR_EMENDA)"

        elif parent.get('query') == 'fornecedores':
            id_fornecedor = f"('{parent.get('IDENTIF_FORNECEDOR')}', \
                               '{parent.get('NOME_FORNECEDOR')}')"
            sql = f"select distinct FORNECEDOR_ID from fornecedores  \
                  where (IDENTIF_FORNECEDOR, NOME_FORNECEDOR)={id_fornecedor}"
            sql = f"select distinct a.NR_CONVENIO from movimento a inner join ({sql}) b  \
                  on a.FORNECEDOR_ID = b.FORNECEDOR_ID"
            sql = f"select a.NR_EMENDA from emendas_convenios a inner join ({sql}) b \
                    on a.NR_CONVENIO = b.NR_CONVENIO"
            table_expression = f"(select c.* from {table_expression} c inner join ({sql}) d \
                                  on c.NR_EMENDA = d.NR_EMENDA)"

        else:
            raise Exception('load_emendas: Unknown parent')

    table_expression = f"{table_expression} table_expression"

    data, pagination = load_data(table_expression=table_expression, selected_fields=selected_fields,
                                  filters=filters, sort=sort,
                                  page_specs=page_specs, use_pagination=use_pagination)

    for d, _ in enumerate(data):
        data[d]['query'] = 'emendas'

    return data, pagination


def load_movimento(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):

    table_expression = 'movimento'
    selected_fields = {'MOV_ID': 'MOV_ID',
                       'NR_CONVENIO': 'NR_CONVENIO',
                       'DATA': 'DATA_MOV',
                       'VALOR': 'VALOR_MOV',
                       'TIPO': 'TIPO_MOV'}

    if parent is not None:
        if parent.get('query') == 'convenios':
            table_expression = f"(select * from {table_expression} \
                                  where NR_CONVENIO = {parent.get('NR_CONVENIO')})"            

        elif parent.get('query') == 'fornecedores':
            id_fornecedor = f"('{parent.get('IDENTIF_FORNECEDOR')}', \
                               '{parent.get('NOME_FORNECEDOR')}')"
            sql = f"select distinct FORNECEDOR_ID from fornecedores  \
                  where (IDENTIF_FORNECEDOR, NOME_FORNECEDOR)={id_fornecedor}"
            table_expression = f"(select distinct a.NR_CONVENIO from {table_expression} \
                                a inner join ({sql}) b on a.FORNECEDOR_ID = b.FORNECEDOR_ID)"

        else:
            raise Exception('load_movimento: Unknown parent')

    table_expression = f"{table_expression} table_expression"

    data, pagination = load_data(table_expression=table_expression, selected_fields=selected_fields,
                                  filters=filters, sort=sort,
                                  page_specs=page_specs, use_pagination=use_pagination)


    for d, _ in enumerate(data):
        data[d]['query'] = 'movimento'

    return data, pagination

def load_licitacoes(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):

    table_expression = 'licitacoes'
    selected_fields = {'NR_CONVENIO': 'NR_CONVENIO',
                       'MODALIDADE_COMPRA': 'MODALIDADE_COMPRA',
                       'TIPO_LICITACAO': 'TIPO_LICITACAO',
                       'FORMA_LICITACAO': 'FORMA_LICITACAO',
                       'REGISTRO_PRECOS': 'REGISTRO_PRECOS',
                       'LICITACAO_INTERNACIONAL': 'LICITACAO_INTERNACIONAL',
                       'STATUS_LICITACAO': 'STATUS_LICITACAO',
                       'VALOR_LICITACAO': 'VALOR_LICITACAO'}


    if parent is not None:
        if parent.get('query') == 'convenios':
            table_expression = f"(select * from {table_expression} \
                                  where NR_CONVENIO = {parent.get('NR_CONVENIO')})"            

        elif parent.get('query') == 'proponentes':
            sql = f"select distinct NR_CONVENIO from convenios \
                    where IDENTIF_PROPONENTE = '{parent.get('IDENTIFICACAO')}'"
            table_expression = f"(select a.* from {table_expression} a inner join ({sql}) b \
                                  on a.NR_CONVENIO = b.NR_CONVENIO)"

        elif parent.get('query') == 'fornecedores':
            sql = f"select distinct NR_CONVENIO from movimento \
                    where FORNECEDOR_ID = {parent.get('FORNECEDOR_ID')}"
            table_expression = f"(select a.* from {table_expression} a inner join ({sql}) b \
                                  on a.NR_CONVENIO = b.NR_CONVENIO)"

        else:
            raise Exception('load_movimento: Unknown parent')

    table_expression = f"{table_expression} table_expression"

    data, pagination = load_data(table_expression=table_expression, selected_fields=selected_fields,
                                  filters=filters, sort=sort,
                                  page_specs=page_specs, use_pagination=use_pagination)


    for d, _ in enumerate(data):
        data[d]['query'] = 'licitacoes'

    return data, pagination
