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
                                  situacao[0]
                                  for situacao in 
                                  db.engine.execute(text("select SIT_CONVENIO from situacoes "\
                                                         "order by SIT_CONVENIO"))
                                  ],
                 'NATUREZA_JURIDICA': [
                                       natureza[0]
                                       for natureza in 
                                       db.engine.execute(text("select NATUREZA_JURIDICA from naturezas " \
                                                              "order by NATUREZA_JURIDICA"))
                                       ],
                 'MODALIDADE': [
                                modalidade[0]
                                for modalidade in
                                db.engine.execute(text("select MODALIDADE from modalidades " \
                                                       "order by MODALIDADE"))
                                ],
                 'TIPO_PARLAMENTAR': [
                                tipo[0]
                                for tipo in
                                db.engine.execute(text("select distinct TIPO_PARLAMENTAR " \
                                                       "from emendas where not TIPO_PARLAMENTAR is null " \
                                                       "order by TIPO_PARLAMENTAR"))
                                ]
                 }

    return atributos, None


def load_fornecedores(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):

    table_expression = f"(select * from movimento where TIPO='P') f"
    
    selected_fields = {'IDENTIF_FORNECEDOR': 'IDENTIF_FORNECEDOR', 
                       'NOME_FORNECEDOR': 'NOME_FORNECEDOR'}
    if parent is not None:
        if parent.get('query') == 'convenios':
            table_expression = f"(select * from movimento \
                                  where TIPO='P' and NR_CONVENIO = '{parent.get('NR_CONVENIO')}') f"

        elif parent.get('query') == 'proponentes':
            sql = f"select distinct NR_CONVENIO from convenios \
                    where IDENTIF_PROPONENTE = '{parent.get('IDENTIF_PROPONENTE')}'"
            table_expression = f"(select a.* from (select * from movimento where TIPO='P') a \
                                  inner join ({sql}) b on a.NR_CONVENIO = b.NR_CONVENIO) f"

        elif parent.get('query') == 'emendas':
            sql = f"select distinct NR_CONVENIO from emendas_convenios \
                    where NR_EMENDA = '{parent.get('NR_EMENDA')}'"
            table_expression = f"(select a.* from (select * from movimento where TIPO='P') a \
                                  inner join ({sql}) b on a.NR_CONVENIO = b.NR_CONVENIO) f"

        elif parent.get('query') == 'municipios':
            sql = f"select distinct IDENTIF_PROPONENTE from proponentes \
                    where COD_MUNIC_IBGE = '{parent.get('codigo_ibge')}'"
            sql = f"select distinct a.NR_CONVENIO from convenios a inner join ({sql}) b \
                    on a.IDENTIF_PROPONENTE = b.IDENTIF_PROPONENTE"
            table_expression = f"(select c.* from (select * from movimento where TIPO='P') c \
                                  inner join ({sql}) d on c.NR_CONVENIO = d.NR_CONVENIO) f"

        elif parent.get('query') == 'estados':
            sql = f"select distinct codigo_ibge from municipios \
                    where codigo_uf = '{parent.get('codigo_uf')}'"
            sql = f"select a.IDENTIF_PROPONENTE from proponentes a \
                    inner join ({sql}) b on a.COD_MUNIC_IBGE = b.codigo_ibge"
            sql = f"select c.NR_CONVENIO from convenios c \
                    inner join ({sql}) d on c.IDENTIF_PROPONENTE = d.IDENTIF_PROPONENTE"
            table_expression = f"(select e.* from (select * from movimento where TIPO='P') e \
                                  inner join ({sql}) f on e.NR_CONVENIO = f.NR_CONVENIO) g"
            
        else:
            raise Exception('load_fornecedores: Unknown parent')

    data, pagination = load_data(table_expression=table_expression, selected_fields=selected_fields,
                                  filters=filters, sort=sort, page_specs=page_specs,
                                  use_pagination=use_pagination, distinct_clause=True)

    for d, _ in enumerate(data):
        data[d]['query'] = 'fornecedores'
        data[d]['table_summary'] = table_expression
        data[d]['filters'] = filters

    return data, pagination

def load_fornecedores_summary(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):
    id_fornecedor = f"('{parent.get('IDENTIF_FORNECEDOR')}', '{parent.get('NOME_FORNECEDOR')}')"
    summary_fields = {
                'DATA_PRIMEIRO_PAGAMENTO': 'min(DATA) as DATA_PRIMEIRO_PAGAMENTO',
                'DATA_ULTIMO_PAGAMENTO': 'max(DATA) as DATA_ULTIMO_PAGAMENTO',
                'PAGAMENTOS': 'round(sum(VALOR), 2) as PAGAMENTOS',
                'MENOR_PAGAMENTO': 'min(VALOR) as MENOR_PAGAMENTO',
                'MAIOR_PAGAMENTO': 'max(VALOR) as MAIOR_PAGAMENTO',
                'MEDIA_PAGAMENTO': 'round(avg(VALOR), 2) as MEDIA_PAGAMENTO',
                'DESVPAD_PAGAMENTO': 'round(std(VALOR), 2) as DESVPAD_PAGAMENTO',
                'QUANTIDADE_PAGAMENTOS': 'count(*) as QUANTIDADE_PAGAMENTOS'
                }
    
    filters = parent.get('filters')
    where = f"where (IDENTIF_FORNECEDOR, NOME_FORNECEDOR)={id_fornecedor}"
    if filters:
        where = f"{where} and {filter_constructor(filters=filters)}"

    result = db.engine.execute(text(f"select {', '.join(summary_fields.values())} \
                                      from {parent.get('table_summary')} \
                                      {where} group by IDENTIF_FORNECEDOR, NOME_FORNECEDOR"))

    data = {list(summary_fields.keys())[p]: r for p, r in enumerate(list(result)[0])}

    data['query'] = 'resumo_fornecedor'

    return data, None


def load_estados(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):
    
    table_expression = 'municipios'
    selected_fields = {'codigo_uf': 'codigo_uf',
                       'uf': 'uf',
                       'estado': 'estado'}

    if parent is not None:            
        if parent.get('query') == 'fornecedores':
            id_fornecedor = f"('{parent.get('IDENTIF_FORNECEDOR')}', \
                               '{parent.get('NOME_FORNECEDOR')}')"
            sql = f"select distinct NR_CONVENIO from movimento  \
                  where (IDENTIF_FORNECEDOR, NOME_FORNECEDOR)={id_fornecedor}"
            sql = f"select a.IDENTIF_PROPONENTE from convenios a inner join ({sql}) b  \
                  on a.NR_CONVENIO = b.NR_CONVENIO"
            sql = f"select c.COD_MUNIC_IBGE from proponentes c inner join ({sql}) d \
                  on c.IDENTIF_PROPONENTE = d.IDENTIF_PROPONENTE"
            table_expression = f"(select * \
                                  from municipios e inner join ({sql}) f \
                                  on e.codigo_ibge = f.COD_MUNIC_IBGE) g"

        else:
            raise Exception('load_estados: Unknown parent')

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
                       'VALOR_REPASSE_EMENDA': 'VALOR_REPASSE_EMENDA',
                       'COM_EMENDAS': 'COM_EMENDAS'}

    if parent is not None:
        if parent.get('query') == 'fornecedores':
            id_fornecedor = f"('{parent.get('IDENTIF_FORNECEDOR')}', \
                               '{parent.get('NOME_FORNECEDOR')}')"
            sql = f"select distinct NR_CONVENIO from movimento " \
                  f"where (IDENTIF_FORNECEDOR, NOME_FORNECEDOR)={id_fornecedor}"
            table_expression = f"(select a.* from convenios a inner join ({sql}) b \
                               on a.NR_CONVENIO = b.NR_CONVENIO) c"

        elif parent.get('query') == 'proponentes':
            table_expression = f"(select * from convenios \
                                  where IDENTIF_PROPONENTE='{parent.get('IDENTIF_PROPONENTE')}') c"

        elif parent.get('query') == 'emendas':
            sql = f"select distinct NR_CONVENIO from emendas_convenios \
                    where NR_EMENDA='{parent.get('NR_EMENDA')}'"
            table_expression = f"(select a.* from convenios a inner join ({sql}) b \
                                  on a.NR_CONVENIO = b.NR_CONVENIO) c"

        elif parent.get('query') == 'municipios':
            sql = f"select distinct IDENTIF_PROPONENTE from proponentes \
                    where COD_MUNIC_IBGE = '{parent.get('codigo_ibge')}'"
            table_expression = f"(select a.* from convenios a inner join ({sql}) b \
                                  on a.IDENTIF_PROPONENTE = b.IDENTIF_PROPONENTE) c"

        elif parent.get('query') == 'estados':
            sql = f"select distinct codigo_ibge from municipios \
                    where codigo_uf = '{parent.get('codigo_uf')}'"
            sql = f"select a.IDENTIF_PROPONENTE from proponentes a inner join ({sql}) b \
                    on a.COD_MUNIC_IBGE = b.codigo_ibge"
            table_expression = f"(select c.* from convenios c inner join ({sql}) d \
                                  on c.IDENTIF_PROPONENTE = d.IDENTIF_PROPONENTE) e"

        else:
            raise Exception('load_convenios: Unknown parent')

    data, pagination = load_data(table_expression=table_expression, selected_fields=selected_fields,
                                  filters=filters, sort=sort,
                                  page_specs=page_specs, use_pagination=use_pagination)
    
    for d, _ in enumerate(data):
        data[d]['query'] = 'convenios'

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

    if parent is not None:
        if parent.get('COD_MUNIC_IBGE') is not None:
            table_expression = f"(select * from municipios " \
                               f"where codigo_ibge = '{parent.get('COD_MUNIC_IBGE')}') m"            

        elif parent.get('query') == 'fornecedores':
            id_fornecedor = f"('{parent.get('IDENTIF_FORNECEDOR')}', \
                               '{parent.get('NOME_FORNECEDOR')}')"
            sql = f"select distinct NR_CONVENIO from movimento \
                    where (IDENTIF_FORNECEDOR, NOME_FORNECEDOR)={id_fornecedor}"
            sql = f"select a.IDENTIF_PROPONENTE from convenios a inner join ({sql}) b \
                    on a.NR_CONVENIO = b.NR_CONVENIO"
            sql = f"select c.COD_MUNIC_IBGE from proponentes c inner join ({sql}) d \
                    on c.IDENTIF_PROPONENTE = d.IDENTIF_PROPONENTE"
            table_expression = f"(select e.* from municipios e inner join ({sql}) f \
                                  on e.codigo_ibge = f.COD_MUNIC_IBGE) g"

        else:
            raise Exception('load_municipios: Unknown parent')


    data, pagination = load_data(table_expression=table_expression, selected_fields=selected_fields,
                                  filters=filters, sort=sort,
                                  page_specs=page_specs, use_pagination=use_pagination)

    for d, _ in enumerate(data):
        data[d]['query'] = 'municipios'

    return data, pagination


def load_proponentes(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):

    table_expression = 'proponentes'
    selected_fields = {'IDENTIF_PROPONENTE': 'IDENTIF_PROPONENTE',
                       'NM_PROPONENTE': 'NM_PROPONENTE',
                       'UF_PROPONENTE': 'UF_PROPONENTE',
                       'MUNIC_PROPONENTE': 'MUNIC_PROPONENTE',
                       'COD_MUNIC_IBGE': 'COD_MUNIC_IBGE'}

    if parent is not None:
        if parent.get('query') == 'municipios':
            table_expression = f"(select * from proponentes " \
                               f"where COD_MUNIC_IBGE = '{parent.get('codigo_ibge')}') p"
        
        elif parent.get('query') == 'estados':
            sql = f"select distinct codigo_ibge from municipios \
                    where codigo_uf = '{parent.get('codigo_uf')}'"
            table_expression = f"(select a.* from proponentes a inner join ({sql}) b \
                                  on a.COD_MUNIC_IBGE = b.codigo_ibge) c"
            
        elif parent.get('query') == 'convenios':
            table_expression = f"(select * from proponentes \
                                  where IDENTIF_PROPONENTE = '{parent.get('IDENTIF_PROPONENTE')}') p"

        else:
            raise Exception('load_proponentes: Unknown parent')

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
                       'VALOR_REPASSE_EMENDA': 'VALOR_REPASSE_EMENDA'}

    if parent is not None:
        if parent.get('query') == 'convenios':
            sql = f"select distinct NR_EMENDA from emendas_convenios \
                    where NR_CONVENIO='{parent.get('NR_CONVENIO')}'"
            table_expression = f"(select a.* from emendas a inner join ({sql}) b \
                                  on a.NR_EMENDA = b.NR_EMENDA) c"

        elif parent.get('query') == 'fornecedores':
            id_fornecedor = f"('{parent.get('IDENTIF_FORNECEDOR')}', \
                               '{parent.get('NOME_FORNECEDOR')}')"
            sql = f"select distinct NR_CONVENIO from movimento \
                    where (IDENTIF_FORNECEDOR, NOME_FORNECEDOR)={id_fornecedor}"
            sql = f"select a.NR_EMENDA from emendas_convenios a inner join ({sql}) b \
                    on a.NR_CONVENIO = b.NR_CONVENIO"
            table_expression = f"(select c.* from emendas c inner join ({sql}) d \
                                  on c.NR_EMENDA = d.NR_EMENDA) e"

        else:
            raise Exception('load_emendas: Unknown parent')

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
                       'DATA': 'DATA',
                       'VALOR': 'VALOR',
                       'TIPO': 'TIPO',
                       'IDENTIF_FORNECEDOR': 'IDENTIF_FORNECEDOR',
                       'NOME_FORNECEDOR': 'NOME_FORNECEDOR'}

    if parent is not None:
        if parent.get('query') == 'convenios':
            table_expression = f"(select * from movimento \
                                  where NR_CONVENIO = {parent.get('NR_CONVENIO')}) m"            

        elif parent.get('query') == 'fornecedores':
            id_fornecedor = f"('{parent.get('IDENTIF_FORNECEDOR')}', \
                               '{parent.get('NOME_FORNECEDOR')}')"
            table_expression = f"(select * from movimento \
                                  where (IDENTIF_FORNECEDOR, NOME_FORNECEDOR) = {id_fornecedor}) m"

        else:
            raise Exception('load_movimento: Unknown parent')

    data, pagination = load_data(table_expression=table_expression, selected_fields=selected_fields,
                                  filters=filters, sort=sort,
                                  page_specs=page_specs, use_pagination=use_pagination)


    for d, _ in enumerate(data):
        data[d]['query'] = 'movimento'

    return data, pagination
