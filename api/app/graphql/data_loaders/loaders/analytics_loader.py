# coding: utf-8
"""analytics_loader.
    """

from app import db
from sqlalchemy import text
from app.graphql.data_loaders.filtering import filter_constructor
from app.graphql.data_loaders.loaders import load_data, set_period_filter


def load_analytics(parent=None, filters=None, data_inicial=None, data_final=None, **kwargs):

    selected_fields = {
                       'QUANTIDADE_CONVENIO': 'QUANTIDADE_CONVENIO',
                       }

    where_conditions = []
    if filters:
        where_conditions += [filter_constructor(filters=filters)]


    if parent is not None:
        if parent.get('query')== 'parlamentares':
            parent_filter = f"((NOME_PARLAMENTAR, TIPO_PARLAMENTAR) = \
                    ('{parent.get('NOME_PARLAMENTAR')}', '{parent.get('TIPO_PARLAMENTAR')}'))"

        elif parent.get('query') == 'proponentes':
            parent_filter = f"(IDENTIF_PROPONENTE = '{parent.get('IDENTIFICACAO')}')"
        
        elif parent.get('query') == 'convenios':
            parent_filter = f"(NR_CONVENIO = {parent.get('NR_CONVENIO')})"
        
        elif parent.get('query') == 'emendas':
            parent_filter = f"(NR_EMENDA = {parent.get('NR_EMENDA')})"

        elif parent.get('query') == 'fornecedores':
            parent_filter = f"(FORNECEDOR_ID = {parent.get('FORNECEDOR_ID')})"

        elif parent.get('query') == 'municipios':
            parent_filter = f"(CODIGO_IBGE = {parent.get('CODIGO_IBGE')})"

        elif parent.get('query') == 'estados':
            parent_filter = f"(UF = '{parent.get('SIGLA')}')"

        else:
            raise Exception(f"load_analytics - Unknown parent: {parent}")

        where_conditions += [parent_filter]
    
    
    where = ' and '.join(where_conditions)
    where = f"where {where}" if where else ''

    date_filter = set_period_filter('DATA_MOV', data_inicial, data_final)
    if date_filter:
        date_filter = f"{date_filter} and"


    mov_type_filter = "TIPO_MOV in ('P', 'T', 'D', 'C')"

    list_convenios = f"select NR_CONVENIO from \
                              (select * \
                              from convenios join (select * from proponentes \
                                                   join municipios using(CODIGO_IBGE)) prop_mun \
                                   using(IDENTIF_PROPONENTE)) conv_prop_mun \
                              join emendas_convenios using(NR_CONVENIO) \
                              join emendas using(NR_EMENDA) \
                              join (select NR_CONVENIO from movimento \
                                    join fornecedores using(FORNECEDOR_ID)) mov_forn \
                              using(NR_CONVENIO) {where} and {date_filter} {mov_type_filter}"

 
    sql = f"""select 
                    count(distinct NR_CONVENIO) as QUANTIDADE_CONVENIOS                    
                    from movimento 
                    where {date_filter} {mov_type_filter} and NR_CONVENIO in ({list_convenios})"""

    result = db.engine.execute(text(sql))
    
    data = [{list(selected_fields.keys())[p]: r for p, r in enumerate(row)} for row in result]
    
    for d, _ in enumerate(data):
        data[d]['query'] = 'analytics'
        data[d]['LIST_CONVENIOS'] = list_convenios
        data[d]['DATA_INICIAL'] = data_inicial
        data[d]['DATA_FINAL'] = data_final

    return data, None
    

def load_analytics_contrapartida(parent=None, **kwargs):
    try:
        selected_fields = {
                        'QUANTIDADE_CONVENIO': 'QUANTIDADE_CONVENIO',
                        'MEDIA_TRIBUTO': 'MEDIA_CONTRAPARTIDA',
                        'DESVIO_PADRAO_CONTRAPARTIDA': 'DESVIO_PADRAO_CONTRAPARTIDA',
                        'MAIOR_CONTRAPARTIDA': 'MAIOR_CONTRAPARTIDA',
                        'MENOR_CONTRAPARTIDA': 'MENOR_CONTRAPARTIDA',
                        'TOTAL_CONTRAPARTIDA': 'TOTAL_CONTRAPARTIDA',
                        'DATA_ULTIMO_CONTRAPARTIDA': 'DATA_ULTIMO_CONTRAPARTIDA',
                        'DATA_PRIMEIRO_CONTRAPARTIDA': 'DATA_PRIMEIRO_CONTRAPARTIDA',
                        'QUANTIDADE_CONTRAPARTIDA': 'QUANTIDADE_CONTRAPARTIDA',
                        }


        if parent.get('query') == 'analytics':        
            date_filter = set_period_filter('DATA_MOV', parent.get('DATA_INICIAL'), parent.get('DATA_FINAL'))
            if date_filter:
                date_filter = f"{date_filter} and"

        else:
            raise Exception(f"load_analytics_contrapartida - Unknown parent: {parent}")
    
        sql = f"""select 
                        count(distinct NR_CONVENIO) as QUANTIDADE_CONVENIOS,
                        round(avg(VALOR_MOV), 2) as MEDIA_CONTRAPARTIDA,
                        round(std(VALOR_MOV), 2) as DESVIO_PADRAO_CONTRAPARTIDA,
                        round(max(VALOR_MOV), 2) as MAIOR_CONTRAPARTIDA,
                        round(min(VALOR_MOV), 2) as MENOR_CONTRAPARTIDA,
                        round(sum(VALOR_MOV), 2) as TOTAL_CONTRAPARTIDA,
                        max(DATA_MOV) as DATA_ULTIMO_CONTRAPARTIDA,
                        min(DATA_MOV) as DATA_PRIMEIRO_CONTRAPARTIDA,
                        count(MOV_ID) as QUANTIDADE_CONTRAPARTIDA
                        
                        from movimento 
                        
                        where {date_filter} TIPO_MOV = 'C' and 
                              NR_CONVENIO in ({parent.get('LIST_CONVENIOS')})"""

        result = db.engine.execute(text(sql))
        
        data = [{list(selected_fields.keys())[p]: r for p, r in enumerate(row)} for row in result]

    except Exception as e:
        raise Exception(f"load_analytics_contrapartida: {repr(e)}")
    

    return data, None

def load_analytics_metrics(parent=None, metric=None, **kwargs):
    try:
        assert metric in ['P', 'C', 'T', 'D']
        selected_fields = {
                        'QUANTIDADE_CONVENIO': 'QUANTIDADE_CONVENIO',
                        'MEDIA': 'MEDIA',
                        'DESVIO_PADRAO': 'DESVIO_PADRAO',
                        'MAIOR_VALOR': 'MAIOR_VALOR',
                        'MENOR_VALOR': 'MENOR_VALOR',
                        'TOTAL': 'TOTAL',
                        'DATA_ULTIMO_EVENTO': 'DATA_ULTIMO_EVENTO',
                        'DATA_PRIMEIRO_EVENTO': 'DATA_PRIMEIRO_EVENTO',
                        'QUANTIDADE_EVENTO': 'QUANTIDADE_EVENTO',
                        }


        if parent.get('query') == 'analytics':        
            date_filter = set_period_filter('DATA_MOV', parent.get('DATA_INICIAL'), parent.get('DATA_FINAL'))
            if date_filter:
                date_filter = f"{date_filter} and"

        else:
            raise Exception(f"load_analytics_metrics - Unknown parent: {parent}")
    
        sql = f"""select 
                        count(distinct NR_CONVENIO) as QUANTIDADE_CONVENIOS,
                        round(avg(VALOR_MOV), 2) as MEDIA,
                        round(std(VALOR_MOV), 2) as DESVIO_PADRAO,
                        round(max(VALOR_MOV), 2) as MAIOR_VALOR,
                        round(min(VALOR_MOV), 2) as MENOR_VALOR,
                        round(sum(VALOR_MOV), 2) as TOTAL,
                        max(DATA_MOV) as DATA_ULTIMO_EVENTO,
                        min(DATA_MOV) as DATA_PRIMEIRO_EVENTO,
                        count(MOV_ID) as QUANTIDADE_EVENTO
                        
                        from movimento 
                        
                        where {date_filter} TIPO_MOV = '{metric}' and 
                              NR_CONVENIO in ({parent.get('LIST_CONVENIOS')})"""

        result = db.engine.execute(text(sql))
        
        data = [{list(selected_fields.keys())[p]: r for p, r in enumerate(row)} for row in result]

    except Exception as e:
        raise Exception(f"load_analytics_contrapartida: {repr(e)}")
    

    return data, None


def load_top_fornecedores(use_pagination=True, parent=None, top_type='value', top_n='10', **kwargs):
    table_expression = "select * from (select * from fornecedores where FORNECEDOR_ID <> -1) forn \
                        join movimento mov using(FORNECEDOR_ID)"
    selected_fields = {'FORNECEDOR_ID': 'FORNECEDOR_ID',
                       'IDENTIF_FORNECEDOR': 'IDENTIF_FORNECEDOR',
                       'NOME_FORNECEDOR': 'NOME_FORNECEDOR',
                       'QUANTIDADE_CONVENIOS': 'QUANTIDADE_CONVENIOS',
                       'DESVIO_PADRAO_PAGAMENTO': 'DESVIO_PADRAO_PAGAMENTO',
                       'MEDIA_PAGAMENTO': 'MEDIA_PAGAMENTO',
                       'TOTAL_PAGAMENTO': 'TOTAL_PAGAMENTO',
                        }
    
    if top_type.lower() == 'value':
        group_clause = 'group by FORNECEDOR_ID order by TOTAL_PAGAMENTO desc'
    
    elif top_type.lower() == 'quantity':
        group_clause = 'group by FORNECEDOR_ID order by QUANTIDADE_CONVENIOS desc'

    elif top_type.lower() == 'average':
        group_clause = 'group by FORNECEDOR_ID order by MEDIA_PAGAMENTO desc'

    elif top_type.lower() == 'deviation':
        group_clause = 'group by FORNECEDOR_ID having count(MOV_ID) >= 10 order by DESVIO_PADRAO_PAGAMENTO asc'

    else:

        raise Exception('load_top_fornecedores: Invalid top_type parameter, must be "value" or "quantity"')

    if parent.get('query') == 'analytics':        
        date_filter = set_period_filter('DATA_MOV', parent.get('DATA_INICIAL'), parent.get('DATA_FINAL'))
        if date_filter:
            date_filter = f"{date_filter} and"

        table_expression = f"(select \
                            FORNECEDOR_ID, \
                            max(IDENTIF_FORNECEDOR) as IDENTIF_FORNECEDOR, \
                            max(NOME_FORNECEDOR) as NOME_FORNECEDOR, \
                            count(distinct NR_CONVENIO) as QUANTIDADE_CONVENIOS, \
                            round(sum(VALOR_MOV), 2) as TOTAL_PAGAMENTO, \
                            round(avg(VALOR_MOV), 2) as MEDIA_PAGAMENTO, \
                            round(std(VALOR_MOV), 2) as DESVIO_PADRAO_PAGAMENTO \
                            from ({table_expression}) a \
                            where {date_filter} NR_CONVENIO in ({parent.get('LIST_CONVENIOS')}) \
                            {group_clause} \
                            limit {top_n})"          

    else:
        raise Exception(f"load_top_fornecedores - Unknown parent: {parent}")

    table_expression = f"{table_expression} table_expression"

    data, _ = load_data(table_expression=table_expression, selected_fields=selected_fields,
                                  use_pagination=use_pagination)

    for d, _ in enumerate(data):
        data[d]['query'] = 'top_fornecedores'

    return data, None

