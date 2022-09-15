# coding: utf-8
"""analytics_loader.
    """

from app import db
from sqlalchemy import text
from app.graphql.data_loaders.filtering import filter_constructor
from app.graphql.data_loaders.loaders import set_period_filter, make_where_clause


def load_analytics(parent=None, filters=None, data_inicial=None, data_final=None, **kwargs):
   
    def base_sql(vigencia_where_clause=None, data_mov_where_clause=None):
        assert not vigencia_where_clause or not data_mov_where_clause, \
                'assertion exception : load_analytics/base_sql'

        if vigencia_where_clause:
            tbl_convenios = f'(select * from convenios where {vigencia_where_clause}) conv'
        else:
            tbl_convenios = 'convenios'
            
        if data_mov_where_clause:
            tbl_movimento = f'(select * from movimento where {data_mov_where_clause}) mov'
        else:
            tbl_movimento = 'movimento'

        return f"""(select * from {tbl_convenios} join proponentes using(IDENTIF_PROPONENTE) 
                join municipios using(CODIGO_IBGE)) conv_prop_mun left join licitacoes using(NR_CONVENIO) 
                left join (select * from emendas_convenios join emendas using(NR_EMENDA)) emd 
                using(NR_CONVENIO) left join (select * from {tbl_movimento} join fornecedores 
                using(FORNECEDOR_ID)) mov_forn using(NR_CONVENIO)
                """
  

    filters_list = [filter_constructor(filters=filters)] if filters else []

    if parent is not None:
        if parent.get('query') == 'parlamentares':
            parent_filters_sub_clause = f"((NOME_PARLAMENTAR, TIPO_PARLAMENTAR) = \
                    ('{parent.get('NOME_PARLAMENTAR')}', '{parent.get('TIPO_PARLAMENTAR')}'))"

        elif parent.get('query') == 'proponentes':
            parent_filters_sub_clause = f"(IDENTIF_PROPONENTE = '{parent.get('IDENTIFICACAO')}')"
        
        elif parent.get('query') == 'convenios':
            parent_filters_sub_clause = f"(NR_CONVENIO = {parent.get('NR_CONVENIO')})"
        
        elif parent.get('query') == 'emendas':
            parent_filters_sub_clause = f"(NR_EMENDA = {parent.get('NR_EMENDA')})"

        elif parent.get('query') == 'fornecedores':
            parent_filters_sub_clause = f"(FORNECEDOR_ID = {parent.get('FORNECEDOR_ID')})"

        elif parent.get('query') == 'municipios':
            parent_filters_sub_clause = f"(CODIGO_IBGE = {parent.get('CODIGO_IBGE')})"

        elif parent.get('query') == 'estados':
            parent_filters_sub_clause = f"(UF = '{parent.get('SIGLA')}')"

        else:

            raise Exception(f"load_analytics - Unknown parent: {parent}")

        filters_list += [parent_filters_sub_clause]
    
    filters_clause = make_where_clause(conditions=filters_list, where_preffix=False)
    


    convenios_where_list = [f"({filters_clause})"] if filters_clause else []
    convenios_where_list += ["(TIPO_MOV in ('P', 'T', 'D', 'C'))"]
    convenios_where_clause = make_where_clause(conditions=convenios_where_list)

    analytics_where_clause = make_where_clause(conditions=filters_clause)

    vigencia_where_clause = set_period_filter(field_name=['DIA_INIC_VIGENC_CONV', 'DIA_FIM_VIGENC_CONV'], 
                                             data_inicial=data_inicial, data_final=data_final)
    data_mov_where_clause = set_period_filter(field_name='DATA_MOV', data_inicial=data_inicial,
                                             data_final=data_final)



    data_atual = db.engine.execute(text("select DATA_ATUAL from data_atual")).scalar()

    sql_ = f"""select distinct NR_CONVENIO, SIT_CONVENIO, COM_EMENDAS, INSUCESSO, 
               DIA_INIC_VIGENC_CONV, DIA_FIM_VIGENC_CONV,
               VL_GLOBAL_CONV, VL_REPASSE_CONV, VL_CONTRAPARTIDA_CONV, VALOR_EMENDA_CONVENIO
               from ({base_sql(vigencia_where_clause=vigencia_where_clause)}) {analytics_where_clause}"""
    
    sql = f"""select 
            count(NR_CONVENIO) as QUANTIDADE_CONVENIO,                
            count(case when SIT_CONVENIO='EM EXECUÇÃO' then NR_CONVENIO end) as QUANTIDADE_EM_EXECUCAO,
            count(case when COM_EMENDAS='SIM' then NR_CONVENIO end) as QUANTIDADE_COM_EMENDAS,
            count(case when (COM_EMENDAS, SIT_CONVENIO)=('SIM', 'EM EXECUÇÃO') 
                       then NR_CONVENIO end) as QUANTIDADE_COM_EMENDAS_EM_EXECUCAO,
            count(case when SIT_CONVENIO='EM EXECUÇÃO' and INSUCESSO >= 0.5 
                       then NR_CONVENIO end) as QUANTIDADE_COM_RISCO,
            sum(VL_REPASSE_CONV) as VALOR_REPASSE,
            sum(VL_CONTRAPARTIDA_CONV) as VALOR_CONTRAPARTIDA,
            sum(VALOR_EMENDA_CONVENIO) as VALOR_EMENDA,
            sum(VL_GLOBAL_CONV) as VALOR_GLOBAL, 
            sum(case when SIT_CONVENIO='EM EXECUÇÃO' then VL_GLOBAL_CONV end) as VALOR_CONVENIO_EM_EXECUCAO,
            sum(case when COM_EMENDAS='SIM' then VL_GLOBAL_CONV end) as VALOR_CONVENIO_COM_EMENDAS,
            sum(case when (COM_EMENDAS, SIT_CONVENIO)=('SIM', 'EM EXECUÇÃO') 
                       then VL_GLOBAL_CONV end) as VALOR_CONVENIO_COM_EMENDAS_EM_EXECUCAO,
            sum(case when SIT_CONVENIO='EM EXECUÇÃO' and INSUCESSO >= 0.5 
                     then VL_GLOBAL_CONV end) as VALOR_CONVENIO_COM_RISCO,
            min(DIA_INIC_VIGENC_CONV) as VIGENCIA_INICIAL,
            max(DIA_FIM_VIGENC_CONV) as VIGENCIA_FINAL,
            '{data_atual}' as DATA_ATUAL
            
            from ({sql_}) conv

            having QUANTIDADE_CONVENIO > 0"""



    result = db.engine.execute(text(sql))

    data = result.one_or_none()

    data = dict(data if data else {}) 
    data['query'] = 'analytics'
    data['MOVIMENTO'] = f"""select distinct MOV_ID, NR_CONVENIO, DATA_MOV, TIPO_MOV, VALOR_MOV, 
                            FORNECEDOR_ID, IDENTIF_FORNECEDOR, NOME_FORNECEDOR 
                            from ({base_sql(data_mov_where_clause=data_mov_where_clause)})
                            {convenios_where_clause}"""
    data['LICITACAO'] = f"""select distinct ID_LICITACAO, NR_CONVENIO, MODALIDADE_COMPRA, 
                            TIPO_LICITACAO, FORMA_LICITACAO, REGISTRO_PRECOS, LICITACAO_INTERNACIONAL,
                            STATUS_LICITACAO, VALOR_LICITACAO 
                            from ({base_sql(vigencia_where_clause=vigencia_where_clause)})
                            {analytics_where_clause}"""
    data['DATA_INICIAL'] = data_inicial
    data['DATA_FINAL'] = data_final

    return data, None
    

def load_analytics_metrics_movimento(parent=None, metric=None, **kwargs):
    try:
        assert metric in ['P', 'C', 'T', 'D']

        if parent.get('query') != 'analytics':        
            raise Exception(f"load_analytics_metrics_movimento - Unknown parent: {parent}")

    
        sql = f"""select 
                count(distinct NR_CONVENIO) as QUANTIDADE_CONVENIO,
                round(avg(VALOR_MOV), 2) as MEDIA,
                round(std(VALOR_MOV), 2) as DESVIO_PADRAO,
                round(max(VALOR_MOV), 2) as MAIOR_VALOR,
                round(min(VALOR_MOV), 2) as MENOR_VALOR,
                round(sum(VALOR_MOV), 2) as TOTAL,
                max(DATA_MOV) as DATA_ULTIMO_EVENTO,
                min(DATA_MOV) as DATA_PRIMEIRO_EVENTO,
                count(MOV_ID) as QUANTIDADE_EVENTO
                
                from ({parent.get('MOVIMENTO')}) mov2
                
                where TIPO_MOV = '{metric}'
                having QUANTIDADE_CONVENIO > 0"""

        result = db.engine.execute(text(sql))
        
        data_ = result.one_or_none()
        data = dict(data_) if data_ else None

    except Exception as e:
        raise Exception(f"load_analytics_metrics_movimento: {repr(e)}")
    
    return data, None


def load_analytics_metrics_licitacao(parent=None, **kwargs):
    try:

        if parent.get('query') != 'analytics':        
            raise Exception(f"load_analytics_metrics_licitacao - Unknown parent: {parent}")
    
        sql = f"""select 
                count(distinct NR_CONVENIO) as QUANTIDADE_CONVENIO,
                count(ID_LICITACAO) as QUANTIDADE_LICITACAO,
                count(case when REGISTRO_PRECOS='SIM' 
                           then ID_LICITACAO end) as QUANTIDADE_REGISTRO_PRECOS,
                count(case when LICITACAO_INTERNACIONAL='SIM' 
                           then ID_LICITACAO end) as QUANTIDADE_INTERNACIONAL,
                round(sum(VALOR_LICITACAO), 2) as TOTAL,
                round(min(VALOR_LICITACAO), 2) as MENOR_VALOR,
                round(max(VALOR_LICITACAO), 2) as MAIOR_VALOR,
                round(avg(VALOR_LICITACAO), 2) as MEDIA,
                round(std(VALOR_LICITACAO), 2) as DESVIO_PADRAO

                from ({parent.get('LICITACAO')}) lic
                
                having QUANTIDADE_CONVENIO > 0"""

        result = db.engine.execute(text(sql))
        
        data_ = result.one_or_none()
        data = dict(data_) if data_ else None

    except Exception as e:
        raise Exception(f"load_analytics_metrics_licitacao: {repr(e)}")
    
    return data, None


def load_top_fornecedores(parent=None, top_type='value', top_n='10', **kwargs):
    
    if parent.get('query') != 'analytics': 
        raise Exception(f"load_top_fornecedores - Unknown parent: {parent}")
    

    if top_type.lower() == 'value':
        group_clause = 'group by FORNECEDOR_ID order by \
                        TOTAL_PAGAMENTO desc, MEDIA_PAGAMENTO desc, QUANTIDADE_CONVENIO desc'
    
    elif top_type.lower() == 'quantity':
        group_clause = 'group by FORNECEDOR_ID order by \
                        QUANTIDADE_CONVENIO desc, TOTAL_PAGAMENTO desc'

    elif top_type.lower() == 'average':
        group_clause = 'group by FORNECEDOR_ID order by \
                        MEDIA_PAGAMENTO desc, TOTAL_PAGAMENTO desc'

    else:

        raise Exception('load_top_fornecedores: Invalid top_type parameter, \
                         must be "value" or "quantity" or average')

    
    sql = f"""(select 
                FORNECEDOR_ID,
                max(IDENTIF_FORNECEDOR) as IDENTIF_FORNECEDOR,
                max(NOME_FORNECEDOR) as NOME_FORNECEDOR,
                count(distinct NR_CONVENIO) as QUANTIDADE_CONVENIO, 
                round(sum(VALOR_MOV), 2) as TOTAL_PAGAMENTO, 
                round(avg(VALOR_MOV), 2) as MEDIA_PAGAMENTO, 
                round(std(VALOR_MOV), 2) as DESVIO_PADRAO_PAGAMENTO 
                from ({parent.get('MOVIMENTO')}) a 
                where FORNECEDOR_ID <> -1
                {group_clause} 
                limit {top_n})"""


    result = db.engine.execute(text(sql))
    
    data = [dict(row) for row in result]

    return data, None