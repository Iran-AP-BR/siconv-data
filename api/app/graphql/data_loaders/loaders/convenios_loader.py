# coding: utf-8
"""convenios_loader.
    """

from app.graphql.data_loaders.loaders import load_data

def load_convenios(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None, **kwargs):
    
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
            sql = f"select NR_CONVENIO from movimento  \
                  where FORNECEDOR_ID = {parent.get('FORNECEDOR_ID')}"
            table_expression = f"(select a.* from {table_expression} a inner join ({sql}) b  \
                  on a.NR_CONVENIO = b.NR_CONVENIO)"

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

        elif parent.get('query') == 'parlamentares':
            sql = f"select distinct NR_EMENDA from emendas where \
                   (NOME_PARLAMENTAR, TIPO_PARLAMENTAR) = \
                   ('{parent.get('NOME_PARLAMENTAR')}', '{parent.get('TIPO_PARLAMENTAR')}')"
            sql = f"select a.NR_CONVENIO from emendas_convenios a \
                    inner join ({sql}) b on a.NR_EMENDA = b.NR_EMENDA"
            table_expression = f"(select c.* from {table_expression} c \
                    inner join ({sql}) d on c.NR_CONVENIO = d.NR_CONVENIO)"

        else:
            raise Exception(f'load_convenios - Unknown parent: {parent}')

    table_expression = f"{table_expression} table_expression"

    data, pagination = load_data(table_expression=table_expression, selected_fields=selected_fields,
                                  filters=filters, sort=sort,
                                  page_specs=page_specs, use_pagination=use_pagination)
    
    for d, _ in enumerate(data):
        data[d]['query'] = 'convenios'

    return data, pagination
