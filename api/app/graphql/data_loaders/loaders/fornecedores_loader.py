# coding: utf-8
"""fornecedores_loader.
    """

from app.graphql.data_loaders.loaders import load_data

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

        elif parent.get('query') == 'parlamentares':
            sql = f"select distinct NR_EMENDA from emendas where \
                   (NOME_PARLAMENTAR, TIPO_PARLAMENTAR) = \
                   ('{parent.get('NOME_PARLAMENTAR')}', '{parent.get('TIPO_PARLAMENTAR')}')"
            sql = f"select a.NR_CONVENIO from emendas_convenios a \
                    inner join ({sql}) b on a.NR_EMENDA = b.NR_EMENDA"
            sql = f"select c.FORNECEDOR_ID from movimento c \
                    inner join ({sql}) d on c.NR_CONVENIO = d.NR_CONVENIO"
            table_expression = f"(select e.* from {table_expression} e \
                    inner join ({sql}) f on e.FORNECEDOR_ID = f.FORNECEDOR_ID)"

        else:
            raise Exception(f'load_fornecedores - Unknown parent: {parent}')

    
    table_expression = f"{table_expression} table_expression"
    
    data, pagination = load_data(table_expression=table_expression, selected_fields=selected_fields,
                                  filters=filters, sort=sort, page_specs=page_specs,
                                  use_pagination=use_pagination, distinct_clause=True)

    for d, _ in enumerate(data):
        data[d]['query'] = 'fornecedores'

    return data, pagination
