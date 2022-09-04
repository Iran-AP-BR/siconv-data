# coding: utf-8
"""municipios_loader.
    """

from app.graphql.data_loaders.loaders import load_data

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
            sql = f"select NR_CONVENIO from movimento  \
                  where FORNECEDOR_ID={parent.get('FORNECEDOR_ID')}"
            sql = f"select a.IDENTIF_PROPONENTE from convenios a inner join ({sql}) b  \
                  on a.NR_CONVENIO = b.NR_CONVENIO"
            sql = f"select distinct c.CODIGO_IBGE from proponentes c inner join ({sql}) d \
                  on c.IDENTIF_PROPONENTE = d.IDENTIF_PROPONENTE"
            table_expression = f"(select e.* from {table_expression} e \
                                inner join ({sql}) f on e.CODIGO_IBGE = f.CODIGO_IBGE)"

        elif parent.get('query') == 'convenios':
            sql = f"select distinct NR_CONVENIO, IDENTIF_PROPONENTE from convenios  \
                  where NR_CONVENIO={parent.get('NR_CONVENIO')}"
            sql = f"select a.CODIGO_IBGE from proponentes a inner join ({sql}) b \
                  on a.IDENTIF_PROPONENTE = b.IDENTIF_PROPONENTE"
            table_expression = f"(select c.* from {table_expression} c \
                                inner join ({sql}) d on c.CODIGO_IBGE = d.CODIGO_IBGE)"

        else:
            raise Exception(f'load_municipios - Unknown parent: {parent}')

    table_expression = f"{table_expression} table_expression"

    data, pagination = load_data(table_expression=table_expression, selected_fields=selected_fields,
                                  filters=filters, sort=sort,
                                  page_specs=page_specs, use_pagination=use_pagination)

    for d, _ in enumerate(data):
        data[d]['query'] = 'municipios'

    return data, pagination
