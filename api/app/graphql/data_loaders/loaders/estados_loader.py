# coding: utf-8
"""estados_loader.
    """

from app.graphql.data_loaders.loaders import load_data

def load_estados(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None, **kwargs):
    
    table_expression = 'municipios'
    selected_fields = {'SIGLA': 'UF',
                       'NOME': 'NOME_ESTADO'}

    if parent is not None:            
        if parent.get('query') == 'fornecedores':
            sql = f"select NR_CONVENIO from movimento  \
                  where FORNECEDOR_ID = {parent.get('FORNECEDOR_ID')}"
            sql = f"select a.IDENTIF_PROPONENTE from convenios a inner join ({sql}) b  \
                  on a.NR_CONVENIO = b.NR_CONVENIO"
            sql = f"select c.CODIGO_IBGE from proponentes c inner join ({sql}) d \
                  on c.IDENTIF_PROPONENTE = d.IDENTIF_PROPONENTE"
            table_expression = f"(select distinct e.UF, e.NOME_ESTADO from {table_expression} e \
                    inner join ({sql}) f on e.CODIGO_IBGE = f.CODIGO_IBGE)"

        elif parent.get('query') == 'convenios':
            sql = f"select IDENTIF_PROPONENTE from convenios where \
                    NR_CONVENIO = {parent.get('NR_CONVENIO')}"
            sql = f"select a.CODIGO_IBGE from proponentes a inner join ({sql}) b \
                  on a.IDENTIF_PROPONENTE = b.IDENTIF_PROPONENTE"
            table_expression = f"(select distinct c.UF, c.NOME_ESTADO from {table_expression} c \
                                inner join ({sql}) d on c.CODIGO_IBGE = d.CODIGO_IBGE)"

        elif parent.get('query') == 'proponentes':
            sql = f"select CODIGO_IBGE from proponentes \
                    where IDENTIF_PROPONENTE='{parent.get('IDENTIFICACAO')}'"
            table_expression = f"(select distinct a.UF, a.NOME_ESTADO from {table_expression} a \
                                 inner join ({sql}) b on a.CODIGO_IBGE = b.CODIGO_IBGE)"

        else:
            raise Exception(f'load_estados - Unknown parent: {parent}')

    table_expression = f"{table_expression} table_expression"

    data, pagination = load_data(table_expression=table_expression, selected_fields=selected_fields,
                                  filters=filters, sort=sort, page_specs=page_specs,
                                  use_pagination=use_pagination)

    for d, _ in enumerate(data):
        data[d]['query'] = 'estados'

    return data, pagination
