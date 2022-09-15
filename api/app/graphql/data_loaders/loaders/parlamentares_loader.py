# coding: utf-8
"""parlamentares_loader.
    """

from app.graphql.data_loaders.loaders import load_data

def load_parlamentares(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None, **kwargs):

    table_expression = '(select distinct NOME_PARLAMENTAR, TIPO_PARLAMENTAR from emendas)'
    selected_fields = {'NOME_PARLAMENTAR': 'NOME_PARLAMENTAR',
                       'TIPO_PARLAMENTAR': 'TIPO_PARLAMENTAR'
                       }

    if parent is not None:
        if parent.get('query') == 'fornecedores':
            sql = f"select NR_CONVENIO from movimento  \
                  where FORNECEDOR_ID = {parent.get('FORNECEDOR_ID')}"
            sql = f"select a.NR_EMENDA from emendas_convenios a inner join ({sql}) b  \
                  on a.NR_CONVENIO = b.NR_CONVENIO"
            table_expression = f"(select distinct c.NOME_PARLAMENTAR, c.TIPO_PARLAMENTAR from emendas c \
                    inner join ({sql}) d on c.NR_EMENDA = d.NR_EMENDA)"
        
        elif parent.get('query') == 'proponentes':
            sql = f"select NR_CONVENIO from convenios  \
                  where IDENTIF_PROPONENTE = '{parent.get('IDENTIFICACAO')}'"
            sql = f"select a.NR_EMENDA from emendas_convenios a inner join ({sql}) b  \
                  on a.NR_CONVENIO = b.NR_CONVENIO"
            table_expression = f"(select distinct c.NOME_PARLAMENTAR, c.TIPO_PARLAMENTAR from emendas c \
                    inner join ({sql}) d on c.NR_EMENDA = d.NR_EMENDA)"
        
        elif parent.get('query') == 'convenios':
            sql = f"select NR_EMENDA from emendas_convenios  \
                  where NR_CONVENIO = {parent.get('NR_CONVENIO')}"
            table_expression = f"(select distinct c.NOME_PARLAMENTAR, c.TIPO_PARLAMENTAR from emendas c \
                    inner join ({sql}) d on c.NR_EMENDA = d.NR_EMENDA)"

        else:
            raise Exception(f'load_parlamentares - Unknown parent: {parent}')

    table_expression = f"{table_expression} table_expression"

    data, pagination = load_data(table_expression=table_expression, selected_fields=selected_fields,
                                  filters=filters, sort=sort,
                                  page_specs=page_specs, use_pagination=use_pagination)

    for d, _ in enumerate(data):
        data[d]['query'] = 'parlamentares'

    return data, pagination
