# coding: utf-8
"""emendas_loader.
    """

from app.graphql.data_loaders.loaders import load_data

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
            sql = f"select NR_CONVENIO from movimento  \
                  where FORNECEDOR_ID = {parent.get('FORNECEDOR_ID')}"
            sql = f"select distinct a.NR_EMENDA from emendas_convenios a inner join ({sql}) b \
                    on a.NR_CONVENIO = b.NR_CONVENIO"
            table_expression = f"(select c.* from {table_expression} c inner join ({sql}) d \
                                  on c.NR_EMENDA = d.NR_EMENDA)"

        elif parent.get('query') == 'parlamentares':
            table_expression = f"(select * from {table_expression} where \
                                (NOME_PARLAMENTAR, TIPO_PARLAMENTAR) = \
                                ('{parent.get('NOME_PARLAMENTAR')}', '{parent.get('TIPO_PARLAMENTAR')}'))"

        else:
            raise Exception(f'load_emendas - Unknown parent: {parent}')

    table_expression = f"{table_expression} table_expression"

    data, pagination = load_data(table_expression=table_expression, selected_fields=selected_fields,
                                  filters=filters, sort=sort,
                                  page_specs=page_specs, use_pagination=use_pagination)

    for d, _ in enumerate(data):
        data[d]['query'] = 'emendas'

    return data, pagination
