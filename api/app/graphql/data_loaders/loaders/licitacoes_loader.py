# coding: utf-8
"""licitacoes_loader.
    """

from app.graphql.data_loaders.loaders import load_data

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

        elif parent.get('query') == 'parlamentares':
            sql = f"select distinct NR_EMENDA from emendas where \
                   (NOME_PARLAMENTAR, TIPO_PARLAMENTAR) = \
                   ('{parent.get('NOME_PARLAMENTAR')}', '{parent.get('TIPO_PARLAMENTAR')}')"
            sql = f"select a.NR_CONVENIO from emendas_convenios a \
                    inner join ({sql}) b on a.NR_EMENDA = b.NR_EMENDA"
            table_expression = f"(select c.* from {table_expression} c \
                    inner join ({sql}) d on c.NR_CONVENIO = d.NR_CONVENIO)"

        else:
            raise Exception(f'load_movimento - Unknown parent: {parent}')

    table_expression = f"{table_expression} table_expression"

    data, pagination = load_data(table_expression=table_expression, selected_fields=selected_fields,
                                  filters=filters, sort=sort,
                                  page_specs=page_specs, use_pagination=use_pagination)


    for d, _ in enumerate(data):
        data[d]['query'] = 'licitacoes'

    return data, pagination
