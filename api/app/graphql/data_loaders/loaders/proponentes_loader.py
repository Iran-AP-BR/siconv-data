# coding: utf-8
"""proponentes_loader.
    """

from app.graphql.data_loaders.loaders import load_data

def load_proponentes(page_specs=None, use_pagination=True, filters=None, parent=None, sort=None):

    table_expression = 'proponentes'
    selected_fields = {'IDENTIFICACAO': 'IDENTIF_PROPONENTE',
                       'NOME_PROPONENTE': 'NM_PROPONENTE',
                       'CODIGO_IBGE': 'CODIGO_IBGE'}

    if parent is not None:
        if parent.get('query') == 'municipios':
            table_expression = f"(select * from {table_expression} " \
                               f"where CODIGO_IBGE = '{parent.get('CODIGO_IBGE')}')"
        
        elif parent.get('query') == 'estados':
            sql = f"select distinct CODIGO_IBGE from municipios \
                    where UF = '{parent.get('SIGLA')}'"
            table_expression = f"(select a.* from {table_expression} a inner join ({sql}) b \
                                  on a.CODIGO_IBGE = b.CODIGO_IBGE)"
            
        elif parent.get('query') == 'convenios':
            table_expression = f"(select * from {table_expression} \
                                  where IDENTIF_PROPONENTE = '{parent.get('IDENTIF_PROPONENTE')}')"
            
        elif parent.get('query') == 'licitacoes':
            sql = f"select distinct IDENTIF_PROPONENTE from convenios \
                    where NR_CONVENIO = {parent.get('NR_CONVENIO')}"
            table_expression = f"(select a.* from {table_expression} a inner join ({sql}) b \
                                  on a.IDENTIF_PROPONENTE = b.IDENTIF_PROPONENTE)"
        
        elif parent.get('query') == 'fornecedores':
            sql = f"select NR_CONVENIO from movimento  \
                  where FORNECEDOR_ID={parent.get('FORNECEDOR_ID')}"
            sql = f"select distinct a.IDENTIF_PROPONENTE from convenios a inner join ({sql}) b \
                    on a.NR_CONVENIO = b.NR_CONVENIO"
            table_expression = f"(select c.* from {table_expression} c inner join ({sql}) d \
                                  on c.IDENTIF_PROPONENTE = d.IDENTIF_PROPONENTE)"

        elif parent.get('query') == 'parlamentares':
            sql = f"select distinct NR_EMENDA from emendas where \
                   (NOME_PARLAMENTAR, TIPO_PARLAMENTAR) = \
                   ('{parent.get('NOME_PARLAMENTAR')}', '{parent.get('TIPO_PARLAMENTAR')}')"
            sql = f"select a.NR_CONVENIO from emendas_convenios a \
                    inner join ({sql}) b on a.NR_EMENDA = b.NR_EMENDA"
            sql = f"select c.IDENTIF_PROPONENTE from convenios c \
                    inner join ({sql}) d on c.NR_CONVENIO = d.NR_CONVENIO"
            table_expression = f"(select e.* from {table_expression} e \
                    inner join ({sql}) f on e.IDENTIF_PROPONENTE = f.IDENTIF_PROPONENTE)"

        else:
            raise Exception(f'load_proponentes - Unknown parent: {parent}')

    table_expression = f"{table_expression} table_expression"

    data, pagination = load_data(table_expression=table_expression, selected_fields=selected_fields,
                                  filters=filters, sort=sort,
                                  page_specs=page_specs, use_pagination=use_pagination)

    for d, _ in enumerate(data):
        data[d]['query'] = 'proponentes'

    return data, pagination
