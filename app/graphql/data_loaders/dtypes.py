# coding: utf-8
"""dtypes.
   """

dtypes_convenios = {
    'NR_CONVENIO': 'object',
    'DIA_ASSIN_CONV': 'object',
    'SIT_CONVENIO': 'object',
    'INSTRUMENTO_ATIVO': 'object',
    'DIA_PUBL_CONV': 'object',
    'DIA_INIC_VIGENC_CONV': 'object',
    'DIA_FIM_VIGENC_CONV': 'object',
    'DIA_LIMITE_PREST_CONTAS': 'object',
    'VL_GLOBAL_CONV': 'float64',
    'VL_REPASSE_CONV': 'float64',
    'VL_CONTRAPARTIDA_CONV': 'float64',
    'COD_ORGAO_SUP': 'object',
    'DESC_ORGAO_SUP': 'object',
    'NATUREZA_JURIDICA': 'object',
    'COD_ORGAO': 'object',
    'DESC_ORGAO': 'object',
    'MODALIDADE': 'object',
    'IDENTIF_PROPONENTE': 'object',
    'OBJETO_PROPOSTA': 'object'
}

dtypes_emendas = {
    'NR_EMENDA': 'object',
    'NOME_PARLAMENTAR': 'object',
    'TIPO_PARLAMENTAR': 'object'
}

dtypes_proponentes = {
    'IDENTIF_PROPONENTE': 'object',
    'NM_PROPONENTE': 'object',
    'UF_PROPONENTE': 'object',
    'MUNIC_PROPONENTE': 'object',
    'COD_MUNIC_IBGE': 'object'
}

dtypes_movimento = {
    'NR_CONVENIO': 'object',
    'DATA': 'object',
    'VALOR': 'float64',
    'TIPO': 'object',
    'IDENTIF_FORNECEDOR': 'object',
    'NOME_FORNECEDOR': 'object'
}

dtypes_emendas_convenios = {
    'NR_EMENDA': 'object',
    'NR_CONVENIO': 'object',
    'VALOR_REPASSE_EMENDA': 'float64'
}

dtypes_municipios = {
    'codigo_ibge': 'object',
    'nome_municipio': 'object',
    'codigo_uf': 'object',
    'uf': 'object',
    'estado': 'object',
    'latitude': 'float64',
    'longitude': 'float64'
}

parse_dates_convenios = ['DIA_ASSIN_CONV', 'DIA_PUBL_CONV',
                         'DIA_INIC_VIGENC_CONV', 'DIA_FIM_VIGENC_CONV', 'DIA_LIMITE_PREST_CONTAS']
parse_dates_movimento = ['DATA']
