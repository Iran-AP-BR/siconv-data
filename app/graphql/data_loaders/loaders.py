# coding: utf-8
"""loaders.
   """

import pandas as pd
from . import page_settings, pagination, load_data
from .filtering import filter_constructor

dtypes_convenios = {'NR_CONVENIO': 'object',
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

parse_dates_convenios = ['DIA_ASSIN_CONV', 'DIA_PUBL_CONV', 'DIA_INIC_VIGENC_CONV', 'DIA_FIM_VIGENC_CONV', 'DIA_LIMITE_PREST_CONTAS']
parse_dates_movimento = ['DATA']

def load_convenios(page_specs=None, _emendas=None, parameters={}):    
    q = None
    conditions = None
    if not parameters.get('NR_EMENDA'):

        conditions = filter_constructor(parameters=parameters, dtypes=dtypes_convenios, parse_dates=parse_dates_convenios)
        convenios = load_data('convenios', dtypes=dtypes_convenios, parse_dates=parse_dates_convenios)
        if _emendas is not None:
            emendas_convenios = load_data('emendas_convenios')
            if _emendas:
                convenios = convenios[convenios['NR_CONVENIO'].isin(emendas_convenios['NR_CONVENIO'])]
            else:
                convenios = convenios[~convenios['NR_CONVENIO'].isin(emendas_convenios['NR_CONVENIO'])]

        if conditions:
            convenios = convenios.query(conditions)

    else:
        emendas_convenios = load_data('emendas_convenios')
        emendas_convenios = emendas_convenios[emendas_convenios['NR_EMENDA']==parameters.get('NR_EMENDA')]    
        convenios = load_data('convenios', dtypes=dtypes_convenios, parse_dates=parse_dates_convenios)
        convenios = convenios[convenios['NR_CONVENIO'].isin(emendas_convenios['NR_CONVENIO'])]

    if page_specs:
        items_count, page_count, idx_first, idx_last = page_settings(convenios, page_specs)
        convenios = convenios[idx_first:idx_last]

    convenios_d = convenios.to_dict('records')

    return convenios_d, pagination(page_specs, page_count, items_count) if page_specs else None


def load_emendas(page_specs=None, parameters={}):
    conditions = None
    if not parameters.get('NR_CONVENIO'):
        conditions = filter_constructor(parameters=parameters, dtypes=dtypes_emendas)
        emendas = load_data('emendas', dtypes=dtypes_emendas)
    else:
        emendas_convenios = load_data('emendas_convenios')
        emendas_convenios = emendas_convenios[emendas_convenios['NR_CONVENIO']==parameters.get('NR_CONVENIO')]    
        emendas = load_data('emendas', dtypes=dtypes_emendas)
        emendas = emendas[emendas['NR_EMENDA'].isin(emendas_convenios['NR_EMENDA'])]
    
    if conditions:
        emendas = emendas.query(conditions)
    
    if page_specs:
        items_count, page_count, idx_first, idx_last = page_settings(emendas, page_specs)
        emendas = emendas[idx_first:idx_last]

    emendas_d = emendas.to_dict('records')
    
    return emendas_d, pagination(page_specs, page_count, items_count) if page_specs else None

def load_proponentes(page_specs=None, parameters={}):
    conditions = filter_constructor(parameters=parameters, dtypes=dtypes_proponentes)
    proponentes = load_data('proponentes', dtypes=dtypes_proponentes)

    if conditions:
        proponentes = proponentes.query(conditions)

    if page_specs:
        items_count, page_count, idx_first, idx_last = page_settings(proponentes, page_specs)
        proponentes = proponentes[idx_first:idx_last]

    proponentes_d = proponentes.to_dict('records')

    return proponentes_d, pagination(page_specs, page_count, items_count) if page_specs else None

def load_movimento(page_specs=None, parameters={}):

    conditions = filter_constructor(parameters=parameters, dtypes=dtypes_movimento)
    movimento = load_data('movimento', dtypes=dtypes_movimento, parse_dates=parse_dates_movimento)

    if conditions:
        movimento = movimento.query(conditions)

    if page_specs:
        items_count, page_count, idx_first, idx_last = page_settings(movimento, page_specs)
        movimento = movimento[idx_first:idx_last]

    movimento_d = movimento.to_dict('records')
 
    return movimento_d, pagination(page_specs, page_count, items_count) if page_specs else None
