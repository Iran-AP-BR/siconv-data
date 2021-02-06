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

def load_convenios(page=1, page_length=1, _emendas=None, parameters={}):    
    conditions = filter_constructor(parameters=parameters, dtypes=dtypes_convenios, parse_dates=parse_dates_convenios)

    convenios = load_data('convenios', dtypes=dtypes_convenios, parse_dates=parse_dates_convenios)
    emendas_convenios = load_data('emendas_convenios')
    emendas = load_data('emendas')
    proponentes = load_data('proponentes')
    movimento = load_data('movimento')

    if _emendas is not None:
        q = convenios['NR_CONVENIO'].isin(emendas_convenios['NR_CONVENIO'].to_list())
        if not _emendas:
            q = ~q
        
        convenios = convenios.query(conditions)[q] if conditions else convenios[q]
    else:
        if conditions:
            convenios = convenios.query(conditions)

    items_count, page_count, idx_first, idx_last = page_settings(convenios, page, page_length)

    convenios = convenios[idx_first:idx_last]
    convenios_d = convenios.to_dict('records')

    for i in range(len(convenios_d)):
        ec = emendas_convenios[emendas_convenios['NR_CONVENIO'] == convenios_d[i]['NR_CONVENIO']]
        convenios_d[i]['EMENDAS'] = emendas[emendas['NR_EMENDA'].isin(ec['NR_EMENDA'].to_list())].to_dict('records')
        convenios_d[i]['PROPONENTE'] = proponentes[proponentes['IDENTIF_PROPONENTE'] == convenios_d[i]['IDENTIF_PROPONENTE']][:1].to_dict('records')[0]
        convenios_d[i]['MOVIMENTO'] = movimento[movimento['NR_CONVENIO'] == convenios_d[i]['NR_CONVENIO']].to_dict('records')

    return convenios_d, pagination(page, page_length, page_count, items_count)


def load_emendas(page=1, page_length=1, parameters={}):

    conditions = filter_constructor(parameters=parameters, dtypes=dtypes_emendas)
    emendas = load_data('emendas', dtypes=dtypes_emendas)
    
    emendas_convenios = load_data('emendas_convenios')
    convenios = load_data('convenios', dtypes=dtypes_convenios, parse_dates=parse_dates_convenios)

    if conditions:
        emendas = emendas.query(conditions)
        
    items_count, page_count, idx_first, idx_last = page_settings(emendas, page, page_length)
    emendas = emendas[idx_first:idx_last]

    emendas_d = emendas.to_dict('records')
    for i in range(len(emendas_d)):
        ec = emendas_convenios[emendas_convenios['NR_EMENDA'] == emendas_d[i]['NR_EMENDA']]
        emendas_d[i]['CONVENIOS'] = convenios[convenios['NR_CONVENIO'].isin(ec['NR_CONVENIO'].to_list())].to_dict('records')

    return emendas_d, pagination(page, page_length, page_count, items_count)

def load_proponentes(page=1, page_length=1, parameters={}):

    conditions = filter_constructor(parameters=parameters, dtypes=dtypes_proponentes)
    proponentes = load_data('proponentes', dtypes=dtypes_proponentes)
    convenios = load_data('convenios', dtypes=dtypes_convenios, parse_dates=parse_dates_convenios)

    if conditions:
        proponentes = proponentes.query(conditions)

    items_count, page_count, idx_first, idx_last = page_settings(proponentes, page, page_length)
    proponentes = proponentes[idx_first:idx_last]

    proponentes_d = proponentes.to_dict('records')
    for i in range(len(proponentes_d)):
        proponentes_d[i]['CONVENIOS'] = convenios[convenios['IDENTIF_PROPONENTE'] == proponentes_d[i]['IDENTIF_PROPONENTE']].to_dict('records')

    return proponentes_d, pagination(page, page_length, page_count, items_count)

def load_movimento(page=1, page_length=1, parameters={}):

    conditions = filter_constructor(parameters=parameters, dtypes=dtypes_movimento)
    movimento = load_data('movimento', dtypes=dtypes_movimento, parse_dates=parse_dates_movimento)

    if conditions:
        movimento = movimento.query(conditions)

    items_count, page_count, idx_first, idx_last = page_settings(movimento, page, page_length)
    movimento = movimento[idx_first:idx_last]

    movimento_d = movimento.to_dict('records')
 
    return movimento_d, pagination(page, page_length, page_count, items_count)
