# coding: utf-8
"""loaders.
   """

import pandas as pd
from . import page_settings, pagination, load_data
from .filtering import filter_constructor

def load_convenios(page=1, page_length=1, _emendas=None, parameters={}):
    dtypes = {'NR_CONVENIO': 'object',
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
            'OBJETO_PROPOSTA': 'object'}

    parse_dates = ['DIA_ASSIN_CONV', 'DIA_PUBL_CONV', 'DIA_INIC_VIGENC_CONV', 'DIA_FIM_VIGENC_CONV', 'DIA_LIMITE_PREST_CONTAS']
    
    convenios = load_data('convenios', dtypes=dtypes, parse_dates=parse_dates)
    emendas_convenios = load_data('emendas_convenios')
    emendas = load_data('emendas')
    
    q = []
    if _emendas is not None:
        if _emendas is True:
            q += [(convenios['NR_CONVENIO'].isin(emendas_convenios['NR_CONVENIO'].to_list()))]
        elif _emendas is False:
            q += [(~convenios['NR_CONVENIO'].isin(emendas_convenios['NR_CONVENIO'].to_list()))]
        
    convenios = filter_constructor(parameters=parameters, data_frame=convenios, dtypes=dtypes, parse_dates=parse_dates, initial_conditions=q)

    items_count, page_count, idx_first, idx_last = page_settings(convenios, page, page_length)

    convenios = convenios[idx_first:idx_last]
    convenios_d = convenios.to_dict('records')

    for i in range(len(convenios_d)):
        ec = emendas_convenios[emendas_convenios['NR_CONVENIO'] == convenios_d[i]['NR_CONVENIO']]
        convenios_d[i]['EMENDAS'] = emendas[emendas['NR_EMENDA'].isin(ec['NR_EMENDA'].to_list())].to_dict('records')
    
    return convenios_d, pagination(page, page_length, page_count, items_count)


def load_emendas(page=1, page_length=1, parameters={}):
    emendas = load_data('emendas')
    emendas_convenios = load_data('emendas_convenios')
    convenios = load_data('convenios')

    emendas = filter_constructor(parameters=parameters, data_frame=emendas)

    items_count, page_count, idx_first, idx_last = page_settings(emendas, page, page_length)
    emendas = emendas[idx_first:idx_last]

    emendas_d = emendas.to_dict('records')
    for i in range(len(emendas_d)):
        ec = emendas_convenios[emendas_convenios['NR_EMENDA'] == emendas_d[i]['NR_EMENDA']]
        emendas_d[i]['CONVENIOS'] = convenios[convenios['NR_CONVENIO'].isin(ec['NR_CONVENIO'].to_list())].to_dict('records')

    return emendas_d, pagination(page, page_length, page_count, items_count)
