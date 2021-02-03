# coding: utf-8
"""loaders.
   """

import pandas as pd
from functools import reduce
from . import page_settings, pagination, load_data

def load_convenios(NR_CONVENIO='', page=1, page_length=1, _emendas=None, parameters={}):
    convenios = load_data('convenios')
    emendas_convenios = load_data('emendas_convenios')
    emendas = load_data('emendas')
    
    conditions = []
    if NR_CONVENIO:
        conditions += [(convenios['NR_CONVENIO'].str.lower()==NR_CONVENIO.lower())]
    if parameters.get('SIT_CONVENIO'):
        conditions += [(convenios['SIT_CONVENIO'].str.lower() == parameters.get('SIT_CONVENIO').lower())]   
    if _emendas is not None:
        if _emendas is True:
            conditions += [(convenios['NR_CONVENIO'].isin(emendas_convenios['NR_CONVENIO'].to_list()))]
        elif _emendas is False:
            conditions += [(~convenios['NR_CONVENIO'].isin(emendas_convenios['NR_CONVENIO'].to_list()))]
    
    if conditions:
        q = reduce(lambda x, y: x & y, conditions)
        convenios = convenios[q]

    items_count, page_count, idx_first, idx_last = page_settings(convenios, page, page_length)

    convenios = convenios[idx_first:idx_last]
    convenios_d = convenios.to_dict('records')

    for i in range(len(convenios_d)):
        ec = emendas_convenios[emendas_convenios['NR_CONVENIO'] == convenios_d[i]['NR_CONVENIO']]
        convenios_d[i]['EMENDAS'] = emendas[emendas['NR_EMENDA'].isin(ec['NR_EMENDA'].to_list())].to_dict('records')
    
    return convenios_d, pagination(page, page_length, page_count, items_count)

def load_emendas(NR_EMENDA='', page=1, page_length=1, parameters={}):
    emendas = load_data('emendas')
    emendas_convenios = load_data('emendas_convenios')
    convenios = load_data('convenios')

    conditions = []
    if NR_EMENDA:
        conditions += [(emendas['NR_EMENDA'].str.lower()==NR_EMENDA.lower())]
    if parameters.get('TIPO_PARLAMENTAR'):
        conditions += [(emendas['TIPO_PARLAMENTAR'].str.lower() == parameters.get('TIPO_PARLAMENTAR').lower())]
    if parameters.get('NOME_PARLAMENTAR'):
        conditions += [(emendas['NOME_PARLAMENTAR'].str.lower().str.contains(parameters.get('NOME_PARLAMENTAR').lower()))]
    
    if conditions:
        q = reduce(lambda x, y: x & y, conditions)
        emendas = emendas[q]

    items_count, page_count, idx_first, idx_last = page_settings(emendas, page, page_length)
    emendas = emendas[idx_first:idx_last]

    emendas_d = emendas.to_dict('records')
    for i in range(len(emendas_d)):
        ec = emendas_convenios[emendas_convenios['NR_EMENDA'] == emendas_d[i]['NR_EMENDA']]
        emendas_d[i]['CONVENIOS'] = convenios[convenios['NR_CONVENIO'].isin(ec['NR_CONVENIO'].to_list())].to_dict('records')

    return emendas_d, pagination(page, page_length, page_count, items_count)
