# coding: utf-8
"""loaders.
   """

import pandas as pd
from . import page_settings, pagination, load_data
from .dtypes import *
from flask import current_app as app
from app.graphql.data_loaders.filtering import filter_constructor

def __load_emendas_convenios__(parameters={}):

    conditions = filter_constructor(parameters=parameters, dtypes=dtypes_emendas_convenios)
    emendas_convenios = load_data('emendas_convenios', dtypes=dtypes_emendas_convenios)
    
    if conditions:
        emendas_convenios = emendas_convenios.query(conditions)

    emendas_convenios_d = emendas_convenios.to_dict('records')
 
    return emendas_convenios_d


def __load_convenios__(page_specs=None, use_pagination=True, parameters={}, order_by=None):
    conditions = filter_constructor(parameters=parameters, dtypes=dtypes_convenios, parse_dates=parse_dates_convenios)
    
    convenios = load_data('convenios', dtypes=dtypes_convenios, parse_dates=parse_dates_convenios)

    if conditions:
        convenios = convenios.query(conditions)

    if order_by:
        by = order_by.get('field')
        ascending = order_by.get('ascending')
        convenios = convenios.sort_values(by=by, ascending=ascending)

    if use_pagination:
        items_count, page_count, idx_first, idx_last, page_specs = page_settings(convenios, page_specs)
        convenios = convenios[idx_first:idx_last]

    convenios_d = convenios.to_dict('records')

    return convenios_d, pagination(page_specs, page_count, items_count) if page_specs else None


def __load_emendas__(page_specs=None, use_pagination=True, parameters={}, order_by=None):

    conditions = filter_constructor(parameters=parameters, dtypes=dtypes_emendas)
    emendas = load_data('emendas', dtypes=dtypes_emendas)

    if conditions:
        emendas = emendas.query(conditions)
    
    if order_by:
        by = order_by.get('field')
        ascending = order_by.get('ascending')
        emendas = emendas.sort_values(by=by, ascending=ascending)

    if use_pagination:
        items_count, page_count, idx_first, idx_last, page_specs = page_settings(emendas, page_specs)
        emendas = emendas[idx_first:idx_last]

    emendas_d = emendas.to_dict('records')
    
    return emendas_d, pagination(page_specs, page_count, items_count) if page_specs else None

def __load_movimento__(page_specs=None, use_pagination=True, parameters={}, order_by=None):

    conditions = filter_constructor(parameters=parameters, dtypes=dtypes_movimento, parse_dates=parse_dates_movimento)
    movimento = load_data('movimento', dtypes=dtypes_movimento, parse_dates=parse_dates_movimento)

    if conditions:
        movimento = movimento.query(conditions)

    if order_by:
        by = order_by.get('field')
        ascending = order_by.get('ascending')
        movimento = movimento.sort_values(by=by, ascending=ascending)

    if use_pagination:
        items_count, page_count, idx_first, idx_last, page_specs = page_settings(movimento, page_specs)
        movimento = movimento[idx_first:idx_last]

    movimento_d = movimento.to_dict('records')
 
    return movimento_d, pagination(page_specs, page_count, items_count) if page_specs else None


##############################################

def load_convenios(page_specs=None, use_pagination=True, parameters=None, obj=None, order_by=None):
    if obj:
        if obj.get('IDENTIF_PROPONENTE'):
            convenios, pagination = __load_convenios__(page_specs=page_specs, 
                                  parameters={'IDENTIF_PROPONENTE': obj.get('IDENTIF_PROPONENTE')}, order_by=order_by)
        else:
            ec_d = __load_emendas_convenios__(parameters={'NR_EMENDA': obj.get('NR_EMENDA')})
            p = 'in$' + '|'.join([em['NR_CONVENIO'] for em in ec_d])
            convenios, pagination = __load_convenios__(page_specs=page_specs, parameters={'NR_CONVENIO': p}, order_by=order_by)
    else:
        convenios, pagination = __load_convenios__(page_specs=page_specs, parameters=parameters, order_by=order_by)
    
    return convenios, pagination

def load_emendas(page_specs=None, use_pagination=True, parameters=None, obj=None, order_by=None):
    if obj:
        emendas_convenios_didct = __load_emendas_convenios__(parameters={'NR_CONVENIO': obj.get('NR_CONVENIO')})
        emendas, pagination = __load_emendas__(page_specs=page_specs, 
                                               parameters={'NR_EMENDA': 'in$' + '|'.join([emc['NR_EMENDA']
                                                for emc in emendas_convenios_didct])}, order_by=order_by)
    else:
        emendas, pagination = __load_emendas__(page_specs=page_specs, parameters=parameters)
    
    return emendas, pagination

def load_proponentes(page_specs=None, use_pagination=True, parameters={}, order_by=None):
    conditions = filter_constructor(parameters=parameters, dtypes=dtypes_proponentes)
    proponentes = load_data('proponentes', dtypes=dtypes_proponentes)

    if conditions:
        proponentes = proponentes.query(conditions)

    if order_by:
        by = order_by.get('field')
        ascending = order_by.get('ascending')
        proponentes = proponentes.sort_values(by=by, ascending=ascending)

    if use_pagination:
        items_count, page_count, idx_first, idx_last, page_specs = page_settings(proponentes, page_specs)
        proponentes = proponentes[idx_first:idx_last]

    proponentes_d = proponentes.to_dict('records')

    return proponentes_d, pagination(page_specs, page_count, items_count) if page_specs else None


def load_movimento(page_specs=None, use_pagination=True, parameters=None, obj=None, order_by=None):
    if obj:
        movimento, pagination = __load_movimento__(page_specs=page_specs, parameters={'NR_CONVENIO': obj['NR_CONVENIO']}, order_by=order_by)
    else:
        movimento, pagination = __load_movimento__(page_specs=page_specs, parameters=parameters, order_by=order_by)

    return movimento, pagination
