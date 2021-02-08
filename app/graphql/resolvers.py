# coding: utf-8
"""Resolvers.
   """

from .data_loaders.loaders import load_convenios, load_emendas, load_proponentes, load_movimento


def resolve_convenios(obj, info, page_specs, emendas=None, **kwargs):
    try:
        convenios, pagination = load_convenios(page_specs=page_specs, _emendas=emendas, parameters=kwargs)
        if kwargs:
            convenios, pagination = load_convenios(page_specs=page_specs, _emendas=emendas, parameters=kwargs)
        elif obj and obj.get('IDENTIF_PROPONENTE'):
            convenios, pagination = load_convenios(page_specs=page_specs, parameters={'IDENTIF_PROPONENTE': obj.get('IDENTIF_PROPONENTE')})
        elif obj:
            convenios, pagination = load_convenios(page_specs=page_specs, parameters={'NR_EMENDA': obj.get('NR_EMENDA')})

        payload = {
            "pagination": pagination,
            "convenios": convenios
        }
    except Exception as error:
        payload = {
            "errors": [str(error)]
        }
    return payload

def resolve_convenio(obj, *_):
    try:
        convenio, _ = load_convenios(parameters={'NR_CONVENIO': obj['NR_CONVENIO']})
        payload = convenio[0]
    except Exception as error:
        payload = {
            "errors": [str(error)]
        }
    
    return payload

def resolve_emendas(obj, info, page_specs, **kwargs):
    try:
        if kwargs:
            emendas, pagination = load_emendas(page_specs=page_specs, parameters=kwargs)
        else:
            emendas, pagination = load_emendas(page_specs=page_specs, parameters={'NR_CONVENIO': obj['NR_CONVENIO']})

        payload = {
            "pagination": pagination,
            "emendas": emendas
        }
    except Exception as error:
        payload = {
            "errors": [str(error)]
        }
    return payload


def resolve_proponentes(obj, info, page_specs, **kwargs):
    try:
        proponentes, pagination = load_proponentes(page_specs=page_specs, parameters=kwargs)
        payload = {
            "pagination": pagination,
            "proponentes": proponentes
        }
    except Exception as error:
        payload = {
            "errors": [str(error)]
        }
    return payload

def resolve_proponente(obj, *_):
    try:
        proponente, _ = load_proponentes(parameters={'IDENTIF_PROPONENTE': obj['IDENTIF_PROPONENTE']})
        payload = proponente[0]
    except Exception as error:
        payload = {
            "errors": [str(error)]
        }
    
    return payload

def resolve_movimentos(obj, info, page_specs, **kwargs):
    try:
        if kwargs:
            movimento, pagination = load_movimento(page_specs=page_specs, parameters=kwargs)
        else:
            movimento, pagination = load_movimento(page_specs=page_specs, parameters={'NR_CONVENIO': obj['NR_CONVENIO']})

        payload = {
            "pagination": pagination,
            "movimento": movimento
        }
    except Exception as error:
        payload = {
            "errors": [str(error)]
        }
    return payload

