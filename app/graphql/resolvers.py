# coding: utf-8
"""Resolvers.
   """

from .data_loaders.loaders import load_convenios, load_emendas, load_proponentes, load_movimento


def resolve_convenios(obj, info, page_specs=None, filters=None, order_by=None):
    try:

        convenios, pagination = load_convenios(page_specs=page_specs, parameters=filters, obj=obj, order_by=order_by)

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

        convenio, _ = load_convenios(parameters={'NR_CONVENIO': obj['NR_CONVENIO']}, use_pagination=False)
        payload = convenio[0]

    except Exception as error:

        payload = {
            "errors": [str(error)]
        }
    
    return payload

def resolve_emendas(obj, info, page_specs=None, filters=None, order_by=None):
    try:

        emendas, pagination = load_emendas(page_specs=page_specs, parameters=filters, obj=obj, order_by=order_by)

        payload = {
            "pagination": pagination,
            "emendas": emendas
        }

    except Exception as error:

        payload = {
            "errors": [str(error)]
        }

    return payload

def resolve_proponentes(obj, info, page_specs=None, filters=None, order_by=None):
    try:

        proponentes, pagination = load_proponentes(page_specs=page_specs, parameters=filters, order_by=order_by)
       
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

        proponente, _ = load_proponentes(parameters={'IDENTIF_PROPONENTE': obj['IDENTIF_PROPONENTE']}, use_pagination=False)
        payload = proponente[0]

    except Exception as error:

        payload = {
            "errors": [str(error)]
        }
    
    return payload

def resolve_movimentos(obj, info, page_specs=None, filters=None, order_by=None):
    try:

        movimento, pagination = load_movimento(page_specs=page_specs, parameters=filters, obj=obj, order_by=order_by)

        payload = {
            "pagination": pagination,
            "movimento": movimento
        }

    except Exception as error:

        payload = {
            "errors": [str(error)]
        }

    return payload

