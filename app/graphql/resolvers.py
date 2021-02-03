# coding: utf-8
"""Resolvers.
   """

from .data_loaders.loaders import load_convenios, load_emendas


def resolve_convenios(obj, info, page, page_length, emendas=None, **kwargs):
    try:
        convenios, pagination = load_convenios(page=page, page_length=page_length, _emendas=emendas, parameters=kwargs)
        payload = {
            "pagination": pagination,
            "convenios": convenios
        }
    except Exception as error:
        payload = {
            "errors": [str(error)]
        }
    return payload


def resolve_convenio(obj, info, NR_CONVENIO):
    assert NR_CONVENIO

    try:
        convenio, _ = load_convenios(NR_CONVENIO=NR_CONVENIO)
        payload = {
            "convenio": convenio[0] if len(convenio) > 0 else None
        }

    except Exception as error:
        payload = {
            "errors": [str(error)]
        }

    return payload

def resolve_emendas(obj, info, page, page_length, **kwargs):
    try:
        emendas, pagination = load_emendas(page=page, page_length=page_length, parameters=kwargs)
        payload = {
            "pagination": pagination,
            "emendas": emendas
        }
    except Exception as error:
        payload = {
            "errors": [str(error)]
        }
    return payload


def resolve_emenda(obj, info, NR_EMENDA):
    assert NR_EMENDA

    try:
        emenda, _ = load_emendas(NR_EMENDA=NR_EMENDA)
        payload = {
            "emenda": emenda[0] if len(emenda) > 0 else None
        }

    except Exception as error:
        payload = {
            "errors": [str(error)]
        }

    return payload
