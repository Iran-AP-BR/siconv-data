# coding: utf-8
"""Resolvers.
   """

from .data_loaders.loaders import load_convenios, load_emendas, load_proponentes, load_movimento


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

def resolve_proponentes(obj, info, page, page_length, **kwargs):
    try:
        proponentes, pagination = load_proponentes(page=page, page_length=page_length, parameters=kwargs)
        payload = {
            "pagination": pagination,
            "proponentes": proponentes
        }
    except Exception as error:
        payload = {
            "errors": [str(error)]
        }
    return payload

def resolve_movimento(obj, info, page, page_length, **kwargs):
    try:
        movimento, pagination = load_movimento(page=page, page_length=page_length, parameters=kwargs)
        payload = {
            "pagination": pagination,
            "movimento": movimento
        }
    except Exception as error:
        payload = {
            "errors": [str(error)]
        }
    return payload
