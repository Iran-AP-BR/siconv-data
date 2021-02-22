# coding: utf-8
"""Resolvers.
   """

from .data_loaders.loaders import *


def resolve_convenios(obj, info, page_specs=None, filters=None, order_by=None):
    try:

        convenios, pagination = load_convenios(page_specs=page_specs, parameters=filters, order_by=order_by)
        print(convenios)

        payload = {
            "pagination": pagination,
            "items": convenios
        }

    except Exception as error:

        payload = {
            "errors": [str(error)]
        }

    return payload


def resolve_conv_emendas(obj, info, page_specs=None, filters=None, order_by=None):
    try:
        
        emendas, pagination = load_emendas(
            page_specs=page_specs, parameters=filters, parent=obj, order_by=order_by)

        payload = {
            "pagination": pagination,
            "items": emendas
        }

    except Exception as error:

        payload = {
            "errors": [str(error)]
        }

    return payload


def resolve_conv_proponente(obj, info, page_specs=None, filters=None, order_by=None):
    try:

        proponente, _ = load_proponentes(parameters=filters, parent=obj, use_pagination=False)
        payload = proponente[0]

    except Exception as error:

        payload = {
            "errors": [str(error)]
        }

    return payload

def resolve_conv_movimentos(obj, info, page_specs=None, filters=None, order_by=None):
    try:

        movimento, pagination = load_movimento(page_specs=page_specs, parameters=filters, parent=obj, order_by=order_by)

        payload = {
            "pagination": pagination,
            "items": movimento
        }

    except Exception as error:

        payload = {
            "errors": [str(error)]
        }

    return payload






def resolve_emendas(obj, info, page_specs=None, filters=None, order_by=None):
    try:

        emendas, pagination = load_emendas(page_specs=page_specs, parameters=filters, order_by=order_by)

        payload = {
            "pagination": pagination,
            "items": emendas
        }

    except Exception as error:

        payload = {
            "errors": [str(error)]
        }

    return payload


def resolve_emd_convenios(obj, info, page_specs=None, filters=None, order_by=None):

    try:

        convenios, pagination = load_convenios(page_specs=page_specs, parameters=filters, parent=obj, order_by=order_by)
        payload = {
            "pagination": pagination,
            "items": convenios
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
            "items": proponentes
        }

    except Exception as error:

        payload = {
            "errors": [str(error)]
        }

    return payload


def resolve_prop_convenios(obj, info, page_specs=None, filters=None, order_by=None):

    try:

        convenios, pagination = load_convenios(page_specs=page_specs, parameters=filters, parent=obj, order_by=order_by)

        payload = {
            "pagination": pagination,
            "items": convenios
        }

    except Exception as error:

        payload = {
            "errors": [str(error)]
        }

    return payload

def resolve_prop_municipios(obj, info, page_specs=None, filters=None, order_by=None):
    
    try:

        municipio, _ = load_municipios(parameters=filters, parent=obj, use_pagination=False)
        payload = municipio[0]

    except Exception as error:

        payload = {
            "errors": [str(error)]
        }

    return payload






def resolve_movimentos(obj, info, page_specs=None, filters=None, order_by=None):
    try:

        movimento, pagination = load_movimento(page_specs=page_specs, parameters=filters, order_by=order_by)

        payload = {
            "pagination": pagination,
            "items": movimento
        }

    except Exception as error:

        payload = {
            "errors": [str(error)]
        }

    return payload


def resolve_mov_convenio(obj, info, page_specs=None, filters=None, order_by=None):
    try:

        convenio, _ = load_convenios(parameters=filters, parent=obj, use_pagination=False)
        payload = convenio[0]

    except Exception as error:

        payload = {
            "errors": [str(error)]
        }

    return payload



def resolve_municipios(obj, info, page_specs=None, filters=None, order_by=None):
    try:

        municipios, pagination = load_municipios(
            page_specs=page_specs, parameters=filters, order_by=order_by)

        payload = {
            "pagination": pagination,
            "items": municipios
        }

    except Exception as error:

        payload = {
            "errors": [str(error)]
        }

    return payload

def resolve_mun_proponentes(obj, info, page_specs=None, filters=None, order_by=None):
    try:

        proponentes, pagination = load_proponentes(page_specs=page_specs, parameters=filters, parent=obj, order_by=order_by)

        payload = {
            "pagination": pagination,
            "items": proponentes
        }

    except Exception as error:

        payload = {
            "errors": [str(error)]
        }

    return payload