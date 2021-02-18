# coding: utf-8
"""Resolvers.
   """

from .data_loaders.loaders import *


def resolve_convenios(obj, info, page_specs=None, filters=None, order_by=None):

    try:

        if obj:
            if obj.get('IDENTIF_PROPONENTE'):
                convenios, pagination = load_convenios(page_specs=page_specs,  parameters={
                                                       'IDENTIF_PROPONENTE': obj.get('IDENTIF_PROPONENTE')}, order_by=order_by)
            elif obj.get('NR_EMENDA'):
                convenios, pagination = load_convenios(
                    page_specs=page_specs, emenda=obj.get('NR_EMENDA'), order_by=order_by)
            else:

                raise Exception(f'{__name__}: Campo inesperado')
        else:
            convenios, pagination = load_convenios(
                page_specs=page_specs, parameters=filters, order_by=order_by)

        payload = {
            "pagination": pagination,
            "items": convenios
        }

    except Exception as error:

        payload = {
            "errors": [str(error)]
        }

    return payload


def resolve_convenio(obj, *_):
    try:

        convenio, _ = load_convenios(
            parameters={'NR_CONVENIO': obj['NR_CONVENIO']}, use_pagination=False)
        payload = convenio[0]

    except Exception as error:

        payload = {
            "errors": [str(error)]
        }

    return payload


def resolve_emendas(obj, info, page_specs=None, filters=None, order_by=None):
    try:

        if obj:
            emendas, pagination = load_emendas(
                page_specs=page_specs, convenio=obj.get('NR_CONVENIO'), order_by=order_by)
        else:
            emendas, pagination = load_emendas(
                page_specs=page_specs, parameters=filters, order_by=order_by)

        payload = {
            "pagination": pagination,
            "items": emendas
        }

    except Exception as error:

        payload = {
            "errors": [str(error)]
        }

    return payload


def resolve_proponentes(obj, info, page_specs=None, filters=None, order_by=None):
    try:

        proponentes, pagination = load_proponentes(
            page_specs=page_specs, parameters=filters, order_by=order_by)

        payload = {
            "pagination": pagination,
            "items": proponentes
        }

    except Exception as error:

        payload = {
            "errors": [str(error)]
        }

    return payload


def resolve_proponente(obj, *_):
    try:

        proponente, _ = load_proponentes(
            parameters={'IDENTIF_PROPONENTE': obj['IDENTIF_PROPONENTE']}, use_pagination=False)
        payload = proponente[0]

    except Exception as error:

        payload = {
            "errors": [str(error)]
        }

    return payload


def resolve_movimentos(obj, info, page_specs=None, filters=None, order_by=None):
    try:

        if obj:
            movimento, pagination = load_movimento(page_specs=page_specs, convenio=obj['NR_CONVENIO'], order_by=order_by)
        else:
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


def resolve_municipio(obj, *_):
    try:

        municipio, _ = load_municipios(parameters={'codigo_ibge': obj['COD_MUNIC_IBGE']}, use_pagination=False)
        payload = municipio[0]

    except Exception as error:

        payload = {
            "errors": [str(error)]
        }

    return payload
