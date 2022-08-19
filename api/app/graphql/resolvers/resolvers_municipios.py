# coding: utf-8
"""Resolvers Municipios.
   """

from app.graphql.data_loaders.loaders import *
from app.graphql.resolvers import resolve

def resolve_municipios(obj, info, **kwargs):
    return resolve(load_municipios, obj=obj, info=info, **kwargs)

def resolve_mun_estado(obj, info, **kwargs):
    return resolve(load_estados, single=True, obj=obj, info=info, **kwargs)

def resolve_mun_convenios(obj, info, **kwargs):
    return resolve(load_convenios, obj=obj, info=info, **kwargs)

def resolve_mun_proponentes(obj, info, **kwargs):
    return resolve(load_proponentes, obj=obj, info=info, **kwargs)

def resolve_mun_fornecedores(obj, info, **kwargs):
    return resolve(load_fornecedores, obj=obj, info=info, **kwargs)
