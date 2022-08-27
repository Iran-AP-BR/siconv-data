# coding: utf-8
"""Resolvers Estados.
   """

from app.graphql.data_loaders.loaders import *
from app.graphql.resolvers import resolve

def resolve_estados(obj, info, **kwargs):
    return resolve(load_estados, obj=obj, info=info, **kwargs)

def resolve_est_municipios(obj, info, **kwargs):
    return resolve(load_municipios, obj=obj, info=info, **kwargs)

def resolve_est_convenios(obj, info, **kwargs):
    return resolve(load_convenios, obj=obj, info=info, **kwargs)

def resolve_est_proponentes(obj, info, **kwargs):
    return resolve(load_proponentes, obj=obj, info=info, **kwargs)

def resolve_est_fornecedores(obj, info, **kwargs):
    return resolve(load_fornecedores, obj=obj, info=info, **kwargs)
