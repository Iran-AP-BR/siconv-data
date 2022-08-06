# coding: utf-8
"""Resolvers Movimento.
   """

from app.graphql.data_loaders.loaders import *
from app.graphql.resolvers import resolve

def resolve_fornecedores(obj, info, **kwargs):
    return resolve(load_fornecedores, obj=obj, info=info,  **kwargs)

def resolve_forn_convenios(obj, info, **kwargs):
    return resolve(load_convenios, obj=obj, info=info, **kwargs)

def resolve_forn_movimento(obj, info, **kwargs):
    return resolve(load_movimento, obj=obj, info=info, **kwargs)

def resolve_forn_estados(obj, info, **kwargs):
    return resolve(load_estados, obj=obj, info=info, **kwargs)

def resolve_forn_municipios(obj, info, **kwargs):
    return resolve(load_municipios, obj=obj, info=info, **kwargs)

def resolve_forn_emendas(obj, info, **kwargs):
    return resolve(load_emendas, obj=obj, info=info, **kwargs)

def resolve_forn_summary(obj, info, **kwargs):
    return resolve(load_fornecedores_summary, single=True, obj=obj, info=info, **kwargs)
