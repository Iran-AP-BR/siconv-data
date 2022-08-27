# coding: utf-8
"""Resolvers Parlamentares.
   """

from app.graphql.data_loaders.loaders import *
from app.graphql.resolvers import resolve

def resolve_parlamentares(obj, info, **kwargs):
    return resolve(load_parlamentares, obj=obj, info=info, **kwargs)

def resolve_par_convenios(obj, info, **kwargs):
    return resolve(load_convenios, obj=obj, info=info, **kwargs)

def resolve_par_fornecedores(obj, info, **kwargs):
    return resolve(load_fornecedores, obj=obj, info=info, **kwargs)

def resolve_par_licitacoes(obj, info, **kwargs):
    return resolve(load_licitacoes, obj=obj, info=info, **kwargs)

def resolve_par_emendas(obj, info, **kwargs):
    return resolve(load_emendas, obj=obj, info=info, **kwargs)

def resolve_par_proponentes(obj, info, **kwargs):
    return resolve(load_proponentes, obj=obj, info=info, **kwargs)

def resolve_par_movimento(obj, info, **kwargs):
    return resolve(load_movimento, obj=obj, info=info, **kwargs)
