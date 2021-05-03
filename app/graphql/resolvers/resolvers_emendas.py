# coding: utf-8
"""Resolvers Emendas.
   """

from app.graphql.data_loaders.loaders import *
from app.graphql.resolvers import resolve

def resolve_emendas(obj, info, **kwargs):
    return resolve(load_emendas, obj=obj, info=info, **kwargs)

def resolve_emd_convenios(obj, info, **kwargs):
    return resolve(load_convenios, obj=obj, info=info, **kwargs)

def resolve_emd_fornecedores(obj, info, **kwargs):
    return resolve(load_fornecedores, obj=obj, info=info, **kwargs)
