# coding: utf-8
"""Resolvers Movimento.
   """

from app.graphql.data_loaders.loaders import *
from app.graphql.resolvers import resolve

def resolve_movimento(obj, info, **kwargs):
    return resolve(load_movimento, obj=obj, info=info,  **kwargs)

def resolve_mov_convenio(obj, info, **kwargs):
    return resolve(load_convenios, single=True, obj=obj, info=info, **kwargs)

def resolve_mov_fornecedor(obj, info, **kwargs):
    return resolve(load_fornecedores, single=True, obj=obj, info=info, **kwargs)