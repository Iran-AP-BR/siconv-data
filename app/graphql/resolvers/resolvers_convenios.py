# coding: utf-8
"""Resolvers Convenios.
   """

from app.graphql.data_loaders.loaders import *
from app.graphql.resolvers import resolve

def resolve_convenios(obj, info, **kwargs):
    return resolve(load_convenios, obj=obj, info=info, **kwargs)

def resolve_conv_emendas(obj, info, **kwargs):
    return resolve(load_emendas, obj=obj, info=info, **kwargs)

def resolve_conv_proponente(obj, info, **kwargs):
    return resolve(load_proponentes, single=True, obj=obj, info=info, **kwargs)

def resolve_conv_movimento(obj, info, **kwargs):
    return resolve(load_movimento, obj=obj, info=info, **kwargs)
