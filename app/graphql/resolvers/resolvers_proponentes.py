# coding: utf-8
"""Resolvers Proponentes.
   """

from app.graphql.data_loaders.loaders import *
from app.graphql.resolvers import resolve

def resolve_proponentes(obj, info, **kwargs):
    return resolve(load_proponentes, obj=obj, info=info, **kwargs)

def resolve_prop_convenios(obj, info, **kwargs):
    return resolve(load_convenios, **kwargs)

def resolve_prop_municipios(obj, info, **kwargs):
    return resolve(load_municipios, single=True, obj=obj, info=info, **kwargs)
