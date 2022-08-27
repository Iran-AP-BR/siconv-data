# coding: utf-8
"""Resolvers Proponentes.
   """

from app.graphql.data_loaders.loaders import *
from app.graphql.resolvers import resolve

def resolve_proponentes(obj, info, **kwargs):
    return resolve(load_proponentes, obj=obj, info=info, **kwargs)

def resolve_prop_convenios(obj, info, **kwargs):
    return resolve(load_convenios, obj=obj, info=info, **kwargs)

def resolve_prop_municipio(obj, info, **kwargs):
    return resolve(load_municipios, single=True, obj=obj, info=info, **kwargs)

def resolve_prop_estado(obj, info, **kwargs):
    return resolve(load_estados, single=True, obj=obj, info=info, **kwargs)

def resolve_prop_fornecedores(obj, info, **kwargs):
    return resolve(load_fornecedores, obj=obj, info=info, **kwargs)

def resolve_prop_licitacoes(obj, info, **kwargs):
    return resolve(load_licitacoes, obj=obj, info=info, **kwargs)
