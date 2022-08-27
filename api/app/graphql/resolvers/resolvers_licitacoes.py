# coding: utf-8
"""Resolvers Licitacoes.
   """

from app.graphql.data_loaders.loaders import *
from app.graphql.resolvers import resolve

def resolve_licitacoes(obj, info, **kwargs):
    return resolve(load_licitacoes, obj=obj, info=info,  **kwargs)

def resolve_lic_convenio(obj, info, **kwargs):
    return resolve(load_convenios, single=True, obj=obj, info=info, **kwargs)

def resolve_lic_proponente(obj, info, **kwargs):
    return resolve(load_proponentes, single=True, obj=obj, info=info, **kwargs)
