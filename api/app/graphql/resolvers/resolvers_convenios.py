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

def resolve_conv_fornecedores(obj, info, **kwargs):
    return resolve(load_fornecedores, obj=obj, info=info, **kwargs)

def resolve_conv_municipio(obj, info, **kwargs):
    return resolve(load_municipios, single=True, obj=obj, info=info, **kwargs)

def resolve_conv_estado(obj, info, **kwargs):
    return resolve(load_estados, single=True, obj=obj, info=info, **kwargs)

def resolve_conv_licitacoes(obj, info, **kwargs):
    return resolve(load_licitacoes, obj=obj, info=info, **kwargs)

def resolve_conv_parlamentares(obj, info, **kwargs):
    return resolve(load_parlamentares, obj=obj, info=info, **kwargs)