# coding: utf-8
"""Resolvers Atributos.
   """

from app.graphql.data_loaders.loaders import *
from app.graphql.resolvers import resolve

def resolve_atributos(obj, info):
    return resolve(load_atributos, single=True, obj=obj, info=info)
