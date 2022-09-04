# coding: utf-8
"""buscarAtributos.
   """

from app.graphql.data_loaders import *

query = {
            "field": "buscarAtributos",
            "type": "Atributos",
            "loader": load_atributos,
            "single": True,
            "children": None
            }
