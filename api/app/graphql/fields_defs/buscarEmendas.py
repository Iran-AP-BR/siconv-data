# coding: utf-8
"""buscarEmendas.
   """

from app.graphql.data_loaders import *

query = {
            "field": "buscarEmendas",
            "type": "Emenda",
            "loader": load_emendas,
            "single": False,
            "children": [
                            {
                                "field": "CONVENIOS",
                                "loader": load_convenios,
                                "single": False
                                },
                            {
                                "field": "FORNECEDORES",
                                "loader": load_fornecedores,
                                "single": False
                                },
                            {
                                "field": "ANALYTICS",
                                "loader": load_analytics,
                                "single": True
                                },
                            ]       
            }
