# coding: utf-8
"""buscarMovimento.
   """

from app.graphql.data_loaders.loaders import *

query = {
            "field": "buscarMovimento",
            "type": "Movimento",
            "loader": load_movimento,
            "single": False,
            "children": [
                            {
                                "field": "CONVENIO",
                                "loader": load_convenios,
                                "single": True
                                },
                            {
                                "field": "FORNECEDOR",
                                "loader": load_fornecedores,
                                "single": True
                                },
                            ]       
            }
