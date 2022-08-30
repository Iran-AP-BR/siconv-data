# coding: utf-8
"""buscarParlamentares.
   """

from app.graphql.data_loaders.loaders import *

query = {
            "field": "buscarParlamentares",
            "type": "Parlamentar",
            "loader": load_parlamentares,
            "single": False,
            "children": [
                            {
                                "field": "PROPONENTES",
                                "loader": load_proponentes,
                                "single": False
                                },
                            {
                                "field": "EMENDAS",
                                "loader": load_emendas,
                                "single": False
                                },
                            {
                                "field": "MOVIMENTO",
                                "loader": load_movimento,
                                "single": False
                                },
                            {
                                "field": "FORNECEDORES",
                                "loader": load_fornecedores,
                                "single": False
                                },
                            {
                                "field": "LICITACOES",
                                "loader": load_licitacoes,
                                "single": False
                                },
                            {
                                "field": "CONVENIOS",
                                "loader": load_convenios,
                                "single": False
                                },
                        ]      
        }
