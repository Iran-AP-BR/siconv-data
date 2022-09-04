# coding: utf-8
"""buscarFornecedores.
   """

from app.graphql.data_loaders import *


query = {
            "field": "buscarFornecedores",
            "type": "Fornecedor",
            "loader": load_fornecedores,
            "single": False,
            "children": [
                            {
                                "field": "CONVENIOS",
                                "loader": load_convenios,
                                "single": False
                                },
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
                                "field": "MUNICIPIOS",
                                "loader": load_municipios,
                                "single": False
                                },
                            {
                                "field": "ESTADOS",
                                "loader": load_estados,
                                "single": False
                                },
                            {
                                "field": "PARLAMENTARES",
                                "loader": load_parlamentares,
                                "single": False
                                },
                            {
                                "field": "ANALYTICS",
                                "loader": load_analytics,
                                "single": True
                                },
                        ]      
        }
