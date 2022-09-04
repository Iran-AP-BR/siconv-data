# coding: utf-8
"""buscarConvenios.
   """

from app.graphql.data_loaders import *

query = {
            "field": "buscarConvenios",
            "type": "Convenio",
            "loader": load_convenios,
            "single": False,
            "children": [
                            {
                                "field": "PROPONENTE",
                                "loader": load_proponentes,
                                "single": True
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
                                "field": "MUNICIPIO",
                                "loader": load_municipios,
                                "single": True
                                },
                            {
                                "field": "ESTADO",
                                "loader": load_estados,
                                "single": True
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
