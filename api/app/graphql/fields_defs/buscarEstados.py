# coding: utf-8
"""buscarEstados.
   """

from app.graphql.data_loaders import *

query = {
            "field": "buscarEstados",
            "type": "Estado",
            "loader": load_estados,
            "single": False,
            "children": [
                            {
                                "field": "PROPONENTES",
                                "loader": load_proponentes,
                                "single": False
                                },
                            {
                                "field": "FORNECEDORES",
                                "loader": load_fornecedores,
                                "single": False
                                },
                            {
                                "field": "MUNICIPIOS",
                                "loader": load_municipios,
                                "single": False
                                },
                            {
                                "field": "CONVENIOS",
                                "loader": load_convenios,
                                "single": False
                                },
                            {
                                "field": "ANALYTICS",
                                "loader": load_analytics,
                                "single": True
                                },
                        ]      
        }
