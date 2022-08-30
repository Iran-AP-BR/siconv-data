# coding: utf-8
"""buscarMunicipios.
   """

from app.graphql.data_loaders.loaders import *

query = {
            "field": "buscarMunicipios",
            "type": "Municipio",
            "loader": load_municipios,
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
                                "field": "ESTADO",
                                "loader": load_estados,
                                "single": True
                                },
                            {
                                "field": "CONVENIOS",
                                "loader": load_convenios,
                                "single": False
                                },
                        ]      
        }
