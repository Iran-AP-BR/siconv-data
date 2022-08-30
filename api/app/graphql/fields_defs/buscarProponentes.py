# coding: utf-8
"""buscarProponentes.
   """

from app.graphql.data_loaders.loaders import *



query = {
            "field": "buscarProponentes",
            "type": "Proponente",
            "loader": load_proponentes,
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
                            ]         
        }
    
