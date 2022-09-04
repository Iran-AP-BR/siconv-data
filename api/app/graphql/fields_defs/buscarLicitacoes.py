# coding: utf-8
"""buscarLicitacoes.
   """

from app.graphql.data_loaders import *

query = {
            "field": "buscarLicitacoes",
            "type": "Licitacao",
            "loader": load_licitacoes,
            "single": False,
            "children": [
                            {
                                "field": "CONVENIO",
                                "loader": load_convenios,
                                "single": True
                                },
                            {
                                "field": "PROPONENTE",
                                "loader": load_proponentes,
                                "single": True
                                },
                        ]      
        }
