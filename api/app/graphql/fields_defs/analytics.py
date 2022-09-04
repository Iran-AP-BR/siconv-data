# coding: utf-8
"""analytics.
   """

from app.graphql.data_loaders import *

query = {
            "field": "analytics",
            "type": "Analytics",
            "loader": load_analytics,
            "single": True,
            "children": [
                            {
                                "field": "PAGAMENTO",
                                "loader": lambda **kwargs: load_analytics_metrics(metric='P', **kwargs),
                                "single": True
                                },
                            {
                                "field": "TRIBUTO",
                                "loader": lambda **kwargs: load_analytics_metrics(metric='T',**kwargs),
                                "single": True
                                },
                            {
                                "field": "DESEMBOLSO",
                                "loader": lambda **kwargs: load_analytics_metrics(metric='D',**kwargs),
                                "single": True
                                },
                            {
                                "field": "CONTRAPARTIDA",
                                "loader": lambda **kwargs: load_analytics_metrics(metric='C',**kwargs),
                                "single": True
                                },
                            {
                                "field": "MAIORES_FORNECEDORES",
                                "loader": lambda **kwargs: load_top_fornecedores(top_type='value', **kwargs),
                                "single": False
                                },
                            {
                                "field": "FORNECEDORES_FREQUENTES",
                                "loader": lambda **kwargs: load_top_fornecedores(top_type='quantity', **kwargs),
                                "single": False
                                },
                            {
                                "field": "MELHORES_MEDIAS_FORNECEDORES",
                                "loader": lambda **kwargs: load_top_fornecedores(top_type='average', **kwargs),
                                "single": False
                                },
                            {
                                "field": "MENORES_VARIACOES_FORNECEDORES",
                                "loader": lambda **kwargs: load_top_fornecedores(top_type='deviation', **kwargs),
                                "single": False
                                },
                            ]       
            }