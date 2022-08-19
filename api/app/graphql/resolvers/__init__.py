# coding: utf-8
"""resolvers.
   """

def resolve(load_function, single=False, obj=None, info=None, page_specs=None, filters=None, sort=None):
    try:

        data, pagination = load_function(parent=obj, page_specs=page_specs, filters=filters, 
                                         sort=sort, use_pagination=False if single else True)
        
        if not single:
             payload = {
                  "pagination": pagination,
                  "items": data
            }
        else:
            payload = data[0] if type(data) is list else data


    except Exception as error:

        payload = {
            "errors": [str(error)]
        }

    return payload


from .resolvers_convenios import *
from .resolvers_emendas import *
from .resolvers_proponentes import *
from .resolvers_movimento import *
from .resolvers_municipios import *
from .resolvers_atributos import *
from .resolvers_fornecedores import *
from .resolvers_estados import *
from .resolvers_licitacoes import *