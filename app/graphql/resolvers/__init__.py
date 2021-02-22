# coding: utf-8
"""resolvers.
   """

def resolve(load_function, single=False, obj=None, info=None, page_specs=None, filters=None, order_by=None):
    try:

        data, pagination = load_function(page_specs=page_specs, filters=filters, parent=obj, order_by=order_by, use_pagination=False if single else True)
        
        if not single:
             payload = {
                  "pagination": pagination,
                  "items": data
            }
        else:
            payload = data[0]


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