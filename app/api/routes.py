# coding: utf-8
"""Resources.
   """

from flask import current_app as app
from app.api.resources import data_atual, files, municipios, swagger


def init_routes(app):
   app.add_url_rule('/', 'index', swagger, methods=['GET'])
   app.add_url_rule('/data_atual', 'data_atual', data_atual, methods=['GET'])
   app.add_url_rule('/municipios', 'municipios', municipios, methods=['GET'])
   app.add_url_rule(f'/{app.config["DATA_ENDPOINT"]}/<tableName>', app.config["DATA_ENDPOINT"], files, methods=['GET'])

   return True