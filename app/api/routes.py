# coding: utf-8
"""Resources.
   """

from app.api.resources import data_atual, files, municipios, swagger
from app.config import DATA_ENDPOINT

def init_routes(app):
   app.add_url_rule('/', 'index', swagger)
   #app.add_url_rule('/docs', 'docs', swagger)
   app.add_url_rule('/data_atual', 'data_atual', data_atual)
   app.add_url_rule('/municipios', 'municipios', municipios)
   app.add_url_rule(f'/{DATA_ENDPOINT}/<tableName>', DATA_ENDPOINT, files)

   return True