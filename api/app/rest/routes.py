# coding: utf-8
"""rest_routes.
   """

from .resources import data_atual, files_parquet, swagger
from . import blueprint


def init_routes(config):
   blueprint.add_url_rule('/', 'swagger', swagger, methods=['GET'])
   blueprint.add_url_rule('/data_atual', 'data_atual', data_atual, methods=['GET'])
   blueprint.add_url_rule(f'/{config["DATA_ENDPOINT"]}/<fileType>/<tableName>', 
                            config["DATA_ENDPOINT"], files_parquet, methods=['GET'])

