# coding: utf-8
"""Resources.
   """

from flask import current_app as app
from app.api.rest.resources import data_atual, files, municipios, swagger
from app.api.graphql.graphql import graphql_playground, graphql_server


def init_routes(app):
   app.add_url_rule('/', 'index', swagger, methods=['GET'])
   app.add_url_rule('/data_atual', 'data_atual', data_atual, methods=['GET'])
   app.add_url_rule('/municipios', 'municipios', municipios, methods=['GET'])
   app.add_url_rule(f'/{app.config["DATA_ENDPOINT"]}/<tableName>', app.config["DATA_ENDPOINT"], files, methods=['GET'])
   app.add_url_rule('/graphql', view_func=graphql_playground, methods=["GET"])
   app.add_url_rule('/graphql', view_func=graphql_server, methods=["POST"])

   return True