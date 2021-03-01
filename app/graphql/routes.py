# coding: utf-8
"""graphql_routes.
   """

from .resources import graphql_playground, graphql_server, load_all
from . import blueprint


def init_routes():
    blueprint.add_url_rule('/', 'playground', graphql_playground, methods=["GET"])
    blueprint.add_url_rule('/', 'index', graphql_server, methods=["POST"])
    blueprint.add_url_rule('/load_all', 'load_all',load_all, methods=["GET"])
