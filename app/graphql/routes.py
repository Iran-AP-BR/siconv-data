# coding: utf-8
"""graphql_routes.
   """

from .query import graphql_playground, graphql_server
from . import blueprint


def init_routes():
   blueprint.add_url_rule('/', 'playground', graphql_playground, methods=["GET"])
   blueprint.add_url_rule('/', 'index', graphql_server, methods=["POST"])
