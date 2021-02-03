# coding: utf-8
"""graphql_routes.
   """

from flask import Blueprint
from .query import graphql_playground, graphql_server

graphql_bp = Blueprint('graphql', __name__, url_prefix='/graphql')

def init_routes():
   graphql_bp.add_url_rule('', 'index_get', graphql_playground, methods=["GET"])
   graphql_bp.add_url_rule('', 'index_post', graphql_server, methods=["POST"])
