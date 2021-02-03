# coding: utf-8
"""GraphQL.
   """
from flask import Blueprint

blueprint = Blueprint('graphql', __name__, url_prefix='/graphql')

from app.graphql.routes import init_routes