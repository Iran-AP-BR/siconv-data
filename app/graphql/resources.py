# coding: utf-8
"""Resolvers.
   """

from flask import jsonify, request
from ariadne import load_schema_from_path, make_executable_schema, graphql_sync, ObjectType
from ariadne.constants import PLAYGROUND_HTML
from app.graphql import schema

from app.security import api_key_required
import os


def graphql_playground():
    return PLAYGROUND_HTML, 200


@api_key_required
def graphql_server():
    data = request.get_json()

    success, result = graphql_sync(
        schema,
        data,
        context_value=request,
        # debug=app.debug
    )

    status_code = 200 if success else 400
    return jsonify(result), status_code
