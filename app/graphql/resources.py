# coding: utf-8
"""Resolvers.
   """

from flask import jsonify, request
from ariadne import load_schema_from_path, make_executable_schema, graphql_sync, ObjectType
from ariadne.constants import PLAYGROUND_HTML
from app.graphql import schema

from app.security import api_key_required
import os

from app.graphql.data_loaders.loaders import (DataLoader, convenios_settings, emendas_settings, movimento_settings, municipios_settings, proponentes_settings)


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


def load_all():
    try:
        DataLoader(**convenios_settings).load(use_pagination=False)
        DataLoader(**emendas_settings).load(use_pagination=False)
        DataLoader(**proponentes_settings).load(use_pagination=False)
        DataLoader(**municipios_settings).load(use_pagination=False)
        DataLoader(**movimento_settings).load(use_pagination=False)
        
        return "Ok", 200
        
    except Exception as e:
        return str(e), 500