# coding: utf-8
"""Resolvers.
   """

from flask import jsonify, request
from ariadne import load_schema_from_path, make_executable_schema, graphql_sync, snake_case_fallback_resolvers, ObjectType
from ariadne.constants import PLAYGROUND_HTML
from app.api.graphql.resolvers import resolve_emenda, resolve_emendas, resolve_convenio, resolve_convenios
from app.security import api_key_required

def graphql_playground():
    return PLAYGROUND_HTML, 200

@api_key_required
def graphql_server():
    data = request.get_json()

    success, result = graphql_sync(
        schema,
        data,
        context_value=request,
        #debug=app.debug
    )

    status_code = 200 if success else 400
    return jsonify(result), status_code



query = ObjectType("Query")

query.set_field("emendas", resolve_emendas)
query.set_field("emenda", resolve_emenda)
query.set_field("convenios", resolve_convenios)
query.set_field("convenio", resolve_convenio)

type_defs = load_schema_from_path("./app/api/graphql/schema.graphql")

schema = make_executable_schema(
    type_defs, query
)