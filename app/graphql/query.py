# coding: utf-8
"""Resolvers.
   """

from flask import jsonify, request
from ariadne import load_schema_from_path, make_executable_schema, graphql_sync, ObjectType
from ariadne.constants import PLAYGROUND_HTML
from .resolvers import resolve_emendas, resolve_convenios, resolve_convenio, resolve_proponentes, resolve_movimentos, resolve_proponente
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
        #debug=app.debug
    )

    status_code = 200 if success else 400
    return jsonify(result), status_code



query = ObjectType("Query")


query.set_field("buscarEmendas", resolve_emendas)
query.set_field("buscarConvenios", resolve_convenios)
query.set_field("buscarProponentes", resolve_proponentes)
query.set_field("buscarMovimento", resolve_movimentos)

convenio = ObjectType("Convenio")
convenio.set_field("PROPONENTE", resolve_proponente)
convenio.set_field("MOVIMENTO", resolve_movimentos)
convenio.set_field("EMENDAS", resolve_emendas)

proponente = ObjectType("Proponente")
proponente.set_field("CONVENIOS", resolve_convenios)

emenda = ObjectType("Emenda")
emenda.set_field("CONVENIOS", resolve_convenios)

movimento = ObjectType("Movimento")
movimento.set_field("CONVENIO", resolve_convenio)

type_defs = load_schema_from_path(os.path.join(os.path.realpath(os.path.dirname(__file__)), 'schema.graphql'))

schema = make_executable_schema(type_defs, query, convenio, proponente, emenda, movimento)