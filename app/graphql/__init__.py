# coding: utf-8
"""GraphQL.
   """
from flask import Blueprint
from ariadne import load_schema_from_path, make_executable_schema, ObjectType
from .resolvers import resolve_emendas, resolve_convenios, resolve_convenio, resolve_proponentes, resolve_movimentos, resolve_proponente
import os


#GRAPHQL_BLUEPRINT_NAME = 'graphql'
#GRAPHQL_BLUEPRINT_URL_PREFIX = '/graphql'
#GRAPHQL_SCHEMA_FILENAME = 'schema.graphql'

blueprint = Blueprint('graphql', __name__, url_prefix='/graphql')


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

type_defs = load_schema_from_path(os.path.join(os.path.realpath(os.path.dirname(__file__)), 'schemas'))

schema = make_executable_schema(type_defs, query, convenio, proponente, emenda, movimento)

from app.graphql.routes import init_routes


