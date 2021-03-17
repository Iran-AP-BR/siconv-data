# coding: utf-8
"""GraphQL.
   """

from flask import Blueprint
from ariadne import load_schema_from_path, make_executable_schema, ObjectType, ScalarType
from .resolvers import *
import os
from dateutil.parser import parse as date_parse
from datetime import datetime

#GRAPHQL_BLUEPRINT_NAME = 'graphql'
#GRAPHQL_BLUEPRINT_URL_PREFIX = '/graphql'
#GRAPHQL_SCHEMA_FILENAME = 'schema.graphql'

blueprint = Blueprint('graphql', __name__, url_prefix='/graphql')

datetime_scalar = ScalarType("Datetime")

@datetime_scalar.serializer
def serialize_datetime(value):
    try:
        value = value.strftime("%Y-%m-%d")

    except Exception as e:
        pass

    return value

@datetime_scalar.value_parser
def parse_datetime_value(value):
    return date_parse(value)


query = ObjectType("Query")

query.set_field("buscarEmendas", resolve_emendas)
query.set_field("buscarConvenios", resolve_convenios)
query.set_field("buscarProponentes", resolve_proponentes)
query.set_field("buscarMovimento", resolve_movimento)
query.set_field("buscarMunicipios", resolve_municipios)
query.set_field("buscarAtributos", resolve_atributos)

convenio = ObjectType("Convenio")
convenio.set_field("PROPONENTE", resolve_conv_proponente)
convenio.set_field("MOVIMENTO", resolve_conv_movimento)
convenio.set_field("EMENDAS", resolve_conv_emendas)

proponente = ObjectType("Proponente")
proponente.set_field("CONVENIOS", resolve_prop_convenios)
proponente.set_field("MUNICIPIO", resolve_prop_municipios)

emenda = ObjectType("Emenda")
emenda.set_field("CONVENIOS", resolve_emd_convenios)

movimento = ObjectType("Movimento")
movimento.set_field("CONVENIO", resolve_mov_convenio)

municipio = ObjectType("Municipio")
municipio.set_field("PROPONENTES", resolve_mun_proponentes)

type_defs = load_schema_from_path(os.path.join(
    os.path.realpath(os.path.dirname(__file__)), 'schemas'))


schema = make_executable_schema(
    type_defs, datetime_scalar, query, convenio, proponente, emenda, movimento, municipio)

from app.graphql.routes import init_routes
