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
        if type(value) == str:
            value = date_parse(value)

        value = value.strftime("%Y-%m-%d")

    except Exception as e:
        pass

    return value

@datetime_scalar.value_parser
def parse_datetime_value(value):
    try:
        return date_parse(value)

    except (ValueError, TypeError):
        raise ValueError(f'"{value}" is not a valid date string')

query = ObjectType("Query")

query.set_field("buscarEmendas", resolve_emendas)
query.set_field("buscarConvenios", resolve_convenios)
query.set_field("buscarProponentes", resolve_proponentes)
query.set_field("buscarMovimento", resolve_movimento)
query.set_field("buscarMunicipios", resolve_municipios)
query.set_field("buscarAtributos", resolve_atributos)
query.set_field("buscarFornecedores", resolve_fornecedores)
query.set_field("buscarEstados", resolve_estados)
query.set_field("buscarLicitacoes", resolve_licitacoes)
query.set_field("buscarParlamentares", resolve_parlamentares)

convenio = ObjectType("Convenio")
convenio.set_field("PROPONENTE", resolve_conv_proponente)
convenio.set_field("EMENDAS", resolve_conv_emendas)
convenio.set_field("MOVIMENTO", resolve_conv_movimento)
convenio.set_field("FORNECEDORES", resolve_conv_fornecedores)
convenio.set_field("LICITACOES", resolve_conv_licitacoes)
convenio.set_field("MUNICIPIO", resolve_conv_municipio)
convenio.set_field("ESTADO", resolve_conv_estado)
convenio.set_field("PARLAMENTARES", resolve_conv_parlamentares)

proponente = ObjectType("Proponente")
proponente.set_field("MUNICIPIO", resolve_prop_municipio)
proponente.set_field("CONVENIOS", resolve_prop_convenios)
proponente.set_field("ESTADO", resolve_prop_estado)
proponente.set_field("FORNECEDORES", resolve_prop_fornecedores)
proponente.set_field("LICITACOES", resolve_prop_licitacoes)
proponente.set_field("PARLAMENTARES", resolve_prop_parlamentares)

emenda = ObjectType("Emenda")
emenda.set_field("CONVENIOS", resolve_emd_convenios)
emenda.set_field("FORNECEDORES", resolve_emd_fornecedores)

movimento = ObjectType("Movimento")
movimento.set_field("CONVENIO", resolve_mov_convenio)
movimento.set_field("FORNECEDOR", resolve_mov_fornecedor)

estado = ObjectType("Estado")
estado.set_field("MUNICIPIOS", resolve_est_municipios)
estado.set_field("CONVENIOS", resolve_est_convenios)
estado.set_field("PROPONENTES", resolve_est_proponentes)
estado.set_field("FORNECEDORES", resolve_est_fornecedores)

municipio = ObjectType("Municipio")
municipio.set_field("ESTADO", resolve_mun_estado)
municipio.set_field("CONVENIOS", resolve_mun_convenios)
municipio.set_field("PROPONENTES", resolve_mun_proponentes)
municipio.set_field("FORNECEDORES", resolve_mun_fornecedores)

fornecedor = ObjectType("Fornecedor")
fornecedor.set_field("CONVENIOS", resolve_forn_convenios)
fornecedor.set_field("MOVIMENTO", resolve_forn_movimento)
fornecedor.set_field("MUNICIPIOS", resolve_forn_municipios)
fornecedor.set_field("ESTADOS", resolve_forn_estados)
fornecedor.set_field("EMENDAS", resolve_forn_emendas)
fornecedor.set_field("RESUMO", resolve_forn_summary)
fornecedor.set_field("PARLAMENTARES", resolve_forn_parlamentares)

licitacao = ObjectType("Licitacao")
licitacao.set_field("CONVENIO", resolve_lic_convenio)
licitacao.set_field("PROPONENTE", resolve_lic_proponente)

parlamentar = ObjectType("Parlamentar")
parlamentar.set_field("CONVENIOS", resolve_par_convenios)
parlamentar.set_field("FORNECEDORES", resolve_par_fornecedores)
parlamentar.set_field("LICITACOES", resolve_par_licitacoes)
parlamentar.set_field("EMENDAS", resolve_par_emendas)
parlamentar.set_field("PROPONENTES", resolve_par_proponentes)
parlamentar.set_field("MOVIMENTO", resolve_par_movimento)


type_defs = load_schema_from_path(os.path.join(
    os.path.realpath(os.path.dirname(__file__)), 'schemas'))


schema = make_executable_schema(
    type_defs, query, convenio, proponente, emenda, movimento, municipio, 
    fornecedor, estado, licitacao, parlamentar, datetime_scalar)

from app.graphql.routes import init_routes
