# coding: utf-8
"""GraphQL.
   """

from flask import Blueprint
import os
from ariadne import load_schema_from_path, make_executable_schema, ObjectType, ScalarType
from dateutil.parser import parse as date_parse
from .fields_defs import fields_defs
from .data_loaders.loaders import *


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


def resolve(load_function, single=False, obj=None, info=None, **kwargs):
    try:

        data, pagination = load_function(parent=obj, use_pagination=False if single else True, **kwargs)
        
        if not single:
             payload = {
                  "pagination": pagination,
                  "items": data
            }
        else:
            payload = data[0] if type(data) is list else data


    except Exception as error:

        payload = {
            "errors": [str(error)]
        }

    return payload


def set_field(type_, field_defs):
    type_.set_field(field_defs.get("field"), lambda obj, info, **kwargs: resolve(field_defs.get("loader"), 
                    single=field_defs.get("single", False), obj=obj, info=info, **kwargs))
    return type_


def setup_schema(fields_defs):
    types = []
    query = ObjectType("Query")

    for parent_field_defs in fields_defs:
        set_field(query, parent_field_defs)
        if parent_field_defs.get("children"):
            type_ = ObjectType(parent_field_defs.get("type"))
            for child_field_defs in parent_field_defs.get("children"):
                set_field(type_, child_field_defs)

            types += [type_]

    type_defs = load_schema_from_path(os.path.join(
        os.path.realpath(os.path.dirname(__file__)), 'schemas'))

    bindables = [query] + types + [datetime_scalar]

    return make_executable_schema(type_defs, bindables)




schema = setup_schema(fields_defs)

from app.graphql.routes import init_routes