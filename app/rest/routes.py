# coding: utf-8
"""rest_routes.
   """

from flask import Blueprint, url_for
from .resources import data_atual, files, municipios, swagger
import os

rest_bp = Blueprint('rest', __name__, url_prefix='/rest', template_folder='templates')
def init_routes(config):
   #rest_bp.add_url_rule('/', 'index', swagger, methods=['GET'])
   rest_bp.add_url_rule('/', 'swagger', swagger, methods=['GET'])
   rest_bp.add_url_rule('/data_atual', 'data_atual', data_atual, methods=['GET'])
   rest_bp.add_url_rule('/municipios', 'municipios', municipios, methods=['GET'])
   rest_bp.add_url_rule(f'/{config["DATA_ENDPOINT"]}/<tableName>', config["DATA_ENDPOINT"], files, methods=['GET'])
   
