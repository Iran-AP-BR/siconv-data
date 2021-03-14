# coding: utf-8
"""
Database
"""
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

def config_db(app):
   db.init_app(app)

from .convenios import Convenio
from .emendas import Emenda
from .movimento import Movimento
from .proponentes import Proponente
from .municipios import Municipio
from .data_atual import DataAtual