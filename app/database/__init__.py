# coding: utf-8
"""
Database
"""
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

def config_db(app):
   db.init_app(app)