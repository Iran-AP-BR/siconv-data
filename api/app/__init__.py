# coding: utf-8
"""Application.

   Defines the create_app function wihich is to be called to create an instance of the application. 
   Returns an instance of app. Additionally, it executes initialization of all routes.
   """

from flask import Flask
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

def create_app():
    """This function has the role of create the app and initialize routes.
       It returns an instance of the application with all routes properly configured.
       """
      
    app = Flask(__name__)
    
    from .config import Config
    app.config.from_object(Config())

    CORS(app)

    db.init_app(app)

    import app.rest as rest
    import app.graphql as graphql
    import app.views as views
    
    rest.init_routes(app.config)
    graphql.init_routes()
    
    app.register_blueprint(views.blueprint)
    app.register_blueprint(rest.blueprint)
    app.register_blueprint(graphql.blueprint)
   
    return app

from app.views import index

