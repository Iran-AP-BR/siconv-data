# coding: utf-8
"""Application.

   Defines the create_app function wihich is to be called to create an instance of the application. 
   Returns an instance of app. Additionally, it executes initialization of all routes.
   """

from flask import Flask
from flask_cors import CORS

def create_app():
    """This function has the role of create the app and initialize routes.
       It returns an instance of the application with all routes properly configured.
       """
      
    app = Flask(__name__)
    
    from config import Config
    app.config.from_object(Config())
    
    CORS(app)

    from app.rest.routes import init_routes as init_rest_routes, rest_bp
    from app.graphql.routes import init_routes as init_graphql_routes, graphql_bp
    
    init_rest_routes(app.config)
    init_graphql_routes()

    app.register_blueprint(rest_bp)
    app.register_blueprint(graphql_bp)

    print(app.url_map)
    return app


