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

    import app.rest as rest
    import app.graphql as graphql

    rest.init_routes(app.config)
    graphql.init_routes()

    app.register_blueprint(rest.blueprint)
    app.register_blueprint(graphql.blueprint)

    print(app.url_map)

    return app


