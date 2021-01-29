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

    from app.api.routes import init_routes
    init_routes(app)

    return app


