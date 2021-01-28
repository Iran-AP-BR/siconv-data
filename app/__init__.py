# coding: utf-8
"""Application.

   Defines the create_app function wihich is to be called to create an instance of the application. 
   Returns an instance of app. Additionally, it executes initialization of all routes.
   """

from datetime import datetime
from app.logger import app_log



def create_app():
    """This function has the role of create the app and initialize routes.
       It returns an instance of the application with all routes properly configured.
       """
   
    from flask import Flask
    from flask_cors import CORS
    from app.api.routes import init_routes
    from flask_swagger_ui import get_swaggerui_blueprint
   
    app = Flask(__name__)
    CORS(app)
    init_routes(app)

    SWAGGER_URL = '' 
    API_URL = '/static/swagger.json' 

    swaggerui_blueprint = get_swaggerui_blueprint(
      SWAGGER_URL,
      API_URL,
      config={  
         'app_name': "Dados do Siconv"
      },
    )

    app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)

    return app

