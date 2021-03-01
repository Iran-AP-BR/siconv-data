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
    
    from .config import Config
    app.config.from_object(Config())
    
    CORS(app)

    import app.rest as rest
    import app.graphql as graphql
    import app.views as views
    
    rest.init_routes(app.config)
    graphql.init_routes()
    
    app.register_blueprint(views.blueprint)
    app.register_blueprint(rest.blueprint)
    app.register_blueprint(graphql.blueprint)
    '''
    from app.graphql.data_loaders.loaders import (DataLoader, dtypes_convenios, dtypes_emendas,
                                                 dtypes_movimento, dtypes_municipios, dtypes_proponentes,
                                                 parse_dates_convenios, parse_dates_movimento)
    
    with app.app_context():
       DataLoader(table_name='convenios',dtypes=dtypes_convenios, parse_dates=parse_dates_convenios).load(use_pagination=False)
       DataLoader(table_name='emendas',dtypes=dtypes_emendas).load(use_pagination=False)
       DataLoader(table_name='proponentes',dtypes=dtypes_proponentes).load(use_pagination=False)
       DataLoader(table_name='municipios',dtypes=dtypes_municipios).load(use_pagination=False)
       DataLoader(table_name='movimento',dtypes=dtypes_movimento, parse_dates=parse_dates_movimento).load(use_pagination=False)
    '''
    #print(app.url_map)
    return app

from app.views import index

