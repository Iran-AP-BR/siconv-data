# coding: utf-8
"""Application.

   Defines the create_app function wihich is to be called to create an instance of the application. 
   Returns an instance of app. Additionally, it executes initialization of all routes.
   """

from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
from app.workers.download_data import update
from app.logger import app_log


sched = BackgroundScheduler()
@sched.scheduled_job('cron', day_of_week='*', hour='8/1', minute='*/15', max_instances=1)
def update_job():
    update()


def create_app():
    """This function has the role of create the app and initialize routes.
       It returns an instance of the application with all routes properly configured.
       """
   
    from flask import Flask
    from flask_cors import CORS
    from app.api.routes import init_routes
   
    app = Flask(__name__)
    CORS(app)
    init_routes(app)
    sched.start()

    return app


