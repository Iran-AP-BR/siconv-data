# coding: utf-8
"""Resources.
   """
from flask import Response, send_from_directory, render_template, current_app as app
import pandas as pd
from app.security import api_key_required
from . import check_target
import os

@api_key_required
def data_atual():
   filename = os.path.join(app.config['DATA_FOLDER'], app.config.get('CURRENT_DATE_FILENAME'))
   data_atual = pd.read_csv(filename, compression='gzip', encoding='utf-8')

   resp = Response(data_atual.loc[0, 'DATA_ATUAL'])
   resp.headers['content-type'] = 'plain/text; charset=utf-8'
   return resp

@api_key_required
@check_target
def files(tableName):
   filename = f'{tableName}{app.config["FILE_EXTENTION"]}'
   return send_from_directory(app.config['DATA_FOLDER'], 
            filename, mimetype=f'application/{app.config["COMPRESSION_METHOD"]}', 
            download_name=filename, as_attachment=True)

def swagger():
   return render_template('swagger.html', title=app.config['APP_TITLE'])