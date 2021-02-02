# coding: utf-8
"""Resources.
   """
from flask import Response, send_file, render_template, current_app as app
from app.security import api_key_required
from app.api import check_target
import os

@api_key_required
def data_atual():
   filename = os.path.join(app.config['DATA_FOLDER'], "data_atual.txt")

   with open(filename, 'r') as fd:
      data_atual = fd.read()

   resp = Response(data_atual)
   resp.headers['content-type'] = 'plain/text; charset=utf-8'
   return resp

@api_key_required
def municipios():
   filename = os.path.join(app.config['STATIC_FOLDER'], "municipios_brasileiros.json")
   return send_file(filename, mimetype='application/json', attachment_filename='municipios_brasileiros.json', as_attachment=True)

@api_key_required
@check_target
def files(tableName):
   filename = f'{tableName}{app.config["FILE_EXTENTION"]}'
   path = os.path.join(app.config['DATA_FOLDER'], filename)
   return send_file(path, mimetype=f'application/{app.config["COMPRESSION_METHOD"]}', attachment_filename=filename, as_attachment=True)

def swagger():
   return render_template('swagger.html', title=app.config['APP_TITLE'])