# coding: utf-8
"""Resources.
   """
from flask import Response, send_file, send_from_directory, render_template, current_app as app
import pandas as pd
from app.security import api_key_required
from . import check_target
import os
from io import BytesIO


@api_key_required
def data_atual():
   filename = os.path.join(app.config['DATA_FOLDER'], app.config.get('CURRENT_DATE_FILENAME'))
   data_atual = pd.read_parquet(filename)

   resp = Response(data_atual.loc[0, 'DATA_ATUAL'].strftime('%Y-%m-%d'))
   resp.headers['content-type'] = 'plain/text; charset=utf-8'
   return resp

@api_key_required
@check_target
def files_parquet(fileType, tableName):
   if fileType == 'parquet':
      return _parquet_file(tableName)
   elif fileType == 'csv':
      return _csv_file(tableName)
 
   resp = Response('Tipo de arquivo inválido.')
   resp.headers['content-type'] = 'plain/text; charset=utf-8'
   resp.status = 404
   return resp


def swagger():
   return render_template('swagger.html', title=app.config['APP_TITLE'])


def _parquet_file(tableName):
   filename = f'{tableName}{app.config["FILE_EXTENTION"]}'
   return send_from_directory(app.config['DATA_FOLDER'], 
            filename, mimetype=f'application/gzip', 
            download_name=filename, as_attachment=True)

def _csv_file(tableName):   
   csv_buffer = BytesIO()
   download_name = f'{tableName}.csv.gz'
   filename = os.path.join(app.config['DATA_FOLDER'], f'{tableName}{app.config["FILE_EXTENTION"]}')
   df = pd.read_parquet(filename)
   df.to_csv(csv_buffer, sep=';', compression='gzip', index=False)
   csv_buffer.seek(0)
   return send_file(csv_buffer, 
            mimetype=f'application/parquet', 
            download_name=download_name, as_attachment=True)