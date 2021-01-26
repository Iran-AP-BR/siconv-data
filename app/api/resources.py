# coding: utf-8
"""Resources.
   """
from flask import send_file, make_response, jsonify
from app.security import api_key_required
from app.api import check_target
import os
from app.config import SITE_ROOT, COMPRESSION_METHOD, FILE_EXTENTION

@api_key_required
def data_atual():
   filename = os.path.join(SITE_ROOT, "files", "data_atual.txt")

   with open(filename, 'r') as fd:
      data_atual = fd.read()

   return data_atual

@api_key_required
def municipios():
   filename = os.path.join(SITE_ROOT, "files", "municipios brasileiros.json")
   return send_file(filename, mimetype='application/json', attachment_filename='municipios brasileiros.json', as_attachment=True)

@api_key_required
@check_target
def files(name):
   filename = f'{name}{FILE_EXTENTION}'
   path = f'files/{filename}'
   return send_file(path, mimetype=f'application/{COMPRESSION_METHOD}', attachment_filename=filename, as_attachment=True)