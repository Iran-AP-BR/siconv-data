# coding: utf-8
"""Api.
   """

from flask import abort
from functools import wraps

filename_list = ['emendas', 'emendas_convenios', 'convenios',
                'proponentes', 'movimento', 'municípios brasileiros']

def check_target(f):
	@wraps(f)
	def decorated_function(*args, **kwargs):
		if kwargs.get('name') not in filename_list:
			abort(404)
		return f(*args, **kwargs)
	return decorated_function
