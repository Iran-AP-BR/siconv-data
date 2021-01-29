# coding: utf-8
"""Securuty.
   """

from flask import request, abort, current_app as app
from functools import wraps

def api_key_required(f):
	@wraps(f)
	def decorated_function(*args, **kwargs):
		if app.config['API_KEY'] is None or request.headers.get("X-Api-Key") != app.config['API_KEY']:
			abort(401)
		return f(*args, **kwargs)
	return decorated_function
