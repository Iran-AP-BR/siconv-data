# coding: utf-8
"""Securuty.
   """

from flask import request, abort
from functools import wraps
from app.config import API_KEY

def api_key_required(f):
	@wraps(f)
	def decorated_function(*args, **kwargs):
		if API_KEY is None or request.headers.get("X-Api-Key") != API_KEY:
			abort(401)
		return f(*args, **kwargs)
	return decorated_function
