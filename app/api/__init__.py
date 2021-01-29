# coding: utf-8
"""Api.
   """

from flask import abort
from functools import wraps
from app.config import TABLE_LIST

def check_target(f):
	@wraps(f)
	def decorated_function(*args, **kwargs):
		if kwargs.get('tableName') not in TABLE_LIST:
			abort(404)
		return f(*args, **kwargs)
	return decorated_function
