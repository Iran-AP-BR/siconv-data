# coding: utf-8
"""Api.
   """

from flask import abort, current_app as app
from functools import wraps


def check_target(f):
	@wraps(f)
	def decorated_function(*args, **kwargs):
		if kwargs.get('tableName') not in app.config['TABLE_LIST']:
			abort(404)
		return f(*args, **kwargs)
	return decorated_function
