# coding: utf-8
"""Api.
   """

from flask import Blueprint, abort, current_app as app
from functools import wraps

blueprint = Blueprint('rest', __name__, url_prefix='/rest', template_folder='templates', static_folder='static')

def check_target(f):
	@wraps(f)
	def decorated_function(*args, **kwargs):
		if kwargs.get('tableName') not in app.config['TABLE_LIST']:
			abort(404)
		return f(*args, **kwargs)
	return decorated_function

from app.rest.routes import init_routes