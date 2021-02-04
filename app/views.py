# coding: utf-8
"""app views.
   """

from flask import Blueprint, send_from_directory, render_template, current_app as app

blueprint = Blueprint('views', __name__, url_prefix='/', template_folder='templates', static_folder='static')

@blueprint.route('/', methods=['GET'])
@blueprint.route('/index', methods=['GET'])
def index():
    return render_template('index.html', title=app.config['APP_TITLE'])

@blueprint.route('/model', methods=['GET'])
def model():
   filename = "model.png"
   return send_from_directory(app.config['DATA_FOLDER'], filename, mimetype='image/png', attachment_filename=filename, as_attachment=False)
