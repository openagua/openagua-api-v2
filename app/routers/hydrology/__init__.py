from flask import Blueprint

hydrology = Blueprint('hydrology', __name__)

from . import routes