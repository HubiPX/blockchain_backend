from flask import Blueprint, session
from blueprints.auth import Auth

logout = Blueprint('logout', __name__)


@logout.route('', methods=['get'])
@Auth.logged_user
def _logout_():
    session.pop("logged_in", None)
    session.pop("user_id", None)
    return '', 200
