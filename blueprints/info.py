from flask import Blueprint, session
from database.models import Users

info = Blueprint('info', __name__)


@info.route('', methods=['get'])
def _info():
    if not session.get("logged_in"):
        return {"status": "NOT_LOGGED_IN"}

    user = Users.query.filter_by(id=session['user_id']).first()
    return {"status": "LOGGED_IN", "username": user.username, "is_admin": user.admin,
            "user_id": user.id, "last_login": user.last_login}
