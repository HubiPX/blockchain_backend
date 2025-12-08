from flask import Blueprint, request, session, jsonify
from database.models import Users
from database.models import db
from database.hash import Hash
import datetime
from blockchain.system_score import add_score_system

login = Blueprint('login', __name__)


@login.route('', methods=['post'])
def _login_():
    post = request.get_json()

    username = post.get("username")
    password = post.get("password")

    if not username:
        return jsonify({"message": "Brak nazwy użytkownika!"}), 400
    elif not password:
        return jsonify({"message": "Brak hasła!"}), 400

    user = Users.query.filter_by(username=username).first()

    if not user:
        return jsonify({"message": "Brak użytkownika o tym nicku!"}), 401
    elif not Hash.verify_password(user.password, password):
        return jsonify({"message": "Podane hasło jest nieprawidłowe!"}), 401

    today = datetime.datetime.now()

    if user.ban_date is not None:
        if user.ban_date < today:
            user.ban_date = None
        else:
            return jsonify({"ban_date": user.ban_date}), 403

    if user.last_login is None:
        user.last_login = today
        add_score_system(100, user)
    elif today.date() != user.last_login.date():
        user.last_login = today
        add_score_system(100, user)
    elif today.date() == user.last_login.date():
        user.last_login = today

    if user.vip_date is not None and today > user.vip_date:
        user.vip_date = None
        user.admin = 0

    session['logged_in'] = True
    session['user_id'] = user.id
    db.session.commit()
    return {"username": user.username, "is_admin": user.admin, "user_id": user.id}, 200
