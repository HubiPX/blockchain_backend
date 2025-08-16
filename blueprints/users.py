from flask import Blueprint, session, request
from sqlalchemy import desc
from database.models import db
from database.models import Users, TransactionsMySQL
from database.hash import Hash
from blueprints.auth import Auth


users = Blueprint('users', __name__)


@users.route('/create', methods=['post'])
def _create_():
    post = request.get_json()

    repassword = post.get("repassword")
    password = post.get("password")
    username = post.get("username")

    if password != repassword:
        return 'Podane hasła są różne!', 406

    if len(password) < 3:
        return 'Hasło jest za krótkie!', 400
    elif len(username) < 3:
        return 'Login jest za krótki!', 400
    elif len(password) > 20:
        return 'Hasło jest za długie!', 400
    elif len(username) > 15:
        return 'Login jest za długi!', 400

    hash_pwd = Hash.hash_password(password)

    if Users.query.filter_by(username=username).first():
        return 'Jest już użytkownik o takim nicku.', 406

    new_user = Users(username=username, password=hash_pwd)

    db.session.add(new_user)
    db.session.commit()
    return 'Utworzono konto pomyślnie!', 201


@users.route('/stats', methods=['GET'])
@Auth.logged_user
def _stats_():
    data = request.args  # dla GET parametry lepiej pobierać z query string
    page = int(data.get("page", 1))
    per_page = 10
    total_users = Users.query.count()
    max_page = (total_users + per_page - 1) // per_page

    user = Users.query.filter_by(id=session.get("user_id")).first()
    user_score = user.score if user else None
    user_id = user.id if user else None

    if page < 1:
        return {"error": "Numer strony musi być większy lub równy 1."}, 400
    elif page > max_page and total_users > 0:
        return {"error": f"Strona {page} nie istnieje. Maksymalna strona to {max_page}."}, 400
    elif total_users == 0:
        return {
            "page": page,
            "max_page": 0,
            "users": []
        }, 200

    offset = (page - 1) * per_page

    paged_users = Users.query.order_by(
        desc(Users.score),
        desc(Users.last_login)
    ).offset(offset).limit(per_page).all()

    return {
        "score": user_score,
        "user_id": user_id,
        "page": page,
        "max_page": max_page,
        "users": [{
            "username": u.username,
            "admin": u.admin,
            "last_login": u.last_login,
            "score": u.score,
            "vip_date": u.vip_date,
            "place": (page - 1) * per_page + i + 1
        } for i, u in enumerate(paged_users)]
    }, 200


@users.route('/top_3', methods=['GET'])
def get_top_3():
    top_users = Users.query.order_by(
        desc(Users.score),
        desc(Users.last_login)
    ).limit(3).all()
    return [{
        "username": x.username,
        "admin": x.admin,
        "score": x.score,
        "place": top_users.index(x) + 1
    } for x in top_users]


@users.route('/change-password', methods=['post'])
@Auth.logged_user
def _change_password_():
    post = request.get_json()

    current_pwd = post.get("password")
    new_pwd = post.get("new_password")
    new_pwd2 = post.get("new_password2")

    if new_pwd != new_pwd2:
        return 'Nowe hasła są różne.', 406

    if not current_pwd or not new_pwd or not new_pwd2:
        return 'Wypełnij wszystkie pola.', 400
    elif len(new_pwd) < 3:
        return 'Nowe hasło jest za krótkie.', 400
    elif len(new_pwd) > 20:
        return 'Nowe hasło jest za długie.', 400
    elif current_pwd == new_pwd:
        return 'Nowe hasło musi być inne niż obecne.', 400

    user = Users.query.filter_by(id=session["user_id"]).first()

    if not Hash.verify_password(user.password, current_pwd):
        return 'Stare hasło jest błędne.', 400

    pwd_hash = Hash.hash_password(new_pwd)
    user.password = pwd_hash
    db.session.commit()
    return '', 200


@users.route('/last_3_transactions', methods=['GET'])
def get_last_3_transactions():
    recent_transactions = TransactionsMySQL.query.order_by(
        desc(TransactionsMySQL.id)
    ).limit(3).all()

    return [{
        "sender": t.sender,
        "recipient": t.recipient,
        "amount": t.amount
    } for t in recent_transactions]


@users.route('/transactions', methods=['POST'])
@Auth.logged_mod
def transactions():
    data = request.get_json()
    page = data.get("page", 1)
    per_page = 50

    total_transactions = TransactionsMySQL.query.count()
    max_page = (total_transactions + per_page - 1) // per_page  # zaokrąglenie w górę

    user = Users.query.filter_by(id=session.get("user_id")).first()
    user_score = user.score if user else None
    user_id = user.id if user else None

    if page < 1:
        return {"error": "Numer strony musi być większy lub równy 1."}, 400
    elif page > max_page and total_transactions > 0:
        return {"error": f"Strona {page} nie istnieje. Maksymalna strona to {max_page}."}, 400
    elif total_transactions == 0:
        return {
            "page": page,
            "max_page": 0,
            "transactions": []
        }, 200

    offset = (page - 1) * per_page

    transactions = TransactionsMySQL.query.order_by(
        desc(TransactionsMySQL.id)
    ).offset(offset).limit(per_page).all()

    return {
        "score": user_score,
        "user_id": user_id,
        "page": page,
        "max_page": max_page,
        "transactions": [{
            "sender": x.sender,
            "recipient": x.recipient,
            "amount": x.amount,
            "date": x.date,
            "code": x.code
        } for x in transactions]
    }, 200


