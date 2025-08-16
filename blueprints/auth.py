from flask import session
from functools import wraps
from database.models import Users


class Auth:
    @staticmethod
    def logged_user(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not session.get('logged_in'):
                return '', 403

            return f(*args, **kwargs)

        return decorated_function

    @staticmethod
    def logged_vip(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not session.get('logged_in'):
                return '', 403

            user_id = session['user_id']
            account = Users.query.filter_by(id=user_id).first()

            if not account.admin >= 1:
                return '', 403

            return f(*args, **kwargs)

        return decorated_function

    @staticmethod
    def logged_mod(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not session.get('logged_in'):
                return '', 403

            user_id = session['user_id']
            account = Users.query.filter_by(id=user_id).first()

            if not account.admin >= 2:
                return '', 403

            return f(*args, **kwargs)

        return decorated_function

    @staticmethod
    def logged_admin(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not session.get('logged_in'):
                return '', 403

            user_id = session['user_id']
            account = Users.query.filter_by(id=user_id).first()

            if not account.admin >= 3:
                return '', 403

            return f(*args, **kwargs)

        return decorated_function

    @staticmethod
    def logged_rcon(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not session.get('logged_in'):
                return '', 403

            user_id = session['user_id']
            account = Users.query.filter_by(id=user_id).first()

            if not account.admin == 4:
                return '', 403

            return f(*args, **kwargs)

        return decorated_function
