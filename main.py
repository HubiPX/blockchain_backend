from flask import Flask
from flask_socketio import SocketIO
from datetime import timedelta
from blockchain.blockchain_mysql import BlockchainMYSQL
from blockchain.blockchain_sqlite import BlockchainSQLite
from blockchain.blockchain_mongo import BlockchainMongo
from blueprints.login import login
from blueprints.admin import admin
from blueprints.logout import logout
from blueprints.users import users
from blueprints.transactions import transactions
from blueprints.info import info
from database.models import db, Users
from database.hash import Hash
from flask_cors import CORS
from flask_pymongo import PyMongo

app = Flask(__name__)
app.permanent_session_lifetime = timedelta(days=7)
app.config['SESSION_COOKIE_SAMESITE'] = 'None'
app.config['SESSION_COOKIE_SECURE'] = True
CORS(app, supports_credentials=True, origins="http://127.0.0.1:5500")
app.config.from_object('database.config.Config')


socketio = SocketIO(app, cors_allowed_origins="http://127.0.0.1:5500")

db.app = app
db.init_app(app)
mongo = PyMongo(app)

DEFAULT_ADMIN_USERNAME = "admin"
DEFAULT_ADMIN_PASSWORD = "admin"

with app.app_context():
    db.create_all()

    if Users.query.filter_by(username=DEFAULT_ADMIN_USERNAME).first() is None:
        pwd = Hash.hash_password(DEFAULT_ADMIN_PASSWORD)
        admin_account = Users(username=DEFAULT_ADMIN_USERNAME, password=pwd, admin=4)
        db.session.add(admin_account)
        db.session.commit()

    app.blockchains = {
        "mysql": BlockchainMYSQL(),
        "sqlite": BlockchainSQLite(),
        "mongo": BlockchainMongo(mongo)
    }

    transactions.blockchain = app.blockchains

app.register_blueprint(login, url_prefix='/api/login')
app.register_blueprint(logout, url_prefix='/api/logout')
app.register_blueprint(users, url_prefix='/api/users')
app.register_blueprint(info, url_prefix='/api/info')
app.register_blueprint(admin, url_prefix='/api/admin')
app.register_blueprint(transactions, url_prefix='/api/transactions')


if __name__ == "__main__":
    app.debug = True
    socketio.run(app, port=4400, allow_unsafe_werkzeug=True)
