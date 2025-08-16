import os


class Config:
    SECRET_KEY = "sekret"
    SQLALCHEMY_DATABASE_URI = 'mysql://root:102309Spot@localhost/blockchain'
    SQLALCHEMY_BINDS = {
        'sqlite_db': f'sqlite:///{os.path.abspath("database/database.db")}'
    }
    SQLALCHEMY_ECHO = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    MONGO_URI = "mongodb://localhost:27017/blockchain"

# class Config:
#     SECRET_KEY = "sekret"
#     SQLALCHEMY_DATABASE_URI = f'sqlite:///{os.path.abspath("database/database.db")}'
#     SQLALCHEMY_ECHO = False
#     SQLALCHEMY_TRACK_MODIFICATIONS = False
