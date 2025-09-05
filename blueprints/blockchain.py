from flask import Blueprint, session, request, jsonify
from sqlalchemy import desc
from database.models import Users, BlockchainBlockMySQL, TransactionsMySQL, MempoolTransactionMySQL, \
    BlockchainTransactionMySQL
from blueprints.auth import Auth

blockchain = Blueprint('blockchain', __name__)


@blockchain.route('/last_3_transactions', methods=['GET'])
def get_last_3_transactions():
    recent_transactions = TransactionsMySQL.query.order_by(
        desc(TransactionsMySQL.id)
    ).limit(3).all()

    return jsonify([{
        "sender": t.sender,
        "recipient": t.recipient,
        "amount": t.amount
    } for t in recent_transactions])


@blockchain.route('/transactions', methods=['POST'])
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
        return jsonify({"message": "Numer strony musi być większy lub równy 1."}), 400
    elif page > max_page and total_transactions > 0:
        return jsonify({"message": f"Strona {page} nie istnieje. Maksymalna strona to {max_page}."}), 400
    elif total_transactions == 0:
        return jsonify({
            "page": page,
            "max_page": 0,
            "transactions": []
        }), 200

    offset = (page - 1) * per_page

    transactions = TransactionsMySQL.query.order_by(
        desc(TransactionsMySQL.id)
    ).offset(offset).limit(per_page).all()

    return jsonify({
        "score": user_score,
        "user_id": user_id,
        "page": page,
        "max_page": max_page,
        "transactions": [{
            "id": x.id,
            "sender": x.sender,
            "recipient": x.recipient,
            "amount": x.amount,
            "date": x.date
        } for x in transactions]
    }), 200


@blockchain.route('/last_3_blocks', methods=['GET'])
def get_last_3_blocks():
    # Pobranie ostatnich 3 bloków posortowanych malejąco po ID
    recent_blocks = BlockchainBlockMySQL.query.order_by(
        desc(BlockchainBlockMySQL.id)
    ).limit(3).all()

    # Zwrócenie JSON-a
    return jsonify([{
        "block_number": b.index,
        "timestamp": b.timestamp,
        "proof": b.proof,
        "previous_hash": b.previous_hash,
        "merkle_root": b.merkle_root
    } for b in recent_blocks])


@blockchain.route('/blocks', methods=['POST'])
@Auth.logged_mod
def blocks():
    data = request.get_json()
    page = data.get("page", 1)
    per_page = 50

    # Liczenie wszystkich bloków w tabeli blockchain_blocks
    total_blocks = BlockchainBlockMySQL.query.count()
    max_page = (total_blocks + per_page - 1) // per_page

    # Pobranie informacji o użytkowniku
    user = Users.query.filter_by(id=session.get("user_id")).first()
    user_score = user.score if user else None
    user_id = user.id if user else None

    # Walidacja numeru strony
    if page < 1:
        return jsonify({"message": "Numer strony musi być większy lub równy 1."}), 400
    elif page > max_page and total_blocks > 0:
        return jsonify({"message": f"Strona {page} nie istnieje. Maksymalna strona to {max_page}."}), 400
    elif total_blocks == 0:
        return jsonify({
            "page": page,
            "max_page": 0,
            "blocks": []
        }), 200

    offset = (page - 1) * per_page

    # Pobranie bloków z tabeli blockchain_blocks
    blocks = BlockchainBlockMySQL.query.order_by(
        desc(BlockchainBlockMySQL.id)
    ).offset(offset).limit(per_page).all()

    # Zwrócenie JSON-a
    return jsonify({
        "score": user_score,
        "user_id": user_id,
        "page": page,
        "max_page": max_page,
        "blocks": [{
            "block_number": x.index,
            "timestamp": x.timestamp,
            "proof": x.proof,
            "previous_hash": x.previous_hash,
            "merkle_root": x.merkle_root
        } for x in blocks]
    }), 200


@blockchain.route('/last_3_mempool', methods=['GET'])
def get_last_3_mempool():
    recent_txs = MempoolTransactionMySQL.query.order_by(
        desc(MempoolTransactionMySQL.id)
    ).limit(3).all()

    return jsonify([{
        "id": tx.id,
        "sender": tx.sender,
        "recipient": tx.recipient,
        "amount": tx.amount,
        "date": tx.date
    } for tx in recent_txs])


@blockchain.route('/mempool', methods=['POST'])
@Auth.logged_mod
def mempool():
    data = request.get_json()
    page = data.get("page", 1)
    per_page = 50

    total_txs = MempoolTransactionMySQL.query.count()
    max_page = (total_txs + per_page - 1) // per_page

    user = Users.query.filter_by(id=session.get("user_id")).first()
    user_score = user.score if user else None
    user_id = user.id if user else None

    if page < 1:
        return jsonify({"message": "Numer strony musi być większy lub równy 1."}), 400
    elif page > max_page and total_txs > 0:
        return jsonify({"message": f"Strona {page} nie istnieje. Maksymalna strona to {max_page}."}), 400
    elif total_txs == 0:
        return jsonify({
            "page": page,
            "max_page": 0,
            "transactions": []
        }), 200

    offset = (page - 1) * per_page

    txs = MempoolTransactionMySQL.query.order_by(
        desc(MempoolTransactionMySQL.id)
    ).offset(offset).limit(per_page).all()

    return jsonify({
        "score": user_score,
        "user_id": user_id,
        "page": page,
        "max_page": max_page,
        "transactions": [{
            "id": tx.id,
            "sender": tx.sender,
            "recipient": tx.recipient,
            "amount": tx.amount,
            "date": tx.date
        } for tx in txs]
    }), 200


@blockchain.route('/blocks_transactions', methods=['POST'])
@Auth.logged_mod
def blocks_transactions():
    data = request.get_json()
    page = data.get("page", 1)
    block_id = data.get("block_id")
    per_page = 50

    if not block_id:
        return jsonify({"message": "Nie podano ID bloku."}), 400

    # Liczenie wszystkich transakcji w danym bloku
    total_txs = BlockchainTransactionMySQL.query.filter_by(block_id=block_id).count()
    max_page = (total_txs + per_page - 1) // per_page

    # Pobranie informacji o użytkowniku
    user = Users.query.filter_by(id=session.get("user_id")).first()
    user_score = user.score if user else None
    user_id = user.id if user else None

    # Walidacja numeru strony
    if page < 1:
        return jsonify({"message": "Numer strony musi być większy lub równy 1."}), 400
    elif page > max_page and total_txs > 0:
        return jsonify({"message": f"Strona {page} nie istnieje. Maksymalna strona to {max_page}."}), 400
    elif total_txs == 0:
        return jsonify({
            "page": page,
            "max_page": 0,
            "transactions": []
        }), 200

    offset = (page - 1) * per_page

    # Pobranie transakcji dla danego bloku
    transactions = BlockchainTransactionMySQL.query.filter_by(block_id=block_id)\
        .order_by(desc(BlockchainTransactionMySQL.id))\
        .offset(offset).limit(per_page).all()

    # Zwrócenie JSON-a
    return jsonify({
        "score": user_score,
        "user_id": user_id,
        "page": page,
        "max_page": max_page,
        "transactions": [{
            "id": t.id,
            "sender": t.sender,
            "recipient": t.recipient,
            "amount": t.amount,
            "date": t.date
        } for t in transactions]
    }), 200
