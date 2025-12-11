from database.models import db, BlockchainBlockMySQL, BlockchainTransactionMySQL, MempoolTransactionMySQL
from blockchain.blockchain_base import BlockchainBase
from collections import defaultdict


class BlockchainMYSQL(BlockchainBase):
    def get_last_block_from_db(self):
        # Pobranie ostatniego bloku z MySQL
        last_block_db = BlockchainBlockMySQL.query.order_by(BlockchainBlockMySQL.index.desc()).first()

        if not last_block_db:
            return None

        print(f"MYSQL Last block loaded: index {last_block_db.index}")

        # Pobranie transakcji powiązanych z blokiem
        transactions = BlockchainTransactionMySQL.query.filter_by(block_id=last_block_db.id).order_by(
            BlockchainTransactionMySQL.id).all()

        block_dict = {
            'index': last_block_db.index,
            'timestamp': last_block_db.timestamp,
            'transactions': [
                {
                    'id': tx.id,
                    'sender': tx.sender,
                    'recipient': tx.recipient,
                    'amount': tx.amount,
                    'date': tx.date
                }
                for tx in transactions
            ],
            'proof': last_block_db.proof,
            'previous_hash': last_block_db.previous_hash,
            'merkle_root': last_block_db.merkle_root
        }

        return block_dict

    def save_block_to_db(self, block, transactions):
        if not transactions:
            transactions = []

        db_block = BlockchainBlockMySQL(
            index=block['index'],
            timestamp=block['timestamp'],
            proof=block['proof'],
            previous_hash=block['previous_hash'],
            merkle_root=block['merkle_root'],
        )
        db.session.add(db_block)
        db.session.flush()

        for tx in transactions:
            db_tx = BlockchainTransactionMySQL(
                block_id=db_block.id,
                sender=tx['sender'],
                recipient=tx['recipient'],
                amount=tx['amount'],
                date=tx['date']
            )
            db.session.add(db_tx)

        db.session.commit()

    def save_transactions_to_mempool(self, transactions):
        if not transactions:
            return

        db_objects = [MempoolTransactionMySQL(**tx) for tx in transactions]
        db.session.add_all(db_objects)
        db.session.commit()

    # --- Nowe metody wymagane przez BlockchainBase ---
    def get_pending_transactions(self, limit):
        txs = (
            MempoolTransactionMySQL
            .query
            .order_by(MempoolTransactionMySQL.id.asc())
            .limit(limit)
            .all()
        )

        return [
            {
                'id': tx.id,
                'sender': tx.sender,
                'recipient': tx.recipient,
                'amount': tx.amount,
                'date': tx.date
            }
            for tx in txs
        ]

    def get_mempool_count(self):
        # Zwraca liczbę transakcji w mempoolu
        return MempoolTransactionMySQL.query.count()

    def clear_pending_transactions(self, transactions):
        # Usuwa z DB dokładnie te transakcje, które zostały już użyte w bloku
        if not transactions:
            return
        ids = [tx['id'] for tx in transactions]
        MempoolTransactionMySQL.query.filter(MempoolTransactionMySQL.id.in_(ids)).delete(synchronize_session=False)
        db.session.commit()

    def get_chain_batch(self, offset: int, limit: int) -> list[dict]:
        """Pobiera fragment blockchaina z bazy (paginacja)."""
        # Pobranie bloków
        blocks = (BlockchainBlockMySQL.query
                  .order_by(BlockchainBlockMySQL.index.asc())
                  .offset(offset)
                  .limit(limit)
                  .all())

        if not blocks:
            return []

        # Pobranie wszystkich transakcji dla tych bloków naraz
        block_ids = [block.id for block in blocks]
        all_txs = (BlockchainTransactionMySQL.query
                   .filter(BlockchainTransactionMySQL.block_id.in_(block_ids))
                   .order_by(BlockchainTransactionMySQL.id.asc())
                   .all())

        # Grupowanie transakcji według block_id
        tx_by_block = defaultdict(list)
        for tx in all_txs:
            tx_by_block[tx.block_id].append(tx)

        # Tworzenie listy bloków z transakcjami
        chain = []
        for block in blocks:
            block_dict = {
                'index': block.index,
                'timestamp': block.timestamp,
                'transactions': [
                    {
                        'id': tx.id,
                        'sender': tx.sender,
                        'recipient': tx.recipient,
                        'amount': tx.amount,
                        'date': tx.date
                    }
                    for tx in tx_by_block[block.id]
                ],
                'proof': block.proof,
                'previous_hash': block.previous_hash,
                'merkle_root': block.merkle_root
            }
            chain.append(block_dict)

        return chain

    def get_transaction_proof(self, block_index: int, tx_id):
        block = BlockchainBlockMySQL.query.filter_by(index=block_index).first()
        if not block:
            return None

        txs = BlockchainTransactionMySQL.query.filter_by(block_id=block.id).order_by(BlockchainTransactionMySQL.id.asc()).all()
        transactions = [
            {"id": tx.id, "sender": tx.sender, "recipient": tx.recipient, "amount": tx.amount, "date": tx.date}
            for tx in txs
        ]

        # znajdź index transakcji w bloku
        tx_index = next((i for i, tx in enumerate(transactions) if tx["id"] == int(tx_id)), None)
        if tx_index is None:
            return None

        proof = self.get_merkle_proof(transactions, tx_index)
        return {
            "transaction": transactions[tx_index],
            "proof": proof,
            "merkle_root": block.merkle_root
        }

    @staticmethod
    def get_user_score(username: str):
        """
        Zwraca oczekiwaną ilość punktów użytkownika `username`
        oraz statystyki transakcji (ile wysłał/odebrał).
        """
        expected_score = 0

        sent_count = 0
        received_count = 0

        # 1. Transakcje z zatwierdzonych bloków
        blocks = BlockchainBlockMySQL.query.order_by(BlockchainBlockMySQL.id.asc()).all()
        for block in blocks:
            txs = BlockchainTransactionMySQL.query.filter_by(block_id=block.id)\
                .order_by(BlockchainTransactionMySQL.id.asc()).all()
            for tx in txs:
                if tx.recipient == username:
                    expected_score += tx.amount
                    received_count += 1
                elif tx.sender == username:
                    expected_score -= tx.amount
                    sent_count += 1

        # 2. Transakcje z mempoola
        mempool_txs = MempoolTransactionMySQL.query.order_by(MempoolTransactionMySQL.id.asc()).all()
        for tx in mempool_txs:
            if tx.recipient == username:
                expected_score += tx.amount
                received_count += 1
            elif tx.sender == username:
                expected_score -= tx.amount
                sent_count += 1

        return {
            "score": expected_score,
            "sent_count": sent_count,
            "received_count": received_count
        }
