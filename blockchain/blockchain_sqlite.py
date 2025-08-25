from database.models import db, BlockchainBlockSQLite, BlockchainTransactionSQLite, MempoolTransactionSQLite
from blockchain.blockchain_base import BlockchainBase


class BlockchainSQLite(BlockchainBase):
    def get_last_block_from_db(self):
        # Pobranie ostatniego bloku z SQLite
        last_block_db = BlockchainBlockSQLite.query.order_by(BlockchainBlockSQLite.index.desc()).first()

        if not last_block_db:
            return None

        print(f"SQLite Last block loaded: index {last_block_db.index}")

        # Pobranie transakcji powiązanych z tym blokiem
        transactions = BlockchainTransactionSQLite.query.filter_by(block_id=last_block_db.id).order_by(
            BlockchainTransactionSQLite.id).all()

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
        db_block = BlockchainBlockSQLite(
            index=block['index'],
            timestamp=block['timestamp'],
            proof=block['proof'],
            previous_hash=block['previous_hash'],
            merkle_root=block['merkle_root'],
            hash=block['hash'],
        )
        db.session.add(db_block)
        db.session.flush()

        for tx in transactions:
            db_tx = BlockchainTransactionSQLite(
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

        db_objects = [MempoolTransactionSQLite(**tx) for tx in transactions]
        db.session.add_all(db_objects)
        db.session.commit()

    # --- Nowe metody wymagane przez BlockchainBase ---
    def get_pending_transactions(self, limit):
        txs = MempoolTransactionSQLite.query.order_by(MempoolTransactionSQLite.date.asc()).limit(limit).all()
        return [{'id': tx.id, 'sender': tx.sender, 'recipient': tx.recipient, 'amount': tx.amount, 'date': tx.date} for tx in txs]

    def get_mempool_count(self):
        return MempoolTransactionSQLite.query.count()

    def clear_pending_transactions(self, transactions):
        if not transactions:
            return
        ids = [tx['id'] for tx in transactions]
        MempoolTransactionSQLite.query.filter(MempoolTransactionSQLite.id.in_(ids)).delete(synchronize_session=False)
        db.session.commit()

    def get_full_chain(self) -> list[dict]:
        """Zwraca cały blockchain z MySQL w kolejności rosnącej po index,
           wraz z transakcjami przypisanymi do każdego bloku"""

        blocks = BlockchainBlockSQLite.query.order_by(
            BlockchainBlockSQLite.index.asc()
        ).all()

        chain = []
        for block in blocks:
            # Pobierz transakcje powiązane z tym blokiem
            txs = BlockchainTransactionSQLite.query.filter_by(
                block_id=block.id
            ).order_by(
                BlockchainTransactionSQLite.id.asc()
            ).all()

            chain.append({
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
                    for tx in txs
                ],
                'proof': block.proof,
                'previous_hash': block.previous_hash,
                'merkle_root': block.merkle_root
            })

        return chain
