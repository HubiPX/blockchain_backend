from database.models import db, BlockchainBlockSQLite, BlockchainTransactionSQLite, MempoolTransactionSQLite
from blockchain.blockchain_base import BlockchainBase
from collections import defaultdict


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
        if not transactions:
            transactions = []

        db_block = BlockchainBlockSQLite(
            index=block['index'],
            timestamp=block['timestamp'],
            proof=block['proof'],
            previous_hash=block['previous_hash'],
            merkle_root=block['merkle_root']
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

    def get_chain_batch(self, offset: int, limit: int) -> list[dict]:
        """Pobiera fragment blockchaina z bazy (paginacja)."""
        # Pobranie bloków
        blocks = (BlockchainBlockSQLite.query
                  .order_by(BlockchainBlockSQLite.index.asc())
                  .offset(offset)
                  .limit(limit)
                  .all())

        if not blocks:
            return []

        # Pobranie wszystkich transakcji dla tych bloków naraz
        block_ids = [block.id for block in blocks]
        all_txs = (BlockchainTransactionSQLite.query
                   .filter(BlockchainTransactionSQLite.block_id.in_(block_ids))
                   .order_by(BlockchainTransactionSQLite.id.asc())
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
        block = BlockchainBlockSQLite.query.filter_by(index=block_index).first()
        if not block:
            return None

        txs = BlockchainTransactionSQLite.query.filter_by(block_id=block.id).order_by(
            BlockchainTransactionSQLite.id.asc()).all()
        transactions = [
            {"id": tx.id, "sender": tx.sender, "recipient": tx.recipient, "amount": tx.amount, "date": tx.date}
            for tx in txs
        ]

        tx_index = next((i for i, tx in enumerate(transactions) if tx["id"] == int(tx_id)), None)
        if tx_index is None:
            return None

        proof = self.get_merkle_proof(transactions, tx_index)
        return {
            "transaction": transactions[tx_index],
            "proof": proof,
            "merkle_root": block.merkle_root
        }
