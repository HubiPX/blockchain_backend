from database.models import db, BlockchainBlockSQLite, BlockchainTransactionSQLite, MempoolTransactionSQLite
from blockchain.blockchain_base import BlockchainBase


class BlockchainSQLite(BlockchainBase):
    def get_mempool_from_db(self):
        return [
            {'sender': tx.sender, 'recipient': tx.recipient, 'amount': tx.amount, 'date': tx.date}
            for tx in MempoolTransactionSQLite.query.all()
        ]

    def get_last_block_from_db(self):
        last_block_db = BlockchainBlockSQLite.query.order_by(BlockchainBlockSQLite.index.desc()).first()
        if last_block_db:
            print(f"SQLite Last block loaded: index {last_block_db.index}")
            return {
                'index': last_block_db.index,
                'timestamp': last_block_db.timestamp,
                'proof': last_block_db.proof,
                'previous_hash': last_block_db.previous_hash,
                'merkle_root': last_block_db.merkle_root,
                'hash': last_block_db.hash
            }
        return None

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

    def save_transaction_to_mempool(self, sender, recipient, amount, date):
        db_tx = MempoolTransactionSQLite(sender=sender, recipient=recipient, amount=amount, date=date)
        db.session.add(db_tx)
        db.session.commit()

    def clear_mempool(self):
        db.session.query(MempoolTransactionSQLite).delete()
        db.session.commit()
