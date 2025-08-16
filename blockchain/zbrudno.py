import hashlib
import json
from datetime import datetime
from database.models import db, BlockchainBlockMySQL, BlockchainTransactionMySQL, MempoolTransactionMySQL


class BlockchainMYSQL:
    def __init__(self):
        self.hm_current_transactions = []  # transakcje do bieżącego bloku
        self.mempool = [
            {'sender': tx.sender, 'recipient': tx.recipient, 'amount': tx.amount, 'date': tx.date}
            for tx in MempoolTransactionMySQL.query.all()
        ]

        # Wczytanie ostatniego bloku z bazy
        last_block_db = BlockchainBlockMySQL.query.order_by(BlockchainBlockMySQL.index.desc()).first()
        if last_block_db:
            self.last_block = {
                'index': last_block_db.index,
                'timestamp': last_block_db.timestamp,
                'proof': last_block_db.proof,
                'previous_hash': last_block_db.previous_hash,
                'merkle_root': last_block_db.merkle_root,
                'hash': last_block_db.hash
            }
            print(f"MYSQL Last block loaded: index {self.last_block['index']}")
        else:
            # Genesis Block jeśli baza jest pusta
            genesis_block = self._create_block(hm_proof=100, hm_previous_hash='mentel')
            db_block = BlockchainBlockMySQL(
                index=genesis_block['index'],
                timestamp=genesis_block['timestamp'],
                proof=genesis_block['proof'],
                previous_hash=genesis_block['previous_hash'],
                merkle_root=genesis_block['merkle_root'],
                hash=self.hm_hash(genesis_block)
            )
            db.session.add(db_block)
            db.session.commit()
            self.last_block = genesis_block
            print("Genesis Block Created:", genesis_block)

    def hm_proof_of_work(self, hm_last_proof, block_hash):
        hm_proof = 0
        while not self.hm_valid_proof(hm_last_proof, hm_proof, block_hash):
            hm_proof += 1
        return hm_proof

    @staticmethod
    def hm_valid_proof(hm_last_proof, hm_proof, block_hash):
        hm_guess = f'{hm_last_proof}{hm_proof}{block_hash}'.encode()
        hm_guess_hash = hashlib.sha256(hm_guess).hexdigest()
        return hm_guess_hash[-3:] == "239"

    def _create_block(self, hm_proof, hm_previous_hash):
        """
        Tworzy nowy blok w pamięci (bez dodawania do bazy)
        """
        block_data = {
            'index': (self.last_block['index'] + 1) if hasattr(self, 'last_block') and self.last_block else 1,
            'timestamp': datetime.utcnow(),
            'transactions': self.hm_current_transactions,
            'proof': hm_proof,
            'previous_hash': hm_previous_hash,
            'merkle_root': self.create_merkle_root(self.hm_current_transactions)
        }
        # Czyścimy bieżące transakcje
        self.hm_current_transactions = []

        # Tworzymy hash i od razu dodajemy do bloku
        block_data['hash'] = self.hm_hash(block_data)
        return block_data

    # Dodaje transakcję do mempoola
    def hm_add_transaction_to_mempool(self, sender, recipient, amount, date):
        # Dodanie do bazy
        db_tx = MempoolTransactionMySQL(sender=sender, recipient=recipient, amount=amount, date=date)
        db.session.add(db_tx)
        db.session.commit()

        # Dodanie do lokalnej listy (opcjonalnie, jeśli chcesz szybki dostęp)
        self.mempool.append({'sender': sender, 'recipient': recipient, 'amount': amount, 'date': date})
        return len(self.mempool)

    def mine_if_ready(self, tx_limit=5):
        if len(self.mempool) >= tx_limit:
            # Pobieramy transakcje do bieżącego bloku
            pending_txs = MempoolTransactionMySQL.query.limit(tx_limit).all()

            self.hm_current_transactions = [
                {
                    'sender': tx.sender,
                    'recipient': tx.recipient,
                    'amount': tx.amount,
                    'date': tx.date
                }
                for tx in pending_txs
            ]

            # Tworzymy nowy blok w pamięci
            proof = self.hm_proof_of_work(self.last_block['proof'], self.last_block['hash'])
            block = self._create_block(proof, self.last_block['hash'])

            # Zapis do MySQL
            db_block = BlockchainBlockMySQL(
                index=block['index'],
                timestamp=block['timestamp'],
                proof=block['proof'],
                previous_hash=block['previous_hash'],
                merkle_root=block['merkle_root'],
                hash=block['hash'],
            )
            db.session.add(db_block)
            db.session.flush()  # aby mieć ID bloku

            # Zapis transakcji w bloku
            for tx in block['transactions']:
                db_tx = BlockchainTransactionMySQL(
                    block_id=db_block.id,
                    sender=tx['sender'],
                    recipient=tx['recipient'],
                    amount=tx['amount'],
                    date=tx['date']
                )
                db.session.add(db_tx)

            db.session.query(MempoolTransactionMySQL).delete()
            db.session.commit()

            # Usuwamy przeniesione transakcje z mempoola
            self.mempool = []

            # Aktualizujemy ostatni blok w pamięci (już z hash)
            self.last_block = block

    @staticmethod
    def hm_hash(data):
        return hashlib.sha256(json.dumps(data, sort_keys=True, default=str).encode()).hexdigest()

    def create_merkle_root(self, transactions):
        if not transactions:
            return None
        hashes = [self.hm_hash(tx) for tx in transactions]
        while len(hashes) > 1:
            if len(hashes) % 2 != 0:
                hashes.append(hashes[-1])
            new_hashes = []
            for i in range(0, len(hashes), 2):
                combined = hashes[i] + hashes[i+1]
                new_hashes.append(hashlib.sha256(combined.encode()).hexdigest())
            hashes = new_hashes
        return hashes[0]
