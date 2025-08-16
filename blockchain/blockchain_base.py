from abc import ABC, abstractmethod
import hashlib, json
from datetime import datetime


class BlockchainBase(ABC):
    def __init__(self):
        self.hm_current_transactions = []
        self.mempool = self.get_mempool_from_db()
        # Zmienione: najpierw pobieramy ostatni blok, potem tworzymy genesis block jeśli go brak
        self.last_block = self.get_last_block_from_db()
        if not self.last_block:  # Dodane: tworzymy genesis block tylko jeśli brak ostatniego bloku
            self.last_block = self._create_genesis_block()

    @abstractmethod
    def get_mempool_from_db(self):
        pass

    @abstractmethod
    def get_last_block_from_db(self):
        pass

    @abstractmethod
    def save_block_to_db(self, block, transactions):
        pass

    @abstractmethod
    def save_transaction_to_mempool(self, sender, recipient, amount, date):
        pass

    @abstractmethod
    def clear_mempool(self):
        pass

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
        block_index = self.last_block['index'] + 1 if self.last_block else 1
        block_data = {
            'index': block_index,
            'timestamp': datetime.utcnow(),
            'transactions': self.hm_current_transactions,
            'proof': hm_proof,
            'previous_hash': hm_previous_hash,
            'merkle_root': self.create_merkle_root(self.hm_current_transactions)
        }
        self.hm_current_transactions = []
        block_data['hash'] = self.hm_hash(block_data)
        return block_data

    def _create_genesis_block(self):
        block = self._create_block(hm_proof=100, hm_previous_hash='mentel')
        self.save_block_to_db(block, [])
        return block

    def hm_add_transaction_to_mempool(self, transactions, tx_limit=5):
        # Obsługa: pojedyncza transakcja albo lista transakcji
        if not isinstance(transactions, list):
            transactions = [transactions]

        for tx in transactions:
            sender = tx['sender']
            recipient = tx['recipient']
            amount = tx['amount']
            date = tx['date']

            # Zapis do DB mempool
            self.save_transaction_to_mempool(sender, recipient, amount, date)

            # Dodanie do mempoola w pamięci
            self.mempool.append({
                'sender': sender,
                'recipient': recipient,
                'amount': amount,
                'date': date
            })

            # --- sprawdzanie, czy można wykopać blok ---
            while len(self.mempool) >= tx_limit:
                # Bierzemy dokładnie tx_limit transakcji
                pending_txs = self.mempool[:tx_limit]
                self.hm_current_transactions = pending_txs

                # Proof of Work
                proof = self.hm_proof_of_work(self.last_block['proof'], self.last_block['hash'])
                block = self._create_block(proof, self.last_block['hash'])

                # Zapis bloku + transakcji do DB
                self.save_block_to_db(block, pending_txs)

                # Aktualizacja stanu
                self.last_block = block

                # Usuwamy zużyte transakcje z mempoola (pamięć + DB)
                self.mempool = self.mempool[tx_limit:]
                self.clear_mempool()
                for leftover in self.mempool:
                    self.save_transaction_to_mempool(
                        leftover['sender'], leftover['recipient'], leftover['amount'], leftover['date']
                    )

        return len(self.mempool)

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
            hashes = [hashlib.sha256((hashes[i] + hashes[i+1]).encode()).hexdigest()
                      for i in range(0, len(hashes), 2)]
        return hashes[0]
