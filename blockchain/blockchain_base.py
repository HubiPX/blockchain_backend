import hashlib
import json
from abc import ABC, abstractmethod
from datetime import datetime


class BlockchainBase(ABC):
    def __init__(self):
        self.hm_current_transactions = []
        self.last_block = self.get_last_block_from_db()
        if not self.last_block:
            self.last_block = self._create_genesis_block()

    @abstractmethod
    def get_last_block_from_db(self):
        pass

    @abstractmethod
    def save_block_to_db(self, block, transactions):
        pass

    @abstractmethod
    def save_transactions_to_mempool(self, transactions: list[dict]):
        pass

    @abstractmethod
    def get_pending_transactions(self, limit):
        """Pobiera określoną liczbę transakcji z mempoola"""
        pass

    @abstractmethod
    def get_mempool_count(self):
        """Zwraca liczbę transakcji w mempoolu"""
        pass

    @abstractmethod
    def clear_pending_transactions(self, transactions):
        """Usuwa określone transakcje z mempoola w DB"""
        pass

    @abstractmethod
    def get_full_chain(self) -> list[dict]:
        """Zwraca cały blockchain z bazy w kolejności od genesis do ostatniego"""
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

    def _mine_block(self, tx_limit):
        """Pomocnicza metoda - kopie blok, gdy mempool >= tx_limit"""
        pending_txs = self.get_pending_transactions(tx_limit)
        self.hm_current_transactions = pending_txs

        proof = self.hm_proof_of_work(self.last_block['proof'], self.last_block['hash'])
        block = self._create_block(proof, self.last_block['hash'])

        self.save_block_to_db(block, pending_txs)
        self.last_block = block

        self.clear_pending_transactions(pending_txs)

    def hm_add_transaction_to_mempool(self, transactions, tx_limit=5):
        if not isinstance(transactions, list):
            transactions = [transactions]

        space_left = tx_limit - self.get_mempool_count()

        # --- przypadek 1: transakcji jest idealnie by wypełnić blok ---
        if space_left == len(transactions):
            self.save_transactions_to_mempool(transactions)
            self._mine_block(tx_limit)

        # --- przypadek 2: mieści się wszystko, ale nie wypełnia bloku ---
        elif space_left > len(transactions):
            self.save_transactions_to_mempool(transactions)

        # --- przypadek 3: transakcji jest więcej niż miejsca w bloku ---
        elif space_left < len(transactions):
            # najpierw dodajemy brakujące do pełnego bloku i tworzymy blok
            first_batch = transactions[:space_left]
            self.save_transactions_to_mempool(first_batch)
            self._mine_block(tx_limit)

            x = (len(transactions) - space_left) // tx_limit
            y = (len(transactions) - space_left) / tx_limit

            for batch_nr in range(x):
                batch = transactions[(batch_nr * tx_limit + space_left):(batch_nr + 1) * tx_limit + space_left]
                self.save_transactions_to_mempool(batch)
                self._mine_block(tx_limit)
            if x != y:
                batch = transactions[(x * tx_limit + space_left):(x + 1) * tx_limit + space_left]
                self.save_transactions_to_mempool(batch)

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
