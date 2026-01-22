import hashlib
import json
from abc import ABC, abstractmethod
from datetime import datetime
import time


class BlockchainBase(ABC):
    TARGET = 2 ** 244

    def __init__(self):
        self.current_transactions = []
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
    def get_chain_batch(self, offset, limit) -> list[dict]:
        """Zwraca ustaloną ilość bloków z bazy sortując po index"""
        pass

    def proof_of_work(self, last_proof, block_hash):
        proof = 0
        while not self.valid_proof(
            last_proof,
            proof,
            block_hash,
            self.TARGET
        ):
            proof += 1
        return proof

    @staticmethod
    def valid_proof(last_proof, proof, block_hash, target):
        guess = f'{last_proof}{proof}{block_hash}'.encode()
        guess_hash = hashlib.sha256(guess).hexdigest()
        guess_int = int(guess_hash, 16)
        return guess_int < target

    def _create_block(self, proof, previous_hash):
        block_index = self.last_block['index'] + 1 if self.last_block else 1

        block_data = {
            'index': block_index,
            'timestamp': datetime.now().replace(microsecond=(datetime.now().microsecond // 1000) * 1000),
            'transactions': self.current_transactions,
            'proof': proof,
            'previous_hash': previous_hash,
            'merkle_root': self.create_merkle_root(self.current_transactions)
        }
        self.current_transactions = []

        return block_data

    def _create_genesis_block(self):
        block = self._create_block(proof=100, previous_hash='mentel')
        self.save_block_to_db(block, [])
        return block

    def _mine_block(self, tx_limit):
        """Pomocnicza metoda — kopie blok, gdy mempool >= tx_limit"""
        pending_txs = self.get_pending_transactions(tx_limit)

        if '_id' in pending_txs[0]:
            pending_txs.sort(key=lambda tx: tx['_id'])
        else:
            pending_txs.sort(key=lambda tx: tx['id'])

        self.current_transactions = pending_txs

        proof = self.proof_of_work(self.last_block['proof'], self.hash(self.last_block))
        block = self._create_block(proof, self.hash(self.last_block))

        self.save_block_to_db(block, pending_txs)
        self.last_block = block

        self.clear_pending_transactions(pending_txs)

    def add_transaction_to_mempool(self, transactions, tx_limit):
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

    def validate_chain(self, batch_size: int = 1000):
        start_time = time.perf_counter()
        last_block = None
        offset = 0
        highest_index = 0  # zapamiętujemy największy index

        while True:
            batch = self.get_chain_batch(offset, batch_size)
            if not batch:
                break

            for i, current_block in enumerate(batch):
                if last_block:
                    previous_block = last_block
                elif i > 0:
                    previous_block = batch[i - 1]
                else:
                    previous_block = None

                if previous_block:
                    # sprawdź hash poprzedniego bloku
                    previous_hash = self.hash(previous_block)
                    if current_block['previous_hash'] != previous_hash:
                        return False, f"Nieprawidłowy poprzedni hash w bloku {current_block['index']}."

                    # sprawdź proof-of-work
                    if not self.valid_proof(previous_block['proof'], current_block['proof'], previous_hash, self.TARGET):
                        return False, f"Nieprawidłowy dowód w bloku {current_block['index']}."

                last_block = current_block
                highest_index = current_block['index']  # aktualizuj

            offset += batch_size

        end_time = time.perf_counter() - start_time

        return True, f"Blockchain jest poprawny. {highest_index} bloków. {round(end_time, 3)}s"

    @staticmethod
    def hash(data):
        return hashlib.sha256(json.dumps(data, sort_keys=True, default=str).encode()).hexdigest()

    def create_merkle_root(self, transactions):
        if not transactions:
            return None
        hashes = [self.hash(tx) for tx in transactions]
        while len(hashes) > 1:
            if len(hashes) % 2 != 0:
                hashes.append(hashes[-1])
            hashes = [hashlib.sha256((hashes[i] + hashes[i+1]).encode()).hexdigest()
                      for i in range(0, len(hashes), 2)]
        return hashes[0]

    def get_merkle_proof(self, transactions: list[dict], tx_index: int) -> list[dict]:
        """
        Zwraca Merkle Proof dla transakcji o danym indeksie w bloku.
        Proof to lista słowników: { 'position': 'left'/'right', 'hash': <string> }
        """
        if not transactions:
            return []

        # Oblicz hash każdej transakcji
        hashes = [self.hash(tx) for tx in transactions]
        index = tx_index
        proof = []

        while len(hashes) > 1:
            if len(hashes) % 2 != 0:
                hashes.append(hashes[-1])

            new_hashes = []
            for i in range(0, len(hashes), 2):
                left, right = hashes[i], hashes[i+1]

                # jeśli nasza transakcja była w tej parze → zapisz sąsiada do ścieżki
                if i == index or i+1 == index:
                    if index == i:  # nasz hash był po lewej
                        proof.append({"position": "right", "hash": right})
                    else:  # nasz hash był po prawej
                        proof.append({"position": "left", "hash": left})
                    index = len(new_hashes)

                new_hashes.append(hashlib.sha256((left + right).encode()).hexdigest())

            hashes = new_hashes

        return proof

    def verify_merkle_proof(self, transaction: dict, proof: list[dict], merkle_root: str) -> bool:
        """
        Weryfikuje dowód Merkle Proof dla podanej transakcji.
        """
        current_hash = self.hash(transaction)

        for step in proof:
            if step["position"] == "left":
                current_hash = hashlib.sha256((step["hash"] + current_hash).encode()).hexdigest()
            else:  # right
                current_hash = hashlib.sha256((current_hash + step["hash"]).encode()).hexdigest()

        return current_hash == merkle_root
