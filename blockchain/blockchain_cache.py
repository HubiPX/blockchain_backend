import hashlib
import json
from time import time


class Blockchain:
    def __init__(self):
        self.hm_current_transactions = []
        self.hm_chain = []
        self.mempool = []

        print("Tworzenie bloku genesis...")
        genesis_block = self.hm_new_block(hm_proof=63955, hm_previous_hash='mentel')
        print("Genesis Block:", genesis_block)
        print("Genesis Hash:", self.hm_hash(genesis_block))

    def hm_proof_of_work(self, hm_last_proof):
        hm_proof = 0
        while not self.hm_valid_proof(hm_last_proof, hm_proof):
            hm_proof += 1
        return hm_proof

    @staticmethod
    def hm_valid_proof(hm_last_proof, hm_proof):
        hm_guess = f'{hm_last_proof}{hm_proof}'.encode()
        hm_guess_hash = hashlib.sha256(hm_guess).hexdigest()
        return hm_guess_hash[-2:] == "07"

    def hm_new_block(self, hm_proof, hm_previous_hash=None):
        merkle_root = create_merkle_root(self.hm_current_transactions)
        hm_block = {
            'index': len(self.hm_chain) + 1,
            'timestamp': time(),
            'transactions': self.hm_current_transactions,
            'proof': hm_proof,
            'previous_hash': hm_previous_hash or self.hm_hash(self.hm_chain[-1]),
            'merkle_root': merkle_root
        }

        self.hm_current_transactions = []
        self.hm_chain.append(hm_block)
        return hm_block

    def hm_new_transaction(self, hm_sender, hm_receiver, hm_amount):
        self.hm_current_transactions.append({
            'sender': hm_sender,
            'receiver': hm_receiver,
            'amount': hm_amount
        })
        return self.hm_last_block['index'] + 1

    def hm_add_transaction_to_mempool(self, sender, receiver, amount):
        if amount <= 0:
            raise ValueError("Transaction amount must be greater than 0.")

        if sender != '0':
            balance = self.get_balance_of_address(sender)
            if balance < amount:
                raise ValueError(
                    f"Sender {sender} has insufficient balance. Available: {balance}, Tried to send: {amount}"
                )

        transaction = {
            'sender': sender,
            'receiver': receiver,
            'amount': amount,
            'status': 'pending'
        }
        self.mempool.append(transaction)
        return len(self.mempool)

    def get_balance_of_address(self, address):
        balance = 0
        for block in self.hm_chain:
            for tx in block['transactions']:
                if tx['receiver'] == address:
                    balance += tx['amount']
                elif tx['sender'] == address:
                    balance -= tx['amount']

        for tx in self.mempool:
            if tx['status'] == 'pending':
                if tx['receiver'] == address:
                    balance += tx['amount']
                elif tx['sender'] == address:
                    balance -= tx['amount']
        return balance

    @staticmethod
    def hm_hash(hm_block):
        hm_block_string = json.dumps(hm_block, sort_keys=True).encode()
        return hashlib.sha256(hm_block_string).hexdigest()

    @property
    def hm_last_block(self):
        return self.hm_chain[-1]

    def mine_block(self, reward_address, reward_amount=63955):
        last_block = self.hm_last_block
        proof = self.hm_proof_of_work(last_block['proof'])

        MAX_TRANSACTIONS_PER_BLOCK = 100
        pending_txs = [tx for tx in self.mempool if tx['status'] == 'pending']
        transactions_to_mine = pending_txs[:MAX_TRANSACTIONS_PER_BLOCK]

        for tx in transactions_to_mine:
            tx['status'] = 'mined'

        self.hm_current_transactions = [
            {
                'sender': tx['sender'],
                'receiver': tx['receiver'],
                'amount': tx['amount']
            } for tx in transactions_to_mine
        ]

        # Nagroda dla górnika
        self.hm_current_transactions.append({
            'sender': '0',
            'receiver': reward_address,
            'amount': reward_amount
        })

        return self.hm_new_block(proof, self.hm_hash(last_block))

    def validate_chain(self):
        chain = self.hm_chain
        for i in range(1, len(chain)):
            current_block = chain[i]
            previous_block = chain[i - 1]

            if current_block['previous_hash'] != self.hm_hash(previous_block):
                return False, f"Invalid previous hash at block {current_block['index']}"

            if not self.hm_valid_proof(previous_block['proof'], current_block['proof']):
                return False, f"Invalid proof at block {current_block['index']}"

        return True, "Blockchain is valid."


def hm_hash_transaction(hm_transaction):
    hm_transaction_string = json.dumps(hm_transaction, sort_keys=True).encode()
    return hashlib.sha256(hm_transaction_string).hexdigest()


def create_merkle_root(hm_transaction):
    if len(hm_transaction) == 0:
        return None

    hashed_transactions = [hm_hash_transaction(tx) for tx in hm_transaction]

    while len(hashed_transactions) > 1:
        if len(hashed_transactions) % 2 != 0:
            hashed_transactions.append(hashed_transactions[-1])

        new_level = []
        for i in range(0, len(hashed_transactions), 2):
            combined = hashed_transactions[i] + hashed_transactions[i + 1]
            new_hash = hashlib.sha256(combined.encode()).hexdigest()
            new_level.append(new_hash)

        hashed_transactions = new_level

    return hashed_transactions[0]
