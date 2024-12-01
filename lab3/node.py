# node.py
from xmlrpc.server import SimpleXMLRPCServer
import json
import time
import logging
import threading
import argparse

class Node:
    def __init__(self, node_id, node_ip, port):
        self.node_id = node_id
        self.node_ip = node_ip
        self.port = port
        self.account_file = f"account_{node_id}.txt"
        self.transaction_log = []
        self.prepared_transactions = {}
        self.crash_scenario = None
        self.crash_point = None
        self.crash_times = 0
        
        # Initialize account values
        if node_id == 2:  # Node managing account A
            self.write_account_value(200)
        elif node_id == 3:  # Node managing account B
            self.write_account_value(300)
            
        self.setup_server()
    
    def setup_server(self):
        self.server = SimpleXMLRPCServer((self.node_ip, self.port), allow_none=True)
        self.server.register_function(self.prepare, "prepare")
        self.server.register_function(self.commit, "commit")
        self.server.register_function(self.abort, "abort")
        self.server.register_function(self.get_balance, "get_balance")
        self.server.register_function(self.reset_balance, "reset_balance")
        self.server.register_function(self.set_crash_scenario, "set_crash_scenario")
        self.server.register_function(self.check_if_committed, "check_if_committed")
        
    def set_crash_scenario(self, scenario, point=None):
        """Set crash scenario for testing"""
        self.crash_scenario = scenario
        self.crash_point = point
        print(f"Node {self.node_id}: Crash scenario set to {scenario} at point {point}")
        return True
    
    def simulate_crash(self):
        """Simulate node crash with timeout"""
        if self.crash_scenario:
            print(f"Node {self.node_id}: Simulating crash for 10 seconds")
            time.sleep(10)  # Sleep to simulate crash
            print(f"Node {self.node_id}: Recovered from crash")
            # self.recover()
            return True
        return False
    
    def recover(self):
        """Recovery procedure after crash"""
        print(f"Node {self.node_id}: Starting recovery procedure")
        # Check prepared transactions
        for tx_id, tx in self.prepared_transactions.items():
            if tx['state'] == 'PREPARED':
                print(f"Node {self.node_id}: Found prepared transaction {tx_id}")
                self.abort()  # Conservative approach: abort if unsure
                
        # Check transaction log
        for log_entry in reversed(self.transaction_log):
            if log_entry['state'] == 'PREPARED':
                self.abort()
            elif log_entry['state'] == 'COMMITTED':
                self.apply_transaction(log_entry['transaction'])
    
    def prepare(self, transaction):
        """Phase 1: Prepare"""
        print(f"Node {self.node_id}: Received prepare for transaction {transaction}")
        
        # Simulate crash before responding if specified
        if self.node_id == 2 and self.crash_scenario == 'c1' and self.crash_point == 'before':
            self.simulate_crash()
            return False
            
        if self.validate_transaction(transaction):
            tx_id = str(time.time())
            self.prepared_transactions[tx_id] = {
                'transaction': transaction,
                'state': 'PREPARED'
            }
            self.log_transaction(transaction, 'PREPARED')
                
            return True
        return False
        
    def commit(self, transaction):
        """Phase 2: Commit"""
        # Simulate crash after responding if specified
        if self.node_id == 2 and self.crash_scenario == 'c2' and self.crash_point == 'after' and self.crash_times < 1:
            self.crash_times += 1
            self.simulate_crash()
            return False

        print(f"Node {self.node_id}: Received commit for transaction")
        success = self.apply_transaction(transaction)
        if success:
            self.log_transaction(transaction, 'COMMITTED')
            self.prepared_transactions = {}  # Clear prepared state
        return success
    
    def abort(self):
        """Abort transaction"""
        print(f"Node {self.node_id}: Aborting transaction")
        self.prepared_transactions = {}
        return True
    
    def apply_transaction(self, transaction):
        """Apply transaction changes"""
        try:
            balance = self.read_account_value()
            
            if transaction['type'] == 'transfer':
                if self.node_id == 2:  # Account A
                    new_balance = balance - transaction['amount']
                else:  # Account B
                    new_balance = balance + transaction['amount']
            elif transaction['type'] == 'bonus':
                new_balance = balance + transaction['bonus_amount']
                
            self.write_account_value(new_balance)
            print(f"Node {self.node_id}: Applied transaction, new balance: {new_balance}")
            return True
        except Exception as e:
            print(f"Node {self.node_id}: Failed to apply transaction: {e}")
            return False
    
    def log_transaction(self, transaction, state):
        """Log transaction state"""
        log_entry = {
            'transaction': transaction,
            'state': state,
            'timestamp': time.time()
        }
        self.transaction_log.append(log_entry)

    def check_if_committed(self, transaction_id):
        
        last_transaction = self.transaction_log[-1]
        if last_transaction['transaction']['transaction_id'] == transaction_id:
            if last_transaction['state'] == 'COMMITTED':
                return True
            else:
                return False
        else:
            return False
        
    def validate_transaction(self, transaction):
        """Validate transaction based on business rules"""
        balance = self.read_account_value()
        
        if transaction['type'] == 'transfer':
            if self.node_id == 2:  # Account A
                if balance < transaction['amount']:
                    print(f"Node {self.node_id}: Insufficient funds. Balance: {balance}, Required: {transaction['amount']}")
                    return False
            return True
        elif transaction['type'] == 'bonus':
            return True
            
        return False

    def write_account_value(self, value):
        with open(self.account_file, 'w') as f:
            f.write(str(value))
            
    def read_account_value(self):
        try:
            with open(self.account_file, 'r') as f:
                return float(f.read().strip())
        except FileNotFoundError:
            return 0.0
            
    def get_balance(self):
        return self.read_account_value()
    
    def reset_balance(self, scenario):
        """Reset balance for test scenario"""
        if self.node_id == 2:  # Account A
            if scenario == 'a' or scenario == 'c':
                self.write_account_value(200)
            elif scenario == 'b':
                self.write_account_value(90)
        elif self.node_id == 3:  # Account B
            if scenario == 'a' or scenario == 'c':
                self.write_account_value(300)
            elif scenario == 'b':
                self.write_account_value(50)
        self.crash_scenario = None
        self.crash_point = None
        self.transaction_log = []
        self.prepared_transactions = {}
        self.crash_times = 0
        return True
    
    def run(self):
        print(f"Node {self.node_id} running on port {self.port}")
        self.server.serve_forever()

def main(args):
    node = Node(args.id, args.ip, args.port)
    node.run()

if __name__== "__main__":
    parser = argparse.ArgumentParser(description="CISC-6935 lab-3 node")
    parser.add_argument("--ip", type=str, default='localhost', help="IP address")
    parser.add_argument("--port", type=int, default=8001, help="Port number")
    parser.add_argument("--id", type=int, default=1, help="Node ID")
    parser.add_argument("--is_coord",action="store_true",default=False, help="If this node is coordinator")
    args = parser.parse_args()
    main(args)