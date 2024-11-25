import socket
import json
import time
import threading
import argparse
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy

class Node:
    def __init__(self, node_id, port, is_coordinator=False):
        self.node_id = node_id
        self.port = port
        self.is_coordinator = is_coordinator
        self.account_file = f"account_{node_id}.txt"
        self.nodes = {
            1: ("localhost", 8001),
            2: ("localhost", 8002),
            3: ("localhost", 8003)
        }
        self.voted = False
        self.prepared = False
        self.crash_scenario = None
        self.crash_point = None
        
        # Initialize account values
        if node_id == 2:  # Node managing account A
            self.write_account_value(200)
        elif node_id == 3:  # Node managing account B
            self.write_account_value(300)
            
        # Start RPC server
        self.server = SimpleXMLRPCServer(("localhost", port), allow_none=True)
        self.server.register_function(self.prepare, "prepare")
        self.server.register_function(self.commit, "commit")
        self.server.register_function(self.abort, "abort")
        self.server.register_function(self.get_balance, "get_balance")
        self.server.register_function(self.reset_balance, "reset_balance")
        self.server.register_function(self.set_crash_scenario, "set_crash_scenario")
    
    def set_crash_scenario(self, scenario, point=None):
        """Set the crash scenario for this node"""
        self.crash_scenario = scenario
        self.crash_point = point
        print(f"Node {self.node_id}: Crash scenario set to {scenario} at point {point}")
        return True
    
    def simulate_crash(self):
        """Simulate a node crash by sleeping"""
        if self.crash_scenario:
            print(f"Node {self.node_id}: Simulating crash for {30} seconds")
            time.sleep(30)  # Simulate crash for 30 seconds

    def reset_balance(self, scenario):
        """Reset account balance based on test scenario"""
        if self.node_id == 2:  # Node managing account A
            if scenario == 'a' or scenario == 'c':
                self.write_account_value(200)
            elif scenario == 'b':
                self.write_account_value(90)
        elif self.node_id == 3:  # Node managing account B
            if scenario == 'a' or scenario == 'c':
                self.write_account_value(300)
            elif scenario == 'b':
                self.write_account_value(50)
        # Reset crash scenario when resetting balances
        self.crash_scenario = None
        self.crash_point = None
        return True
    
    def write_account_value(self, value):
        with open(self.account_file, 'w') as f:
            f.write(str(value))
            
    def read_account_value(self):
        try:
            with open(self.account_file, 'r') as f:
                return float(f.read().strip())
        except FileNotFoundError:
            return 0.0
            
    def validate_transaction(self, transaction):
        balance = self.read_account_value()
        
        if transaction['type'] == 'transfer':
            if self.node_id == 2:  # Account A
                return balance >= transaction['amount']
            return True
        elif transaction['type'] == 'bonus':
            return True
            
        return False
        
    def prepare(self, transaction):
        print(f"Node {self.node_id}: Received prepare for transaction {transaction}")
        # Simulate crash before responding if scenario is 'c1' and this is node 2
        if self.node_id == 2 and self.crash_scenario == 'c1' and self.crash_point == 'before':
            self.simulate_crash()
            return False
            
        if self.validate_transaction(transaction):
            self.prepared = True
            self.voted = True
            
            # Simulate crash after responding if scenario is 'c2' and this is node 2
            if self.node_id == 2 and self.crash_scenario == 'c2' and self.crash_point == 'after':
                self.simulate_crash()
                
            return True
        return False
        
    def commit(self, transaction):
        if not self.prepared:
            return False
            
        balance = self.read_account_value()
        
        if transaction['type'] == 'transfer':
            if self.node_id == 2:  # Account A
                new_balance = balance - transaction['amount']
            else:  # Account B
                new_balance = balance + transaction['amount']
        elif transaction['type'] == 'bonus':
            if self.node_id == 2:  # Account A
                bonus = balance * 0.2
                new_balance = balance + bonus
            else:  # Account B
                account_a = ServerProxy(f"http://{self.nodes[2][0]}:{self.nodes[2][1]}")
                a_balance = account_a.get_balance()
                bonus = a_balance * 0.2
                new_balance = balance + bonus
                
        self.write_account_value(new_balance)
        self.prepared = False
        return True
        
    def abort(self):
        self.prepared = False
        return True
        
    def get_balance(self):
        return self.read_account_value()
        
    def run(self):
        print(f"Node {self.node_id} running on port {self.port}")
        self.server.serve_forever()

def main(args):
    node = Node(args.id, args.port, is_coordinator=args.is_coord)
    node.run()

if __name__== "__main__":
    parser = argparse.ArgumentParser(description="CISC-6935 lab-1 server")
    parser.add_argument("--ip", type=str, default='localhost', help="IP address")
    parser.add_argument("--port", type=int, default=8001, help="Port number")
    parser.add_argument("--id", type=int, default=1, help="Node ID")
    parser.add_argument("--is_coord",action="store_true",default=False, help="If this node is coordinator")
    args = parser.parse_args()
    main(args)

    

