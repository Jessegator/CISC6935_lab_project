# coordinator.py
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy, Fault
import threading
import logging
import time
import socket
import argparse

TIMEOUT = 5  # Timeout in seconds for node responses

class Coordinator:
    def __init__(self, node_ip, port=8001):
        self.port = port
        self.node_ip = node_ip
        # Modify the ip addressof participants correspondingly
        self.nodes = {
            2: ServerProxy(f"10.128.0.10:8002", allow_none=True),
            3: ServerProxy(f"10.128.0.11:8003", allow_none=True)
        }
        self.transaction_log = []
        self.setup_server()
        
    def setup_server(self):
        self.server = SimpleXMLRPCServer((self.node_ip, self.port), allow_none=True)
        self.server.register_function(self.execute_transaction, "execute_transaction")
        self.server.register_function(self.get_balances, "get_balances")
        self.server.register_function(self.reset_scenario, "reset_scenario")
        
    def execute_transaction(self, transaction, crash_scenario=None):
        """Execute 2PC protocol with timeout and failure handling"""
        transaction_id = str(time.time())
        transaction['transaction_id'] = transaction_id
        print(f"\nCoordinator: Starting transaction {transaction_id}")

        if transaction['type'] == 'bonus':
            try:
                a_balance = self.nodes[2].get_balance()
                bonus_amount = a_balance * 0.2
                transaction['bonus_amount'] = bonus_amount
                print(f"Coordinator: Calculated bonus amount: {bonus_amount} (20% of A's balance: {a_balance})")
            except Exception as e:
                print(f"Coordinator: Error calculating bonus: {e}")
                return {
                    'status': 'error',
                    'message': 'Failed to calculate bonus amount'
                }
        
        # Phase 1: Prepare
        prepare_responses = {}
        prepare_timeout = False
        
        for node_id, node in self.nodes.items():
            try:
                socket.setdefaulttimeout(TIMEOUT)
                print(f"Coordinator: Sending prepare to Node-{node_id}")
                prepared = node.prepare(transaction)
                prepare_responses[node_id] = prepared
                print(f"Coordinator: Node-{node_id} prepare response: {prepared}")
                    
            except socket.timeout:
                print(f"Coordinator: Timeout waiting for Node-{node_id} prepare response")
                prepare_timeout = True
                prepare_responses[node_id] = False
            except Exception as e:
                print(f"Coordinator: Error from Node-{node_id} during prepare: {e}")
                prepare_responses[node_id] = False
                
        # Handle timeout/failures
        if prepare_timeout:
            for node_id, response in prepare_responses.items():
                if not response:
                    print(f"Coordinator: Waiting for Node-{node_id} recovery...")
                    if self.wait_for_node_recovery(node_id):
                        try:
                            prepared = self.nodes[node_id].prepare(transaction)
                            prepare_responses[node_id] = prepared
                        except Exception:
                            prepare_responses[node_id] = False

        # Check prepare responses
        if all(prepare_responses.values()):
            print("Coordinator: All nodes prepared successfully")
            return self.commit_phase(transaction_id, transaction, crash_scenario)
            # self.commit_phase(transaction_id, transaction)
            
        
        else:
            print("Coordinator: Some nodes failed to prepare, initiating abort")
            self.abort_transaction(transaction_id)
            failed_nodes = [nid for nid, resp in prepare_responses.items() if not resp]
            return {
                'status': 'error',
                'message': f'Transaction aborted: Nodes {failed_nodes} failed to prepare'
            }
            
    def commit_phase(self, transaction_id, transaction, crash_scenario=None):
        """Execute commit phase with failure handling"""
        commit_responses = {}
        commit_timeout = False
        
        print("\nCoordinator: Starting commit phase")
        for node_id, node in self.nodes.items():
            try:
                socket.setdefaulttimeout(TIMEOUT)
                print(f"Coordinator: Sending commit to Node-{node_id}")
                committed = node.commit(transaction)
                commit_responses[node_id] = committed
                if not crash_scenario == 'c3':
                    print(f"Coordinator: Node-{node_id} commit response: {committed}")
            except socket.timeout:
                print(f"Coordinator: Timeout waiting for Node-{node_id} commit response")
                commit_timeout = True
                commit_responses[node_id] = False
            except Exception as e:
                print(f"Coordinator: Error from Node-{node_id} during commit: {e}")
                commit_responses[node_id] = False
                
        # Handle timeout/failures
        if commit_timeout:
            for node_id, response in commit_responses.items():
                if not response:
                    print(f"Coordinator: Waiting for Node-{node_id} recovery...")
                    if self.wait_for_node_recovery(node_id):
                        try:
                            committed = self.nodes[node_id].commit(transaction)
                            commit_responses[node_id] = committed
                        except Exception:
                            commit_responses[node_id] = False


        # Simulate coordinator crash after DoCommit
        if crash_scenario == 'c3':
            commit_responses = {}
            print("\nCoordinator: Simulating crash after DoCommit")
            time.sleep(10)  # Sleep to simulate crash
            print("Coordinator: Recovered from crash")
            for node_id, node in self.nodes.items():
                committed = node.check_if_committed(transaction_id)
                commit_responses[node_id] = committed
            # return self.recover_transaction(transaction_id, transaction)   


        if all(commit_responses.values()):
            print("Coordinator: All nodes committed successfully")
            return {
                'status': 'success',
                'message': 'Transaction completed successfully'
            }
        else:
            print("Coordinator: Partial commit detected, initiating recovery")
            failed_nodes = [nid for nid, resp in commit_responses.items() if not resp]
            return {
                'status': 'error',
                'message': f'Transaction partially committed, Nodes {failed_nodes} failed'
            }
            
    def recover_transaction(self, transaction_id, transaction):
        """Recovery procedure after coordinator crash"""
        print("\nCoordinator: Starting recovery procedure")
        return self.commit_phase(transaction_id, transaction)
            
    def abort_transaction(self, transaction_id):
        """Abort transaction on all nodes"""
        print("\nCoordinator: Aborting transaction")
        for node_id, node in self.nodes.items():
            try:
                node.abort()
                print(f"Coordinator: Node-{node_id} aborted successfully")
            except Exception as e:
                print(f"Coordinator: Failed to abort on Node-{node_id}: {e}")
                
    def wait_for_node_recovery(self, node_id, timeout=5):
        """Wait for a node to recover, with timeout"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                self.nodes[node_id].get_balance()
                print(f"Coordinator: Node-{node_id} recovered")
                return True
            except Exception:
                time.sleep(1)
        print(f"Coordinator: Node-{node_id} recovery timeout")
        return False
        
    def get_balances(self):
        """Get current balances from all nodes"""
        balances = {}
        for node_id, node in self.nodes.items():
            try:
                balances[str(node_id)] = node.get_balance()  # Convert key to string
            except Exception as e:
                balances[str(node_id)] = f"Error: {e}"
        return balances
        
    def reset_scenario(self, scenario, crash_scenario=None):
        """Reset all nodes for a new scenario"""
        print(f"\nCoordinator: Resetting for scenario {scenario}")
        for node_id, node in self.nodes.items():
            try:
                node.reset_balance(scenario)
                if crash_scenario == 'c1':
                    if node_id == 2:  # Node-2
                        node.set_crash_scenario('c1', 'before')
                elif crash_scenario == 'c2':
                    if node_id == 2:  # Node-2
                        node.set_crash_scenario('c2', 'after')
            except Exception as e:
                print(f"Coordinator: Failed to reset Node-{node_id}: {e}")
        return True
        
    def run(self):
        print(f"Coordinator running on port {self.port}")
        self.server.serve_forever()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CISC-6935 lab-3 coordinator")
    parser.add_argument("--ip", type=str, default='localhost', help="IP address")
    parser.add_argument("--port", type=int, default=8001, help="Port number")
    parser.add_argument("--id", type=int, default=1, help="Node ID")
    parser.add_argument("--is_coord",action="store_true",default=False, help="If this node is coordinator")
    args = parser.parse_args()
    coordinator = Coordinator()
    coordinator.run()