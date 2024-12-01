import random
import time
import threading
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import argparse
from node import Node

class RaftNode:
    def __init__(self, node_id, port, peer_ports, leader_id):
        self.node_id = node_id
        self.port = port
        self.peer_ports = peer_ports
        self.leader_id = leader_id  # 2 or 3
        
        # Raft state
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.commit_index = -1
        self.last_applied = -1
        self.state = 'follower'
        self.votes_received = set()
        self.last_heartbeat = 0
        self.peers = {
            port: ServerProxy(f"http://localhost:{port}", allow_none=True)
            for port in peer_ports
        }
        
        # Account state
        self.account_file = f"account_{leader_id}_{node_id}.txt"
        self.balance = 0
        self.load_balance()
        
        # Start election timer
        self.reset_election_timer()
        threading.Thread(target=self.election_timer_thread, daemon=True).start()
        threading.Thread(target=self.heartbeat_thread, daemon=True).start()

    def load_balance(self):
        try:
            with open(self.account_file, 'r') as f:
                self.balance = float(f.read().strip())
        except FileNotFoundError:
            self.balance = 200 if self.leader_id == 2 else 300
            self.save_balance()

    def save_balance(self):
        with open(self.account_file, 'w') as f:
            f.write(str(self.balance))

    def reset_election_timer(self):
        self.last_heartbeat = time.time()
        self.election_timeout = random.uniform(3, 6)  # Random timeout between 3-6 seconds

    def election_timer_thread(self):
        while True:
            if self.state != 'leader':
                if time.time() - self.last_heartbeat > self.election_timeout:
                    self.start_election()
            time.sleep(1)

    def heartbeat_thread(self):
        while True:
            if self.state == 'leader':
                self.send_heartbeat()
            time.sleep(1)

    def start_election(self):
        self.state = 'candidate'
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}
        
        # Request votes from peers
        for peer_port, peer in self.peers.items():
            try:
                vote_granted = peer.request_vote(
                    self.current_term,
                    self.node_id,
                    len(self.log) - 1,
                    self.log[-1]['term'] if self.log else 0
                )
                if vote_granted:
                    self.votes_received.add(peer_port)
            except Exception as e:
                print(f"Error requesting vote from {peer_port}: {e}")

        # Check if we won the election
        if len(self.votes_received) > len(self.peers) / 2:
            self.state = 'leader'
            print(f"Node {self.node_id} became leader for term {self.current_term}")
            self.send_heartbeat()

    def request_vote(self, term, candidate_id, last_log_index, last_log_term):
        if term > self.current_term:
            self.current_term = term
            self.state = 'follower'
            self.voted_for = None
        
        if (term < self.current_term or 
            (self.voted_for is not None and self.voted_for != candidate_id)):
            return False
            
        if (len(self.log) - 1 > last_log_index or
            (self.log and self.log[-1]['term'] > last_log_term)):
            return False
            
        self.voted_for = candidate_id
        self.reset_election_timer()
        return True

    def send_heartbeat(self):
        for peer_port, peer in self.peers.items():
            try:
                peer.append_entries(
                    self.current_term,
                    self.node_id,
                    len(self.log) - 1,
                    self.log[-1]['term'] if self.log else 0,
                    [],
                    self.commit_index
                )
            except Exception as e:
                print(f"Error sending heartbeat to {peer_port}: {e}")

    def append_entries(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        if term < self.current_term:
            return False
            
        if term > self.current_term:
            self.current_term = term
            self.state = 'follower'
            self.voted_for = None
            
        self.reset_election_timer()
        
        # Handle log consistency check
        if prev_log_index >= len(self.log):
            return False
        if prev_log_index >= 0 and self.log[prev_log_index]['term'] != prev_log_term:
            return False
            
        # Append new entries
        if entries:
            self.log = self.log[:prev_log_index + 1]
            self.log.extend(entries)

            for entry in entries:
                if entry['type'] == 'commit':
                    self.apply_transaction(entry['transaction'])
                elif entry['type'] == 'reset':
                    # Handle reset balance entry
                    self.balance = entry['balance']
                    self.save_balance()
                    print(f"Node {self.node_id} reset balance to {self.balance}")
            
        # Update commit index
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log) - 1)
            
        return True


    def apply_transaction(self, transaction):
        """Apply transaction and update balance"""
        try:
            if transaction['type'] == 'transfer':
                if self.leader_id == 2:  # Account A
                    self.balance -= transaction['amount']
                else:  # Account B
                    self.balance += transaction['amount']
            elif transaction['type'] == 'bonus':
                self.balance += transaction['bonus_amount']
                
            self.save_balance()
            print(f"Node {self.node_id}: Applied transaction, new balance: {self.balance}")
            return True
        except Exception as e:
            print(f"Node {self.node_id}: Failed to apply transaction: {e}")
            return False
        

    def validate_transaction(self, transaction):
        """Validate transaction and achieve consensus with followers"""
        if self.state != 'leader':
            return False
            
        # Create log entry for validation
        log_entry = {
            'term': self.current_term,
            'transaction': transaction,
            'type': 'validate'
        }
        
        # Try to replicate log entry to followers
        success = self.replicate_log_entry(log_entry)
        if not success:
            return False
            
        # Perform local validation
        if transaction['type'] == 'transfer':
            if self.leader_id == 2:  # Account A
                return self.balance >= transaction['amount']
            return True
        elif transaction['type'] == 'bonus':
            return True
            
        return False

    def commit_transaction(self, transaction):
        """Commit transaction and achieve consensus with followers"""
        if self.state != 'leader':
            return False
            
        # Create log entry for commit
        log_entry = {
            'term': self.current_term,
            'transaction': transaction,
            'type': 'commit'
        }
        
        # Try to replicate log entry to followers
        success = self.replicate_log_entry(log_entry)
        if not success:
            return False
            
        # Apply transaction

        return self.apply_transaction(transaction)


    def replicate_log_entry(self, log_entry):
        """Replicate log entry to followers and wait for majority consensus"""
        self.log.append(log_entry)
        success_count = 1  # Count self
        
        for peer_port, peer in self.peers.items():
            try:
                success = peer.append_entries(
                    self.current_term,
                    self.node_id,
                    len(self.log) - 2,
                    self.log[-2]['term'] if len(self.log) > 1 else 0,
                    [log_entry],
                    self.commit_index
                )
                if success:
                    success_count += 1
            except Exception as e:
                print(f"Error replicating log entry to {peer_port}: {e}")
                
        # Check if we have majority
        return success_count > (len(self.peers) + 1) / 2
    
    def reset_balance(self, scenario):
        """Reset balance for test scenario"""
        # Set leader's balance
        if self.leader_id == 2:  # Account A
            if scenario == 'a' or scenario == 'c':
                self.balance = 200
            elif scenario == 'b':
                self.balance = 90
        elif self.leader_id == 3:  # Account B
            if scenario == 'a' or scenario == 'c':
                self.balance = 300
            elif scenario == 'b':
                self.balance = 50
                
        self.save_balance()
        
        # Create a reset log entry and replicate to followers
        log_entry = {
            'term': self.current_term,
            'type': 'reset',
            'balance': self.balance
        }
        
        # Replicate to followers
        for peer_port, peer in self.peers.items():
            try:
                success = peer.append_entries(
                    self.current_term,
                    self.node_id,
                    len(self.log) - 1,
                    self.log[-1]['term'] if self.log else 0,
                    [log_entry],
                    self.commit_index
                )
                print(f"Reset balance replicated to peer {peer_port}: {success}")
            except Exception as e:
                print(f"Error resetting balance for peer {peer_port}: {e}")
        
class EnhancedNode(Node):
    def __init__(self, node_id, port, raft_port, peer_ports, leader_id):
        super().__init__(node_id, port)
        self.raft_node = RaftNode(node_id, raft_port, peer_ports, leader_id)
        self.server.register_function(self.request_vote, "request_vote")
        self.server.register_function(self.append_entries, "append_entries")
        self.server.register_function(self.reset_balance, "reset_balance")
        
    def prepare(self, transaction):
        """Override prepare to use Raft consensus"""
        return self.raft_node.validate_transaction(transaction)
        
    def commit(self, transaction):
        """Override commit to use Raft consensus"""
        return self.raft_node.commit_transaction(transaction)
        
    def get_balance(self):
        """Override get_balance to use Raft state"""
        return self.raft_node.balance
    
    def request_vote(self, term, candidate_id, last_log_index, last_log_term):
        """Forward request_vote to RaftNode"""
        return self.raft_node.request_vote(term, candidate_id, last_log_index, last_log_term)
        
    def append_entries(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        """Forward append_entries to RaftNode"""
        return self.raft_node.append_entries(term, leader_id, prev_log_index, prev_log_term, entries, leader_commit)
    
    def reset_balance(self, scenario):
        """Reset balance for test scenario"""
        print(f"Entering the enhanced node reset balance for")
        self.raft_node.reset_balance(scenario=scenario)
        self.crash_scenario = None
        self.crash_point = None
        self.transaction_log = []
        self.prepared_transactions = {}
        self.crash_times = 0
        return True

def main():
    parser = argparse.ArgumentParser(description="Raft Node Server")
    parser.add_argument('--node_id', type=int, required=True, help="Node ID")
    parser.add_argument('--port', type=int, required=True, help="Main port for 2PC")
    parser.add_argument('--raft_port', type=int, required=True, help="Port for Raft consensus")
    parser.add_argument('--peer_ports', type=str, required=True, help="Comma-separated list of peer Raft ports")
    parser.add_argument('--is_leader', type=bool, default=False, help="Whether this node is a leader")
    parser.add_argument('--leader_id', type=int, help="ID of the leader node (2 or 3)")
    
    args = parser.parse_args()
    
    # Convert peer_ports string to list of integers
    peer_ports = [int(p) for p in args.peer_ports.split(',')]
    
    # Create enhanced node with Raft support
    node = EnhancedNode(
        node_id=args.node_id,
        port=args.port,
        raft_port=args.raft_port,
        peer_ports=peer_ports,
        leader_id=args.leader_id
    )
    
    print(f"Starting Enhanced Node {args.node_id}")
    print(f"Main port (2PC): {args.port}")
    print(f"Raft port: {args.raft_port}")
    print(f"Peer ports: {peer_ports}")
    print(f"Is leader: {args.is_leader}")
    
    # Start the node's server
    node.run()

if __name__ == "__main__":
    main()