# node.py
import socket
import threading
import time
import json
import sys
import os
import random

# Node configuration
ALL_NODES = {
    0: ('localhost', 10001),
    1: ('localhost', 10002),
    2: ('localhost', 10003)
}

class Node:
    def __init__(self, node_id):
        self.node_id = node_id
        self.state = 'Follower'
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.commit_index = -1
        self.last_applied = -1
        self.leader_id = None
        self.next_index = {i: 0 for i in ALL_NODES.keys()}  # Leader: next index to send for each follower
        self.match_index = {i: -1 for i in ALL_NODES.keys()}  # Leader: highest log entry known to be replicated
        self.lock = threading.RLock()
        
        # Election timeout between 3-5 seconds (as per reference code)
        self.election_timeout = random.uniform(3, 5)
        self.last_heartbeat = time.time()
        self.election_timer = None
        
        # Initialize log file
        self.log_file = "CISC6935"
        self.init_log_file()
        
        # Load existing log entries if any
        self.load_log()

    def init_log_file(self):
        """Initialize the log file if it doesn't exist"""
        if not os.path.exists(self.log_file):
            with open(self.log_file, 'w') as f:
                f.write("")
            print(f"Created log file: {self.log_file}")

    def load_log(self):
        """Load existing log entries from file"""
        try:
            with open(self.log_file, 'r') as f:
                lines = f.readlines()
                for line in lines:
                    if line.strip():
                        entry = json.loads(line.strip())
                        self.log.append(entry)
        except FileNotFoundError:
            pass

    def save_log_entry(self, entry):
        """Save a single log entry to the log file"""
        with open(self.log_file, 'a') as f:
            f.write(json.dumps(entry) + '\n')

    def reset_log_file(self):
        """Reset log file with current log entries (used after log consistency check)"""
        with open(self.log_file, 'w') as f:
            for entry in self.log:
                f.write(json.dumps(entry) + '\n')

    def start_election_timer(self):
        """Start or restart the election timer"""
        if self.election_timer:
            self.election_timer.cancel()
        self.election_timer = threading.Timer(self.election_timeout, self.start_election)
        self.election_timer.start()

    def start_server(self):
        """Start the server to listen for incoming connections"""
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(ALL_NODES[self.node_id])
        server.listen(5)
        print(f"Node {self.node_id} listening on {ALL_NODES[self.node_id]}")
        
        # Start election timer
        self.start_election_timer()
        
        # Start heartbeat thread if leader
        self.heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        self.heartbeat_thread.daemon = True
        self.heartbeat_thread.start()

        while True:
            try:
                conn, addr = server.accept()
                client_thread = threading.Thread(target=self.handle_client, args=(conn,))
                client_thread.daemon = True
                client_thread.start()
            except Exception as e:
                print(f"Error accepting connection: {e}")

    # def handle_client(self, conn):
    #     """Handle incoming client connections"""
    #     try:
    #         data = conn.recv(1024).decode()
    #         if data:
    #             message = json.loads(data)
    #             response = self.process_message(message)
    #             conn.send(json.dumps(response).encode())
    #     except Exception as e:
    #         print(f"Error handling client: {e}")
    #     finally:
    #         conn.close()

    def handle_client(self, conn):
        """Handle incoming client connections"""
        try:
            data = conn.recv(1024).decode()
            if data:
                message = json.loads(data)
                print(f"\nNode {self.node_id} received message: {message['type']} from {conn.getpeername()}")
                response = self.process_message(message)
                print(f"Node {self.node_id} sending response: {response}")
                conn.send(json.dumps(response).encode())
        except Exception as e:
            print(f"Error handling client: {e}")
        finally:
            conn.close()

    def process_message(self, message):
        """Process incoming messages based on their type"""
        print("Entered process_message...")
        """Process incoming messages based on their type"""
        try:
            # Acquire lock with timeout to prevent deadlock
            if not self.lock.acquire(timeout=5):
                print("Warning: Could not acquire lock in process_message")
                return {'status': 'error', 'message': 'Lock acquisition timeout'}
            
            try:
                print("Within lock in process_message")
                if message['type'] == 'RequestVote':
                    return self.handle_request_vote(message)
                elif message['type'] == 'AppendEntries':
                    return self.handle_append_entries(message)
                elif message['type'] == 'SubmitValue':
                    return self.handle_submit_value(message)
                elif message['type'] == 'SimulateFailure':
                    return self.handle_simulate_failure()
                elif message['type'] == 'SimulateRecover':
                    return self.handle_simulate_recover()
                else:
                    return {'status': 'error', 'message': 'Unknown message type'}
            finally:
                self.lock.release()
                
        except Exception as e:
            print(f"Error in process_message: {e}")
            return {'status': 'error', 'message': str(e)}

    # def handle_request_vote(self, message):
    #     """Handle incoming vote requests"""
    #     with self.lock:
    #         term = message['term']
    #         candidate_id = message['candidate_id']
            
    #         # Update term if necessary
    #         if term > self.current_term:
    #             self.current_term = term
    #             self.state = 'Follower'
    #             self.voted_for = None
    #             self.leader_id = None
            
    #         # Check if vote can be granted
    #         if (term >= self.current_term and 
    #             (self.voted_for is None or self.voted_for == candidate_id) and
    #             (len(message['log']) >= len(self.log))):
                
    #             self.voted_for = candidate_id
    #             self.start_election_timer()
    #             return {'term': self.current_term, 'vote_granted': True}
            
    #         return {'term': self.current_term, 'vote_granted': False}
    def handle_request_vote(self, message):
        """Handle incoming vote requests"""
        print("Entered handle_request_vote...")
  
        term = message['term']
        candidate_id = message['candidate_id']
        
        print(f"\nNode {self.node_id} processing RequestVote from node {candidate_id}")
        print(f"Current term: {self.current_term}, Received term: {term}")
        print(f"Current voted_for: {self.voted_for}")
        
        # Update term if necessary
        if term > self.current_term:
            print(f"Node {self.node_id} updating term from {self.current_term} to {term}")
            self.current_term = term
            self.state = 'Follower'
            self.voted_for = None
            self.leader_id = None
        
        # Check if vote can be granted
        can_vote = (term >= self.current_term and 
                (self.voted_for is None or self.voted_for == candidate_id) and
                (len(message.get('log', [])) >= len(self.log)))
        
        if can_vote:
            print(f"Node {self.node_id} granting vote to node {candidate_id}")
            self.voted_for = candidate_id
            self.start_election_timer()
            return {'term': self.current_term, 'vote_granted': True}
        
        print(f"Node {self.node_id} rejecting vote for node {candidate_id}")
        return {'term': self.current_term, 'vote_granted': False}

    # def handle_append_entries(self, message):
    #     """Handle incoming append entries (including heartbeats)"""
    #     with self.lock:
    #         term = message['term']
    #         leader_id = message['leader_id']
    #         prev_log_index = message['prev_log_index']
    #         prev_log_term = message['prev_log_term']
    #         entries = message['entries']
    #         leader_commit = message['leader_commit']

    #         # Reply false if term < currentTerm
    #         if term < self.current_term:
    #             return {'term': self.current_term, 'success': False}

    #         # Update term if necessary
    #         if term > self.current_term:
    #             self.current_term = term
    #             self.voted_for = None

    #         # Reset election timer and update leader
    #         self.last_heartbeat = time.time()
    #         self.start_election_timer()
    #         self.state = 'Follower'
    #         self.leader_id = leader_id

    #         # Consistency check
    #         if prev_log_index >= len(self.log) or \
    #            (prev_log_index >= 0 and self.log[prev_log_index]['term'] != prev_log_term):
    #             return {'term': self.current_term, 'success': False}

    #         # Append new entries
    #         if entries:
    #             # Delete conflicting entries and append new ones
    #             self.log = self.log[:prev_log_index + 1]
    #             self.log.extend(entries)
    #             self.reset_log_file()

    #         # Update commit index
    #         if leader_commit > self.commit_index:
    #             self.commit_index = min(leader_commit, len(self.log) - 1)
    #             self.apply_committed_entries()

    #         return {'term': self.current_term, 'success': True}

    def handle_append_entries(self, message):
        """Handle incoming append entries (including heartbeats)"""
        with self.lock:
            term = message['term']
            leader_id = message['leader_id']
            prev_log_index = message['prev_log_index']
            prev_log_term = message['prev_log_term']
            entries = message['entries']
            leader_commit = message['leader_commit']

            print(f"\nNode {self.node_id} processing AppendEntries from leader {leader_id}")
            print(f"Current term: {self.current_term}, Received term: {term}")
            print(f"Entries to append: {len(entries)}")

            # Reply false if term < currentTerm
            if term < self.current_term:
                print(f"Node {self.node_id} rejecting AppendEntries: term ({term}) < currentTerm ({self.current_term})")
                return {'term': self.current_term, 'success': False}

            # Update term if necessary
            if term > self.current_term:
                print(f"Node {self.node_id} updating term from {self.current_term} to {term}")
                self.current_term = term
                self.voted_for = None

            # Reset election timer and update leader
            self.last_heartbeat = time.time()
            self.start_election_timer()
            self.state = 'Follower'
            self.leader_id = leader_id

            # Consistency check
            if prev_log_index >= len(self.log) or \
            (prev_log_index >= 0 and self.log[prev_log_index]['term'] != prev_log_term):
                print(f"Node {self.node_id} rejecting AppendEntries: log inconsistency")
                print(f"prev_log_index: {prev_log_index}, log length: {len(self.log)}")
                return {'term': self.current_term, 'success': False}

            # Append new entries
            if entries:
                print(f"Node {self.node_id} appending {len(entries)} new entries")
                # Delete conflicting entries and append new ones
                self.log = self.log[:prev_log_index + 1]
                self.log.extend(entries)
                self.reset_log_file()

            # Update commit index
            if leader_commit > self.commit_index:
                print(f"Node {self.node_id} updating commit index from {self.commit_index} to {min(leader_commit, len(self.log) - 1)}")
                self.commit_index = min(leader_commit, len(self.log) - 1)
                self.apply_committed_entries()

            return {'term': self.current_term, 'success': True}

    def handle_submit_value(self, message):
        """Handle client value submissions"""
        if self.state != 'Leader':
            return {
                'status': 'redirect',
                'leader': self.leader_id,
                'message': f'Not the leader. Current leader is node {self.leader_id}'
            }

        # Append the entry to leader's log
        entry = {
            'term': self.current_term,
            'value': message['value']
        }
        self.log.append(entry)
        self.save_log_entry(entry)

        # Try to replicate to followers
        success = self.replicate_log()
        
        if success:
            return {'status': 'success', 'term': self.current_term}
        else:
            return {'status': 'error', 'message': 'Failed to replicate to majority'}

    def replicate_log(self):
        """Replicate log entries to followers"""
        success_count = 1  # Count self
        for node_id in ALL_NODES:
            if node_id != self.node_id:
                try:
                    prev_log_index = self.next_index[node_id] - 1
                    prev_log_term = self.log[prev_log_index]['term'] if prev_log_index >= 0 else 0
                    entries = self.log[self.next_index[node_id]:]
                    
                    response = self.send_message(node_id, {
                        'type': 'AppendEntries',
                        'term': self.current_term,
                        'leader_id': self.node_id,
                        'prev_log_index': prev_log_index,
                        'prev_log_term': prev_log_term,
                        'entries': entries,
                        'leader_commit': self.commit_index
                    })

                    if response.get('success'):
                        self.next_index[node_id] = len(self.log)
                        self.match_index[node_id] = len(self.log) - 1
                        success_count += 1
                    else:
                        # Decrement nextIndex and try again
                        self.next_index[node_id] = max(0, self.next_index[node_id] - 1)
                except Exception as e:
                    print(f"Error replicating to node {node_id}: {e}")

        # Return True if majority successful
        return success_count > len(ALL_NODES) // 2

    # def start_election(self):
    #     """Start leader election process"""
    #     print("Start election stage...")
    #     with self.lock:
    #         print("Went into the lock in start_election")
    #         if self.state == 'Leader':
    #             return

    #         self.state = 'Candidate'
    #         self.current_term += 1
    #         self.voted_for = self.node_id
    #         self.leader_id = None

    #         votes_received = 1  # Vote for self
            
    #         # Request votes from all other nodes
    #         for node_id in ALL_NODES:
    #             if node_id != self.node_id:
    #                 try:
    #                     response = self.send_message(node_id, {
    #                         'type': 'RequestVote',
    #                         'term': self.current_term,
    #                         'candidate_id': self.node_id,
    #                         'log': self.log
    #                     })

    #                     if response.get('vote_granted'):
    #                         votes_received += 1
    #                 except Exception as e:
    #                     print(f"Error requesting vote from node {node_id}: {e}")

    #         # If received majority votes, become leader
    #         if votes_received > len(ALL_NODES) // 2:
    #             self.become_leader()
    #         else:
    #             self.state = 'Follower'
    #             self.start_election_timer()
    def start_election(self):
        """Start leader election process"""
        with self.lock:
            if self.state == 'Leader':
                return

            print(f"\nNode {self.node_id} starting election for term {self.current_term + 1}")
            self.state = 'Candidate'
            self.current_term += 1
            self.voted_for = self.node_id
            self.leader_id = None

            votes_received = 1  # Vote for self
            print(f"Node {self.node_id} voted for self")
            
            # Request votes from all other nodes
            for node_id in ALL_NODES:
                if node_id != self.node_id:
                    try:
                        print(f"Node {self.node_id} requesting vote from node {node_id}")
                        response = self.send_message(node_id, {
                            'type': 'RequestVote',
                            'term': self.current_term,
                            'candidate_id': self.node_id,
                            'log': self.log
                        })

                        if response and response.get('vote_granted'):
                            votes_received += 1
                            print(f"Node {self.node_id} received vote from node {node_id}")
                        else:
                            print(f"Node {self.node_id} was denied vote from node {node_id}")
                    except Exception as e:
                        print(f"Error requesting vote from node {node_id}: {e}")

            print(f"Node {self.node_id} received {votes_received} votes out of {len(ALL_NODES)}")
            # If received majority votes, become leader
            if votes_received > len(ALL_NODES) // 2:
                self.become_leader()
            else:
                print(f"Node {self.node_id} failed to get majority votes, returning to Follower state")
                self.state = 'Follower'
                self.start_election_timer()

    def become_leader(self):
        """Transition to leader state"""
        self.state = 'Leader'
        self.leader_id = self.node_id
        print(f"Node {self.node_id} became leader for term {self.current_term}")
        
        # Initialize leader state
        self.next_index = {i: len(self.log) for i in ALL_NODES.keys()}
        self.match_index = {i: -1 for i in ALL_NODES.keys()}
        
        # Send initial empty AppendEntries
        self.broadcast_append_entries()

    def send_heartbeat(self):
        """Send periodic heartbeats if leader"""
        while True:
            time.sleep(1)  # Send heartbeat every second
            if self.state == 'Leader':
                self.broadcast_append_entries()

    # def broadcast_append_entries(self):
    #     """Broadcast AppendEntries to all followers"""
    #     if self.state != 'Leader':
    #         return

    #     for node_id in ALL_NODES:
    #         if node_id != self.node_id:
    #             try:
    #                 prev_log_index = len(self.log) - 1
    #                 prev_log_term = self.log[prev_log_index]['term'] if self.log else 0
                    
    #                 self.send_message(node_id, {
    #                     'type': 'AppendEntries',
    #                     'term': self.current_term,
    #                     'leader_id': self.node_id,
    #                     'prev_log_index': prev_log_index,
    #                     'prev_log_term': prev_log_term,
    #                     'entries': [],  # Empty for heartbeat
    #                     'leader_commit': self.commit_index
    #                 })
    #             except Exception as e:
    #                 print(f"Error sending heartbeat to node {node_id}: {e}")

    def broadcast_append_entries(self):
        """Broadcast AppendEntries to all followers"""
        if self.state != 'Leader':
            return

        for node_id in ALL_NODES:
            if node_id != self.node_id:
                try:
                    prev_log_index = len(self.log) - 1
                    prev_log_term = self.log[prev_log_index]['term'] if self.log else 0
                    
                    print(f"\nNode {self.node_id} sending heartbeat to node {node_id}")
                    self.send_message(node_id, {
                        'type': 'AppendEntries',
                        'term': self.current_term,
                        'leader_id': self.node_id,
                        'prev_log_index': prev_log_index,
                        'prev_log_term': prev_log_term,
                        'entries': [],  # Empty for heartbeat
                        'leader_commit': self.commit_index
                    })
                except Exception as e:
                    print(f"Error sending heartbeat to node {node_id}: {e}")

    def apply_committed_entries(self):
        """Apply committed log entries"""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            print(f"Applied log entry: {entry['value']}")

    def handle_simulate_failure(self):
        """Simulate node failure by deleting log file"""
        try:
            os.remove(self.log_file)
            self.log = []
            self.commit_index = -1
            self.last_applied = -1
            return {'status': 'success', 'message': f'Node {self.node_id} simulated failure'}
        except Exception as e:
            return {'status': 'error', 'message': str(e)}

    def handle_simulate_recover(self):
        """Simulate node recovery"""
        self.init_log_file()
        self.state = 'Follower'
        self.start_election_timer()
        return {'status': 'success', 'message': f'Node {self.node_id} recovered'}

    # def send_message(self, node_id, message):
    #     """Send message to another node"""
    #     print("start to send message...")
    #     try:
    #         ip, port = ALL_NODES[node_id]
    #         with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    #             sock.connect((ip, port))
    #             sock.sendall(json.dumps(message).encode())
    #             response = json.loads(sock.recv(1024).decode())
    #             print("Already get the response")
    #             return response
    #     except Exception as e:
    #         print(f"Error sending message to node {node_id}: {e}")
    #         return {'status': 'error', 'message': str(e)}

    def send_message(self, node_id, message):
        """Send message to another node"""
        try:
            ip, port = ALL_NODES[node_id]
            print(f"\nNode {self.node_id} sending {message['type']} to node {node_id} at {ip}:{port}")
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((ip, port))
                sock.sendall(json.dumps(message).encode())
                response = json.loads(sock.recv(1024).decode())
                print(f"Node {self.node_id} received response from node {node_id}: {response}")
                return response
        except Exception as e:
            print(f"Error sending message to node {node_id}: {e}")
            return None

    def run(self):
        """Start the node"""
        print(f"Starting node {self.node_id} in {self.state} state")
        self.start_server()

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python node.py <node_id>")
        sys.exit(1)
    
    node_id = int(sys.argv[1])
    if node_id not in ALL_NODES:
        print(f"Invalid node_id. Must be one of {list(ALL_NODES.keys())}")
        sys.exit(1)

    node = Node(node_id)
    try:
        node.run()
    except KeyboardInterrupt:
        print(f"\nShutting down node {node_id}...")
        sys.exit(0)