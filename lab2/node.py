# node.py
import socket
import threading
import time
import json
import sys
import os
import random
from datetime import datetime

# Node configuration
ALL_NODES = {
    0: ('10.128.0.5', 10001),
    1: ('10.128.0.6', 10002),
    2: ('10.128.0.7', 10003)
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
        
        self.election_timeout = random.uniform(1, 3) # Here I set them to 1~3s
        self.last_heartbeat = time.time()
        self.election_timer = None

        self.running = True
        self.heartbeat_thread = None
        self.election_thread = None
        self.is_crashed = False
        
        # Initialize log file
        self.log_dir = "raft_logs"
        os.makedirs(self.log_dir, exist_ok=True)
        self.log_file = os.path.join(self.log_dir, f"node_{node_id}_log.json")
        self.state_file = os.path.join(self.log_dir, f"node_{node_id}_state.json")

        self.init_log_file()
        self.load_state()

    def stop_threads(self):
        """Stop all running threads except the main server socket"""
        self.running = False
        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            self.heartbeat_thread.join(timeout=1)
        if self.election_thread and self.election_thread.is_alive():
            self.election_thread.join(timeout=1)
        if self.election_timer:
            self.election_timer.cancel()

    def send_heartbeat(self):
        """Modified heartbeat method with crash check"""
        while self.running and not self.is_crashed:
            time.sleep(1)  # Send heartbeat every second
            if self.state == 'Leader' and not self.is_crashed:
                self.broadcast_append_entries()

    def init_log_file(self):

        if not os.path.exists(self.log_file):
            with open(self.log_file, 'w') as f:
                f.write("")
            print(f"Created log file: {self.log_file}")

        if not os.path.exists(self.state_file):
            self.save_state()
            print(f"Created state file: {self.state_file}")

    def load_state(self):
        """Load node state from file"""
        try:
            with open(self.state_file, 'r') as f:
                state = json.load(f)
                self.current_term = state.get('current_term', 0)
                self.voted_for = state.get('voted_for')
                self.commit_index = state.get('commit_index', -1)
                self.last_applied = state.get('last_applied', -1)
                self.state = state.get('state', 'Follower')
                self.leader_id = state.get('leader_id')
            
            # Load log entries
            with open(self.log_file, 'r') as f:
                lines = f.readlines()
                self.log = []
                for line in lines:
                    if line.strip():
                        entry = json.loads(line.strip())
                        self.log.append(entry)
        except FileNotFoundError:
            pass

    def save_state(self):
        """Save node state to file"""
        state = {
            'current_term': self.current_term,
            'voted_for': self.voted_for,
            'commit_index': self.commit_index,
            'last_applied': self.last_applied,
            'state': self.state,
            'leader_id': self.leader_id
        }
        with open(self.state_file, 'w') as f:
            json.dump(state, f, indent=2)

    def save_log_entry(self, entry):
        """Save a log entry with timestamp"""
        entry_with_metadata = {
            'term': entry['term'],
            'value': entry['value'],
            'timestamp': datetime.now().isoformat(),
            'node_id': self.node_id
        }
        with open(self.log_file, 'a') as f:
            f.write(json.dumps(entry_with_metadata) + '\n')
        return entry_with_metadata

    def reset_log_file(self):
        """Reset log file with current log entries (used after log consistency check)"""
        with open(self.log_file, 'w') as f:
            for entry in self.log:
                f.write(json.dumps(entry) + '\n')

    def start_server(self):
        """Modified server start method that keeps running even when crashed"""
        try:
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind(ALL_NODES[self.node_id])
            server.listen(5)
            server.settimeout(1)  # Add timeout to allow for graceful shutdown
            print(f"Node {self.node_id} listening on {ALL_NODES[self.node_id]}")
            
            self.running = True
            self.is_crashed = False
            
            # Start election timer
            self.start_election_timer()
            
            # Start heartbeat thread
            self.heartbeat_thread = threading.Thread(target=self.send_heartbeat)
            self.heartbeat_thread.daemon = True
            self.heartbeat_thread.start()

            while True:  # Keep the server socket running always
                try:
                    conn, addr = server.accept()
                    client_thread = threading.Thread(target=self.handle_client, args=(conn,))
                    client_thread.daemon = True
                    client_thread.start()
                except socket.timeout:
                    # Timeout is expected, just continue
                    continue
                except Exception as e:
                    if self.running:  # Only print error if we're supposed to be running
                        print(f"Error accepting connection: {e}")
                        
        except Exception as e:
            print(f"Server socket error: {e}")
        finally:
            server.close()

    def start_election_timer(self):
        """Start or restart the election timer"""
        if self.election_timer:
            self.election_timer.cancel()
        self.election_timer = threading.Timer(self.election_timeout, self.start_election)
        self.election_timer.start()

    def start_election(self):
        """Start leader election process"""
        with self.lock:
            if self.state == 'Leader':
                return

            print(f"\nNode {self.node_id} starting election for term {self.current_term + 1}")
            self.state = 'Candidate'
            self.current_term += 1
            self.voted_for = self.node_id # Vote for itself
            self.leader_id = None

            votes_received = 1  # Vote for itself
            print(f"Node {self.node_id} voted for itself")
            
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

    def handle_client(self, conn):
        """Handle incoming client connections with crash awareness"""
        try:
            data = conn.recv(1024).decode()
            if data:
                message = json.loads(data)
                print(f"\nNode {self.node_id} received message: {message['type']} from {conn.getpeername()}")
                
                # Always process recovery messages, even when crashed
                if message['type'] == 'SimulateRecover':
                    response = self.handle_simulate_recover()
                # Only process other messages if not crashed
                elif not self.is_crashed:
                    response = self.process_message(message)
                else:
                    response = {'status': 'error', 'message': 'Node is crashed'}
                
                print(f"Node {self.node_id} sending response: {response}")
                conn.send(json.dumps(response).encode())
        except Exception as e:
            print(f"Error handling client: {e}")
        finally:
            conn.close()
    
    def process_message(self, message):
        """Modified process_message to handle messages while crashed"""
        try:
            if not self.lock.acquire(timeout=10):
                print("Warning: Could not acquire lock in process_message")
                return {'status': 'error', 'message': 'Lock acquisition timeout'}
            
            try:
                # Allow processing of recover message even when crashed
                if message['type'] == 'SimulateRecover':
                    return self.handle_simulate_recover()
                    
                # If crashed, don't process any other messages
                if self.is_crashed:
                    return {'status': 'error', 'message': 'Node is crashed'}
                    
                # Process other messages normally when not crashed
                if message['type'] == 'RequestLogSync':
                    return self.handle_request_log_sync(message)
                elif message['type'] == 'TriggerLeaderChange':
                    return self.handle_trigger_leader_change()
                elif message['type'] == 'QueryState':
                    return self.handle_query_state(message)
                elif message['type'] == 'RequestVote':
                    return self.handle_request_vote(message)
                elif message['type'] == 'AppendEntries':
                    return self.handle_append_entries(message)
                elif message['type'] == 'SubmitValue':
                    return self.handle_submit_value(message)
                elif message['type'] == 'SimulateFailure':
                    return self.handle_simulate_failure(message)
                else:
                    return {'status': 'error', 'message': 'Unknown message type'}
            finally:
                self.lock.release()
                    
        except Exception as e:
            print(f"Error in process_message: {e}")
            return {'status': 'error', 'message': str(e)} 

    def handle_request_vote(self, message):
        """Handle incoming vote requests"""
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
            self.voted_for = None  # Reset vote for new term
            self.leader_id = None
            self.save_state()  # Save the new term and reset vote
        
        # Check if vote can be granted
        # First condition: term should be up-to-date
        term_ok = term >= self.current_term
        
        # Second condition: we haven't voted for someone else in this term,
        # or we previously voted for this candidate
        vote_ok = (self.voted_for is None or self.voted_for == candidate_id)
        
        # Third condition: candidate's log is at least as complete as ours
        log_ok = len(message.get('log', [])) >= len(self.log)
        
        can_vote = term_ok and vote_ok and log_ok
        
        if can_vote:
            print(f"Node {self.node_id} granting vote to node {candidate_id}")
            self.voted_for = candidate_id
            self.save_state()  # Save our vote
            self.start_election_timer()
            return {'term': self.current_term, 'vote_granted': True}
        
        print(f"Node {self.node_id} rejecting vote for node {candidate_id}")
        return {'term': self.current_term, 'vote_granted': False}

    def step_down_as_leader(self, preferred_candidate):
        """Step down as leader and help preferred candidate win election"""
        print(f"Node {self.node_id} stepping down with preference for node {preferred_candidate}")
        
        # Increment term
        self.current_term += 1
        self.state = 'Follower'
        self.leader_id = None
        self.voted_for = preferred_candidate  # Vote for preferred candidate in the new term
        
        # Save this state
        self.save_state()
        
        # Tell other nodes about the new term via heartbeat
        for node_id in ALL_NODES:
            if node_id != self.node_id and node_id != preferred_candidate:
                try:
                    self.send_message(node_id, {
                        'type': 'AppendEntries',
                        'term': self.current_term,
                        'leader_id': None,
                        'prev_log_index': len(self.log) - 1,
                        'prev_log_term': self.log[-1]['term'] if self.log else 0,
                        'entries': [],
                        'leader_commit': self.commit_index
                    })
                except Exception as e:
                    print(f"Error notifying node {node_id}: {e}")

    def handle_append_entries(self, message):
        """Handle incoming append entries with enhanced logging"""
        with self.lock:
            term = message['term']
            leader_id = message['leader_id']
            prev_log_index = message['prev_log_index']
            prev_log_term = message['prev_log_term']
            entries = message['entries']
            leader_commit = message['leader_commit']

            print(f"\nNode {self.node_id} processing AppendEntries from leader {leader_id}")

            if term < self.current_term:
                return {'term': self.current_term, 'success': False}

            if term > self.current_term:
                self.current_term = term
                self.voted_for = None
                self.save_state()

            self.last_heartbeat = time.time()
            self.start_election_timer()
            self.state = 'Follower'
            self.leader_id = leader_id

            # Log consistency check
            if prev_log_index >= len(self.log) or (prev_log_index >= 0 and self.log[prev_log_index]['term'] != prev_log_term):
                return {'term': self.current_term, 'success': False}

            # Append new entries
            if entries:
                print(f"Node {self.node_id} appending {len(entries)} new entries")
                # Delete conflicting entries and append new ones
                self.log = self.log[:prev_log_index + 1]
                
                # Save new entries to log file
                with open(self.log_file, 'w') as f:
                    # First write existing entries
                    for entry in self.log:
                        f.write(json.dumps(entry) + '\n')
                    # Then append new entries
                    for entry in entries:
                        entry_with_metadata = {
                            'term': entry['term'],
                            'value': entry['value'],
                            'timestamp': datetime.now().isoformat(),
                            'node_id': self.node_id
                        }
                        f.write(json.dumps(entry_with_metadata) + '\n')
                        self.log.append(entry_with_metadata)

            # Update commit index and apply entries
            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, len(self.log) - 1)
                self.apply_committed_entries()
                self.save_state()

            return {'term': self.current_term, 'success': True}

    def apply_committed_entries(self):
        """Apply committed log entries"""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]
            print(f"Node {self.node_id} applied log entry: {entry['value']}")
            print(f"Entry metadata - Term: {entry['term']}, Timestamp: {entry['timestamp']}")

    def handle_submit_value(self, message):
        """Handle client value submissions with enhanced logging"""
        if self.state != 'Leader':
            return {
                'status': 'redirect',
                'leader': self.leader_id,
                'message': f'Not the leader. Current leader is node {self.leader_id}'
            }

        # Create log entry with metadata
        entry = {
            'term': self.current_term,
            'value': message['value']
        }
        
        # Save to leader's log first
        entry_with_metadata = self.save_log_entry(entry)
        self.log.append(entry_with_metadata)

        # Try to replicate to followers
        success = self.replicate_log()
        
        if success:
            # Update state after successful replication
            self.commit_index = len(self.log) - 1
            self.save_state()
            return {
                'status': 'success',
                'term': self.current_term,
                'log_position': len(self.log) - 1,
                'timestamp': entry_with_metadata['timestamp']
            }
        else:
            # Remove entry if replication failed
            self.log.pop()
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

    def recover_without_backup(self):
        """Fallback recovery method when no backup is available"""
        self.init_log_file()
        self.load_state()
        self.state = 'Follower'
        
        success = self.sync_with_cluster()
        if success:
            self.start_election_timer()
            return {'status': 'success', 'message': f'Node {self.node_id} recovered with clean state and synchronized'}
        else:
            return {'status': 'error', 'message': 'Failed to synchronize with cluster'}

    def sync_with_cluster(self):
        """Synchronize recovering node with the current cluster state"""
        print(f"Node {self.node_id} attempting to sync with cluster...")
        
        # Only sync entries that are newer than our last entry
        last_log_index = len(self.log) - 1
        last_log_term = self.log[last_log_index]['term'] if last_log_index >= 0 else 0
        
        for node_id in ALL_NODES:
            if node_id != self.node_id:
                try:
                    # Query other node's state
                    response = self.send_message(node_id, {
                        'type': 'QueryState',
                        'term': self.current_term
                    })
                    
                    if response and response.get('status') == 'success':
                        # If this node has a higher term, update our term
                        if response['term'] > self.current_term:
                            self.current_term = response['term']
                            self.voted_for = None
                            self.save_state()
                        
                        # Request log sync from the point where we left off
                        sync_response = self.send_message(node_id, {
                            'type': 'RequestLogSync',
                            'term': self.current_term,
                            'last_log_index': last_log_index,
                            'last_log_term': last_log_term
                        })
                        
                        if sync_response and sync_response.get('status') == 'success':
                            # Only append new entries that we don't have
                            new_log = sync_response['log']
                            if len(new_log) > len(self.log):
                                # Verify consistency at the overlap point
                                if last_log_index >= 0:
                                    if new_log[last_log_index]['term'] == last_log_term:
                                        # Append only the new entries
                                        self.log.extend(new_log[last_log_index + 1:])
                                else:
                                    self.log = new_log
                                    
                            self.commit_index = min(sync_response['commit_index'], len(self.log) - 1)
                            self.last_applied = min(sync_response['last_applied'], len(self.log) - 1)
                            self.leader_id = sync_response['leader_id']
                            
                            # Save the synchronized state
                            self.reset_log_file()
                            self.save_state()
                            return True
                except Exception as e:
                    print(f"Error syncing with node {node_id}: {e}")
                    continue
        
        return False

    def handle_request_log_sync(self, message):

        """Handle requests for log synchronization from recovering nodes"""
        if message['term'] < self.current_term:
            return {
                'status': 'error',
                'term': self.current_term,
                'message': 'Term is outdated'
            }
        
        return {
            'status': 'success',
            'term': self.current_term,
            'log': self.log,
            'commit_index': self.commit_index,
            'last_applied': self.last_applied,
            'leader_id': self.leader_id
        }
    
    def handle_simulate_failure(self, message):
        """Simulate a leader failure with inconsistent log - leader has one extra entry"""
        if self.state != 'Leader':
            return {
                'status': 'redirect',
                'leader': self.leader_id,
                'message': f'Not the leader. Current leader is node {self.leader_id}'
            }
        
        try:
            print(f"Simulating failure of leader node {self.node_id} with inconsistent log")
            
            # First append new entry ONLY to leader's log
            entry = {
                'term': self.current_term,
                'value': message['value']
            }
            entry_with_metadata = self.save_log_entry(entry)
            self.log.append(entry_with_metadata)
            print(f"Leader appended new entry: {entry['value']}")
            
            # Save backup before crashing
            backup_suffix = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_dir = os.path.join(self.log_dir, f"backup_{backup_suffix}")
            os.makedirs(backup_dir, exist_ok=True)
            
            # Backup current files with the inconsistent log
            if os.path.exists(self.log_file):
                backup_log = os.path.join(backup_dir, f"node_{self.node_id}_log.json")
                with open(self.log_file, 'r') as src, open(backup_log, 'w') as dst:
                    dst.write(src.read())
            
            if os.path.exists(self.state_file):
                backup_state = os.path.join(backup_dir, f"node_{self.node_id}_state.json")
                with open(self.state_file, 'r') as src, open(backup_state, 'w') as dst:
                    dst.write(src.read())
            
            # Save backup location for recovery
            self.backup_location = backup_dir
            
            # Immediately crash - don't even try to replicate
            self.is_crashed = True
            self.stop_threads()
            
            # Clear state
            self.state = 'Follower'
            self.log = []
            self.commit_index = -1
            self.last_applied = -1
            self.leader_id = None
            self.voted_for = None
            
            # Delete current files
            if os.path.exists(self.log_file):
                os.remove(self.log_file)
            if os.path.exists(self.state_file):
                os.remove(self.state_file)
            
            print(f"Leader node {self.node_id} crashed with one extra log entry")
            return {
                'status': 'success',
                'message': f'Leader node {self.node_id} crashed with inconsistent log state'
            }
                
        except Exception as e:
            print(f"Error in failure simulation: {e}")
            return {'status': 'error', 'message': str(e)}

    def handle_simulate_recover(self):
        """Handle recovery with improved error handling"""
        try:
            print(f"Node {self.node_id} starting recovery process...")
            
            # Reset crash flag
            self.is_crashed = False
            self.running = True
            
            # Find most recent backup
            backup_dirs = [d for d in os.listdir(self.log_dir) if d.startswith('backup_')]
            if not backup_dirs:
                print(f"No backup found for node {self.node_id}, proceeding with clean recovery")
                return self.recover_without_backup()
            
            latest_backup = max(backup_dirs)
            backup_dir = os.path.join(self.log_dir, latest_backup)
            
            # Restore from backup
            backup_log = os.path.join(backup_dir, f"node_{self.node_id}_log.json")
            backup_state = os.path.join(backup_dir, f"node_{self.node_id}_state.json")
            
            # Restore state file
            if os.path.exists(backup_state):
                with open(backup_state, 'r') as src, open(self.state_file, 'w') as dst:
                    dst.write(src.read())
                self.load_state()
                print(f"Restored state from backup: Term={self.current_term}, State={self.state}")
            
            # Restore log file and entries
            if os.path.exists(backup_log):
                with open(backup_log, 'r') as src, open(self.log_file, 'w') as dst:
                    dst.write(src.read())
                
                self.log = []
                with open(self.log_file, 'r') as f:
                    for line in f:
                        if line.strip():
                            entry = json.loads(line.strip())
                            self.log.append(entry)
                print(f"Restored {len(self.log)} log entries from backup")
            
            # Start necessary threads
            self.heartbeat_thread = threading.Thread(target=self.send_heartbeat)
            self.heartbeat_thread.daemon = True
            self.heartbeat_thread.start()
            
            self.start_election_timer()
            
            print(f"Node {self.node_id} recovered successfully")
            return {
                'status': 'success',
                'message': f'Node {self.node_id} recovered successfully'
            }
                
        except Exception as e:
            print(f"Error during recovery: {e}")
            return {
                'status': 'error',
                'message': f'Recovery failed: {str(e)}'
            }

    def handle_trigger_leader_change(self):
        """Handle the perfect leader change request"""
        with self.lock:
            if self.state != 'Leader':
                return {
                    'status': 'redirect',
                    'leader': self.leader_id,
                    'message': f'Not the leader. Current leader is node {self.leader_id}'
                }

            # Ensure all entries are fully replicated before changing leader
            if not self.ensure_full_replication():
                return {
                    'status': 'error',
                    'message': 'Cannot change leader: entries not fully replicated'
                }

            old_leader = self.node_id
            
            # Find the best candidate (node with most up-to-date log)
            best_candidate = self.find_best_candidate()
            
            if best_candidate is None:
                return {
                    'status': 'error',
                    'message': 'No suitable candidate found for leader change'
                }

            # Step down and trigger election with preferred candidate
            print(f"Node {self.node_id} stepping down, preferring node {best_candidate} as new leader")
            self.step_down_as_leader(best_candidate)

            # Wait briefly for the election to complete
            time.sleep(2)

            return {
                'status': 'success',
                'old_leader': old_leader,
                'new_leader': best_candidate,
                'message': f'Leader changed from node {old_leader} to node {best_candidate}'
            }

    def ensure_full_replication(self):
        """Ensure all entries are fully replicated to a majority of nodes"""
        # First, try to replicate any pending entries
        if not self.replicate_log():
            return False
        
        up_to_date_count = 1  # Count self

        for node_id in ALL_NODES:
            if node_id != self.node_id:
                if self.match_index[node_id] == len(self.log) - 1:
                    up_to_date_count += 1

        return up_to_date_count >= len(ALL_NODES) // 2 + 1

    def find_best_candidate(self):
        """Find the best candidate for new leader based on log completeness"""
        candidates = {}
        
        # Query all nodes for their current state
        for node_id in ALL_NODES:
            if node_id != self.node_id:
                try:
                    response = self.send_message(node_id, {
                        'type': 'QueryState',
                        'term': self.current_term
                    })
                    
                    if response and response.get('status') == 'success':
                        candidates[node_id] = {
                            'log_length': response['log_length'],
                            'match_index': self.match_index[node_id]
                        }
                except Exception as e:
                    print(f"Error querying node {node_id}: {e}")
                    continue

        # Filter candidates that are fully up-to-date
        fully_synced_candidates = [
            node_id for node_id, state in candidates.items()
            if state['match_index'] == len(self.log) - 1
        ]

        if not fully_synced_candidates:
            return None

        # Among fully synced candidates, choose one randomly
        return random.choice(fully_synced_candidates)

    def handle_query_state(self, message):
        """Handle query about node's current state"""
        return {
            'status': 'success',
            'term': self.current_term,
            'log_length': len(self.log),
            'state': self.state
        }

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