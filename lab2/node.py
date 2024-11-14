import threading
import socket
import time
import random
import json
import argparse
import os

ALL_NODES = {
    0: ('localhost', 54321),  # Replace with IP and port for Node 0
    1: ('localhost', 54322),  # Replace with IP and port for Node 1
    2: ('localhost', 54323),  # Replace with IP and port for Node 2
}



class Node:
    def __init__(self, node_id):
        self.node_id = node_id
        self.state = 'Follower'
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.commit_index = -1
        self.leader_id = None
        self.is_leader = False
        self.votes_received = 0
        self.election_timeout = random.randint(150, 300) / 1000.0
        self.reset_election_timeout()
        self.lock = threading.Lock()
        
        # Initialize log file
        if not os.path.exists(LOG_FILE_PATH):
            open(LOG_FILE_PATH, 'w').close()

    def reset_election_timeout(self):
        self.election_timeout_time = time.time() + self.election_timeout

    def start_server(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(ALL_NODES[self.node_id])
        server.listen(5) # listing maximum 5
        print(f"Node {self.node_id} listening on {ALL_NODES[self.node_id]}")
        while True:
            conn, addr = server.accept()
            data = conn.recv(1024)
            if data:
                message = json.loads(data.decode())
                response = self.handle_message(message)
                conn.send(json.dumps(response).encode())
            conn.close()

    def handle_message(self, message):
        with self.lock:
            if message['type'] == 'RequestVote':
                return self.handle_request_vote(message)
            elif message['type'] == 'AppendEntries':
                return self.handle_append_entries(message)
            elif message['type'] == 'SubmitValue':
                return self.handle_submit_value(message)
        return {'error': 'Unknown request type'}
    
    def handle_request_vote(self, message):
        if message['term'] > self.current_term:
            # Update term and reset state if candidate’s term is higher
            self.current_term = message['term']
            self.voted_for = None
            self.state = 'Follower'
            self.reset_election_timeout()
            
        vote_granted = False
        if (self.voted_for is None or self.voted_for == message['candidate_id']) and message['term'] >= self.current_term:
            self.voted_for = message['candidate_id']
            vote_granted = True
            self.reset_election_timeout()  # Extend timeout after voting
        
        return {'type': 'RequestVoteResponse', 'term': self.current_term, 'vote_granted': vote_granted}


    def handle_append_entries(self, message):
        self.reset_election_timeout()
        if message['term'] < self.current_term:
            return {'type': 'AppendEntriesResponse', 'term': self.current_term, 'success': False}
        self.state = 'Follower'
        self.current_term = message['term']
        self.leader_id = message['leader_id']
        if message['prev_log_index'] == len(self.log) - 1 and (len(self.log) == 0 or self.log[-1]['term'] == message['prev_log_term']):
            self.log.extend(message['entries'])
            self.commit_index = message['leader_commit']
            return {'type': 'AppendEntriesResponse', 'term': self.current_term, 'success': True}
        return {'type': 'AppendEntriesResponse', 'term': self.current_term, 'success': False}

    def handle_submit_value(self, message):
        if self.is_leader:
            self.log.append({'term': self.current_term, 'command': message['value']})
            self.commit_index = len(self.log) - 1
            self.record_log_entry(message['value'])
            self.broadcast_append_entries()
            return {'type': 'SubmitValueResponse', 'status': 'success'}
        else:
            # Redirect to leader
            leader_ip, leader_port = ALL_NODES[self.leader_id]
            return {'type': 'SubmitValueResponse', 'status': 'not_leader', 'leader_ip': leader_ip, 'leader_port': leader_port}

    def record_log_entry(self, value):
        with open(LOG_FILE_PATH, 'a') as f:
            f.write(f"<write, {value}, term: {self.current_term}>\n")
        print(f"Node {self.node_id} logged value: {value}")

    def broadcast_append_entries(self):
        for node_id, (ip, port) in ALL_NODES.items():
            if node_id != self.node_id:
                message = {
                    'type': 'AppendEntries',
                    'term': self.current_term,
                    'leader_id': self.node_id,
                    'prev_log_index': len(self.log) - 2,
                    'prev_log_term': self.log[-2]['term'] if len(self.log) > 1 else 0,
                    'entries': [self.log[-1]] if self.log else [],
                    'leader_commit': self.commit_index
                }
                threading.Thread(target=self.send_message, args=(node_id, message)).start()

    def start_election(self):
        with self.lock:
            self.state = 'Candidate'
            self.current_term += 1  # Increment term for a new election
            self.voted_for = self.node_id
            self.votes_received = 1  # Vote for self
            print(f"Node {self.node_id} started an election for term {self.current_term}")

            # Send vote requests to other nodes
            for node_id, (ip, port) in ALL_NODES.items():
                if node_id != self.node_id:
                    message = {
                        'type': 'RequestVote',
                        'term': self.current_term,
                        'candidate_id': self.node_id,
                        'last_log_index': len(self.log) - 1,
                        'last_log_term': self.log[-1]['term'] if self.log else 0
                    }
                    threading.Thread(target=self.send_message, args=(node_id, message)).start()

            # Check if the node already has a majority vote
            if self.votes_received > len(ALL_NODES) // 2:
                self.become_leader()


    def become_leader(self):
        self.state = 'Leader'
        self.is_leader = True
        self.leader_id = self.node_id
        print(f"Node {self.node_id} became the leader for term {self.current_term}")
        self.broadcast_append_entries()

    def send_message(self, node_id, message):
        ip, port = ALL_NODES[node_id]
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect((ip, port))
            client.send(json.dumps(message).encode())
            response = client.recv(1024)
            if response:
                response = json.loads(response.decode())
                if message['type'] == 'RequestVote' and response['vote_granted']:
                    with self.lock:
                        self.votes_received += 1
                        if self.votes_received > len(ALL_NODES) // 2 and self.state == 'Candidate':
                            self.become_leader()
            client.close()
        except:
            print(f"Node {self.node_id} failed to connect to Node {node_id}")

    def run(self):

        while True:
            with self.lock:
                if time.time() > self.election_timeout_time and self.state != 'Leader':
                    print(f"Node {self.node_id} election timeout; starting election for term {self.current_term + 1}")
                    self.start_election()
            if self.is_leader:
                self.broadcast_append_entries()
            time.sleep(0.1)

    def status_check(self):
        while True:
            time.sleep(5)  # Adjust the interval as needed
            if self.is_leader:
                print(f"Node {self.node_id} is currently the leader for term {self.current_term}")
            elif self.leader_id is not None:
                print(f"Node {self.node_id} sees Node {self.leader_id} as the leader for term {self.current_term}")
            else:
                print(f"Node {self.node_id} has no leader elected for term {self.current_term}")


def main(args):
    global NODE_ID, LOG_FILE_PATH
    NODE_ID = args.id
    LOG_FILE_PATH = f"node_{NODE_ID}_log.txt"
    # time.sleep(5)
    node = Node(NODE_ID)
    threading.Thread(target=node.start_server).start()
    threading.Thread(target=node.status_check).start()
    node.run()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="CISC-6935 lab-2 node")
    parser.add_argument("--id", type=int, default=0, help="node id")
    args = parser.parse_args()
    main(args)
