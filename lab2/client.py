import socket
import json
import random
import time
import sys

ALL_NODES = {
    0: ('localhost', 10001),
    1: ('localhost', 10002),
    2: ('localhost', 10003)
}

class Client:
    def __init__(self):
        self.current_leader = None
        self.last_known_leader = None

    def send_message(self, node_id, message):
        """Send message to a specific node"""
        ip, port = ALL_NODES[node_id]
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
                client.connect((ip, port))
                client.sendall(json.dumps(message).encode())
                response = json.loads(client.recv(1024).decode())
                return response
        except Exception as e:
            print(f"Error communicating with node {node_id}: {e}")
            return None

    def submit_value(self, value):
        """Submit a value to the cluster"""
        target_node = self.last_known_leader if self.last_known_leader is not None else random.choice(list(ALL_NODES.keys()))
        
        max_retries = 5
        retries = 0
        
        while retries < max_retries:
            print(f"Attempting to submit value to node {target_node}...")
            response = self.send_message(target_node, {
                'type': 'SubmitValue',
                'value': value
            })
            
            if response is None:
                print(f"Node {target_node} is unreachable")
                target_node = random.choice(list(ALL_NODES.keys()))
                retries += 1
                continue
                
            if response['status'] == 'success':
                self.last_known_leader = target_node
                print(f"Successfully submitted value: {value}")
                return True
            
            elif response['status'] == 'redirect':
                print(f"Redirecting to leader node {response['leader']}")
                target_node = response['leader']
                self.last_known_leader = target_node
                retries += 1
                continue
            
            elif response['status'] == 'error':
                target_node = random.choice(list(ALL_NODES.keys()))
                retries += 1
                continue
            else:
                print(f"Error: {response.get('message', 'Unknown error')}")
                retries += 1
                
        print("Failed to submit value after maximum retries")
        return False

    def simulate_node_failure(self):
        """
        Simulate a scenario where leader crashes after appending an entry but before replication,
        creating log inconsistency.
        """
        if self.last_known_leader is None:
            print("No known leader to simulate failure. Finding leader first...")
            # Try to find leader by submitting a value
            if not self.submit_value("test_value"):
                print("Failed to find leader")
                return False
        
        leader_id = self.last_known_leader
        print(f"Starting inconsistent leader failure simulation for leader node {leader_id}")
        
        # Step 1: Submit a special value that triggers immediate crash after append
        response = self.send_message(leader_id, {
            'type': 'SimulateFailure',
            'value': f"special_value_before_crash_{time.time()}"
        })
        
        if not response or response.get('status') != 'success':
            print("Failed to initiate inconsistent failure simulation")
            return False
            
        print("Successfully simulated inconsistent leader failure")
        print("Leader has crashed with one unrepublicated log entry")
        return True

    def simulate_node_recovery(self, node_id):
        """Simulate recovery of a specific node"""
        print(f"Simulating recovery of node {node_id}...")
        response = self.send_message(node_id, {
            'type': 'SimulateRecover'
        })
        if response and response['status'] == 'success':
            print(f"Node {node_id} recovery simulated successfully")
        else:
            print(f"Failed to simulate node recovery: {response.get('message', 'Unknown error')}")

    def trigger_leader_change(self):
        """Trigger a perfect leader change"""
        if self.last_known_leader is None:
            print("No known leader to change from. Trying random node...")
            target_node = random.choice(list(ALL_NODES.keys()))
        else:
            target_node = self.last_known_leader
            
        response = self.send_message(target_node, {
            'type': 'TriggerLeaderChange'
        })
        
        if response and response['status'] == 'success':
            print(f"Leader change initiated successfully")
            print(f"Old leader: Node {response['old_leader']}")
            print(f"New leader: Node {response['new_leader']}")
            self.last_known_leader = response['new_leader']
        else:
            print(f"Failed to trigger leader change: {response.get('message', 'Unknown error')}")

    def print_help(self):
        """Print available commands"""
        print("\nAvailable commands:")
        print("  submit <value>    - Submit a value to the cluster")
        print("  fail_leader       - Simulate failure of the current leader")
        print("  recover <node_id> - Simulate recovery of a failed node")
        print("  change_leader     - Trigger a perfect leader change")
        print("  help              - Show this help message")
        print("  exit              - Exit the client")

def main():
    client = Client()
    print("Client Started...")
    print("Type 'help' for available commands")

    while True:
        try:
            command = input("\nEnter command: ").strip().split()
            
            if not command:
                continue

            if command[0] == "exit":
                break

            elif command[0] == "help":
                client.print_help()

            elif command[0] == "submit":
                if len(command) < 2:
                    print("Usage: submit <value>")
                    continue
                value = " ".join(command[1:])
                client.submit_value(value)

            elif command[0] == "change_leader":
                client.trigger_leader_change()

            elif command[0] == "fail_leader":
                client.simulate_node_failure()

            elif command[0] == "recover":
                if len(command) != 2:
                    print("Usage: recover <node_id>")
                    continue
                try:
                    node_id = int(command[1])
                    if node_id not in ALL_NODES:
                        print(f"Invalid node_id. Must be one of {list(ALL_NODES.keys())}")
                        continue
                    client.simulate_node_recovery(node_id)
                except ValueError:
                    print("Node ID must be a number")

            else:
                print(f"Unknown command: {command[0]}")
                print("Type 'help' for available commands")

        except KeyboardInterrupt:
            print("\nExiting...")
            break
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()