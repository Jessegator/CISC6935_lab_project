import socket
import json
import random

# Define all known node IPs and ports
ALL_NODES = {
    0: ('localhost', 54321),  # Replace with IP and port for Node 0
    1: ('localhost', 54322),  # Replace with IP and port for Node 1
    2: ('localhost', 54323),  # Replace with IP and port for Node 2
}

def submit_value(value):
    # Start with a random node from the cluster
    node_ip, node_port = random.choice(ALL_NODES)
    while True:
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect((node_ip, node_port))
            
            # Send the SubmitValue request
            message = {'type': 'SubmitValue', 'value': value}
            client.send(json.dumps(message).encode())
            response = json.loads(client.recv(1024).decode())
            
            if response['status'] == 'success':
                print(f"Value '{value}' submitted successfully to the leader at {node_ip}:{node_port}.")
                break
            elif response['status'] == 'not_leader':
                # Redirect to the leader if known
                node_ip, node_port = response['leader_ip'], response['leader_port']
                print(f"Redirected to new leader at {node_ip}:{node_port}")
            client.close()
        except Exception as e:
            # If there's an error, select a new random node and retry
            print(f"Failed to submit value to {node_ip}:{node_port}: {e}")
            node_ip, node_port = random.choice(ALL_NODES)

if __name__ == '__main__':
    value = input("Enter a value to submit: ")
    submit_value(value)
