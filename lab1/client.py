# socket_client.py
# -*- coding:utf-8 -*-

import socket
import threading
import time
from monitor_lib import collect_resource_status
import random
import argparse


def report_resource_usage(s):
    while True:
        resource_status = collect_resource_status()
        s.sendall((resource_status + '\n').encode('utf-8'))
        time.sleep(5)  # Report every 5 seconds

# Function to handle tasks received from the server
def handle_workload(s):
    while True:
        task = s.recv(1024).decode()
        if task == "calculate_pi":
            result = calculate_pi()
            # time.sleep(random.randint(1, 5))  # Simulate processing time
            task_end_time = time.time()  # Record task end time
            print(f"Calculated Pi: {result}, completed at {task_end_time}")
            s.sendall(f"task_complete {task_end_time}\n".encode('utf-8'))
        elif task == "exit":
            print("Exiting...")
            break

# Monte Carlo Method for Calculating Pi
def calculate_pi(num_samples=1000000):
    inside_circle = 0

    for _ in range(num_samples):
        x = random.uniform(0, 1)
        y = random.uniform(0, 1)

        if x ** 2 + y ** 2 <= 1:
            inside_circle += 1

    pi_estimate = (inside_circle / num_samples) * 4

    return pi_estimate

def main(args):
    ip_port = (args.ip, args.port)
    s = socket.socket()
    s.connect(ip_port)
    print("Connected to the server...")

    # Start threads for reporting resource usage and handling workload
    threading.Thread(target=report_resource_usage, args=(s,)).start()
    threading.Thread(target=handle_workload, args=(s,)).start()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="CISC-6935 lab-1 client")
    parser.add_argument("--ip", type=str, default='127.0.0.1', help="IP address")
    parser.add_argument("--port", type=int, default=9999, help="Port number")
    args = parser.parse_args()
    main(args)
