# socket_server_thread.py
# -*- coding:utf-8 -*-

import socket
import threading
import time
import json
import argparse

workers = []
tasks_assigned = 0
round_robin_index = 0
IDLE_CPU_THRESHOLD = 10  # CPU usage threshold in percentage to detect idle workers
IDLE_TIME_THRESHOLD = 30 # Time in seconds for a worker to be considered idle

def link_handler(args, conn, addr):
    global tasks_assigned
    buffer = ""

    while True:
        client_data = conn.recv(1024).decode()
        if client_data == "exit":
            print(f'Worker [{addr[0]}:{addr[1]}] disconnected')
            break

        buffer += client_data

        # Process complete JSON messages in buffer
        while '\n' in buffer:
            message, buffer = buffer.split('\n', 1)
            if not message:
                continue

            try:
                # Parse and print resource usage
                resource_status = json.loads(message)
                for worker in workers:
                    if worker['addr'] == addr:
                        worker['cpu_usage'] = resource_status['cpu']['lavg_1']
                        worker['memory_free'] = resource_status['memory']['free']
                        worker['last_report_time'] = time.time()  # Update the last report time
                        print(f"Worker {addr} updated: CPU={worker['cpu_usage']}, Memory Free={worker['memory_free']}")
            except json.JSONDecodeError:
                print(f'Invalid data from [{addr[0]}:{addr[1]}]: {message}')

        # Assign a new workload every 10 seconds
        if tasks_assigned < 20:
            time.sleep(10)
            if args.load_balancer == "lowest_cpu":
                assign_task_based_on_cpu_usage()
            elif args.load_balancer == "rr":
                assign_task_round_robin()
            else:
                raise ValueError(f"Load balancer {args.load_balancer} is not supported")
            tasks_assigned += 1


    conn.close()


def remove_worker(addr):
    global workers
    workers = [worker for worker in workers if worker['addr'] != addr]

# Function to detect idle workers
def detect_idle_workers():
    while True:
        current_time = time.time()
        idle_workers = []
        for worker in workers:
            # cpu_idle = worker['cpu_usage'] < IDLE_CPU_THRESHOLD
            time_idle = (current_time - worker['last_task_time']) > IDLE_TIME_THRESHOLD  # Time since last task, not report
            if time_idle:
                idle_workers.append(worker)
                print(f"Worker {worker['addr']} is idle (CPU Usage: {worker['cpu_usage']}, Idle for {current_time - worker['last_task_time']} seconds)")
        time.sleep(5)  # Check every 5 seconds


# Assign tasks based on lowest CPU usage
def assign_task_based_on_cpu_usage():

    if workers:
        # Select the worker with the lowest CPU usage
        selected_worker = min(workers, key=lambda w: w['cpu_usage'])
        conn = selected_worker['conn']
        addr = selected_worker['addr']

        # Assign the task to the selected worker
        assign_task(conn, addr, "calculate_pi")
        selected_worker['last_task_time'] = time.time()

        print(f"Task assigned to worker {addr} with lowest CPU usage: {selected_worker['cpu_usage']}")

def assign_task_round_robin():
    global round_robin_index
    if workers:
        # Select the next worker using round robin scheduling
        selected_worker = workers[round_robin_index % len(workers)]
        conn = selected_worker['conn']
        addr = selected_worker['addr']

        # Assign the task to the selected worker
        assign_task(conn, addr, "calculate_pi")
        selected_worker['last_task_time'] = time.time()

        # Update the round-robin index to point to the next worker
        round_robin_index += 1
        print(f"Task assigned to worker {addr} using Round Robin")

# Function to send tasks to workers
def assign_task(conn, addr, task):
    print(f"Assigning task to worker {addr[0]}:{addr[1]}")
    conn.sendall(task.encode('utf-8'))


# Main server function to manage worker connections and tasks
def main(args,):
    global workers
    ip_port = (args.ip, args.port)
    sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sk.bind(ip_port)
    sk.listen(args.max_conn)
    print('Server started. Waiting for workers...')

    # Start the idle detection thread
    threading.Thread(target=detect_idle_workers, daemon=True).start()

    while True:
        conn, addr = sk.accept()
        print(f'New worker connected from [{addr[0]}:{addr[1]}]')
        workers.append({
            'conn': conn,
            'addr': addr,
            'cpu_usage': float('inf'),  # Initialize with high CPU usage
            'memory_free': float('inf'),
            'last_report_time': time.time(),
            'last_task_time': time.time()  # Track the time of the last assigned task
        })
        threading.Thread(target=link_handler, args=(args, conn, addr)).start()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="CISC-6935 lab-1 server")
    parser.add_argument("--ip", type=str, default='127.0.0.1', help="IP address")
    parser.add_argument("--port", type=int, default=64432, help="Port number")
    parser.add_argument("--max_conn", type=int, default=10, help="Maximum number of connections allowed")
    parser.add_argument("--load_balancer", type=str, default="lowest_cpu", help="Load balancing strategy")
    parser.add_argument("--num_tasks",type=int, default=20, help="Number of tasks the server will assign")
    args = parser.parse_args()
    main(args)
