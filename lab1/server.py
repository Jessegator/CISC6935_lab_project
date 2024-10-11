# socket_server_thread.py
# -*- coding:utf-8 -*-

import socket
import threading
import time
import json
import argparse
import sys

# Store workers' connections and resource usage
workers = []
tasks_assigned = 0
completed_tasks = 0
round_robin_index = 0  # For round-robin scheduling

IDLE_CPU_THRESHOLD = 10  # CPU usage threshold to detect idle workers
IDLE_TIME_THRESHOLD = 30  # Time threshold in seconds to detect idle workers

# Track task completion times
task_times = []


def link_handler(args, conn, addr):
    global tasks_assigned, completed_tasks
    buffer = ""

    while True:
        client_data = conn.recv(1024).decode()
        if client_data == "exit":
            print(f'Worker [{addr[0]}:{addr[1]}] disconnected')
            remove_worker(addr)
            break

        buffer += client_data

        # Process complete JSON messages in buffer
        while '\n' in buffer:
            message, buffer = buffer.split('\n', 1)
            if not message:
                continue

            try:
                # Parse and print resource usage
                if "task_complete" in message:
                    _, task_end_time = message.split(" ")
                    task_end_time = float(task_end_time)
                    for worker in workers:
                        if worker['addr'] == addr:
                            completion_time = task_end_time - worker['last_task_time']
                            task_times.append(completion_time)
                            completed_tasks += 1
                            worker['busy'] = False
                            print(f"Task completed by worker {addr} in {completion_time} seconds")


                else:
                    resource_status = json.loads(message)
                    for worker in workers:
                        if worker['addr'] == addr:
                            worker['cpu_usage'] = resource_status['cpu']['lavg_1']
                            worker['memory_free'] = resource_status['memory']['free']
                            worker['last_report_time'] = time.time()  # Update last report time when receiving a report
                            print(f"Worker {addr} updated: CPU={worker['cpu_usage']}, Memory Free={worker['memory_free']}")
            except json.JSONDecodeError:
                print(f'Invalid data from [{addr[0]}:{addr[1]}]: {message}')

        if tasks_assigned < args.num_tasks:
            time.sleep(10)
            if args.load_balancer == "lowest_cpu":
                # print(f"task assigned: {tasks_assigned}")
                # print(f"task complete: {completed_tasks}")
                assign_task_based_on_cpu_usage()
            elif args.load_balancer == "rr":
                assign_task_round_robin()
            else:
                raise ValueError(f"Load balancer {args.load_balancer} is not supported")

        if completed_tasks >= args.num_tasks:
            evaluate_performance()
            sys.exit(0)

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

def assign_task_to_worker(worker):
    global tasks_assigned
    if not worker['busy'] and tasks_assigned < 20:
        conn = worker['conn']
        addr = worker['addr']
        assign_task(conn, addr, "calculate_pi")
        worker['last_task_time'] = time.time()  # Update last task time
        worker['busy'] = True  # Mark worker as busy
        tasks_assigned += 1
        print(f"Task assigned to worker {addr}")
# Assign tasks based on lowest CPU usage
def assign_task_based_on_cpu_usage():
    global tasks_assigned
    if workers and tasks_assigned < 20:
        # Select the worker with the lowest CPU usage
        available_workers = [w for w in workers if not w['busy']]
        if available_workers:

            selected_worker = min(workers, key=lambda w: w['cpu_usage'])
            assign_task_to_worker(selected_worker)
            # print(f"Task assigned to worker {selected_worker['addr']} with lowest CPU usage: {selected_worker['cpu_usage']}")
        time.sleep(1)


def assign_task_round_robin():
    global tasks_assigned, round_robin_index
    if workers and tasks_assigned < 20:
        # Select the next worker using round-robin scheduling
        selected_worker = workers[round_robin_index % len(workers)]
        if not selected_worker['busy']:
            assign_task_to_worker(selected_worker)
            round_robin_index += 1
            # print(f"Task assigned to worker {selected_worker['addr']} using Round Robin")



        # Update the round-robin index to point to the next worker


        time.sleep(1)

# Function to send tasks to workers
def assign_task(conn, addr, task):
    print(f"Assigning task to worker {addr[0]}:{addr[1]}")
    conn.sendall(task.encode('utf-8'))

def evaluate_performance():
    avg_completion_time = sum(task_times) / len(task_times)
    cpu_usages = [worker['cpu_usage'] for worker in workers]
    avg_cpu_usage = sum(cpu_usages) / len(cpu_usages)
    variance_cpu_usage = sum((x - avg_cpu_usage) ** 2 for x in cpu_usages) / len(cpu_usages)
    print(f"\n--- Performance Evaluation ---")
    print(f"Average Task Completion Time: {avg_completion_time} seconds")
    print(f"Average CPU Usage: {avg_cpu_usage}")
    print(f"CPU Usage Variance: {variance_cpu_usage}")
    print(f"Completed {len(task_times)} tasks")
    print(f"--------------------------------")

# Main server function to manage worker connections and tasks
def main(args):
    global workers, tasks_assigned
    ip_port = (args.ip, args.port)
    sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sk.bind(ip_port)
    sk.listen(args.max_conn)
    print('Server started. Waiting for workers...')

    # Start the idle detection thread
    threading.Thread(target=detect_idle_workers, daemon=True).start()

    while tasks_assigned < args.num_tasks:
        conn, addr = sk.accept()
        print(f'New worker connected from [{addr[0]}:{addr[1]}]')
        workers.append({
            'conn': conn,
            'addr': addr,
            'cpu_usage': float('inf'),  # Initialize with high CPU usage
            'memory_free': float('inf'),
            'last_report_time': time.time(),
            'last_task_time': time.time(),  # Track the time of the last assigned task
            'busy': False
        })
        threading.Thread(target=link_handler, args=(args, conn, addr)).start()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="CISC-6935 lab-1 server")
    parser.add_argument("--ip", type=str, default='127.0.0.1', help="IP address")
    parser.add_argument("--port", type=int, default=9999, help="Port number")
    parser.add_argument("--max_conn", type=int, default=10, help="Maximum number of connections allowed")
    parser.add_argument("--load_balancer", type=str, default="lowest_cpu", help="Load balancing strategy")
    parser.add_argument("--num_tasks",type=int, default=20, help="Number of tasks the server will assign")
    args = parser.parse_args()
    main(args)
