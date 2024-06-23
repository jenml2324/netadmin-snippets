#!/usr/bin/python3
import concurrent.futures
import socket
import time
import pandas as pd
from tqdm import tqdm
from multiprocessing import Manager, Value, Lock
import subprocess
import re

# Read the list of IP/domain and ports
def read_targets(filename, default_ports):
    targets = []
    valid_line_pattern = re.compile(r'^[a-zA-Z0-9\.\-: ]*$')

    with open(filename, 'r') as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if not valid_line_pattern.match(line):
                raise ValueError(f"Invalid character detected in line: {line}")
            if ':' in line:
                target, port = line.split(':')
                targets.append((target.strip(), int(port)))
            elif ' ' in line:
                target, port = line.split()
                targets.append((target.strip(), int(port)))
            else:
                for port in default_ports:
                    targets.append((line, port))
    return targets

# Check the validity of the IP address
def is_valid_ip(ip):
    try:
        socket.inet_aton(ip)
        return True
    except socket.error:
        return False

# Get the IP address corresponding to the domain
def resolve_domain(target):
    if is_valid_ip(target):
        return target
    try:
        return socket.gethostbyname(target)
    except socket.gaierror:
        return None

# Parse the output of paping
def parse_paping_output(output):
    # Remove color codes
    output = re.sub(r'\x1b\[\d+(;\d+)*m', '', output)
    lines = output.splitlines()
    results = {
        'attempted': 0,
        'connected': 0,
        'failed': 0,
        'packet_loss_rate': 0.0,
        'min_time': float('inf'),
        'max_time': float('-inf'),
        'avg_time': 0
    }
    if paping_debug:
        print("👇 ***** task start ***** 👇")
        print("\n")
        if "Connecting to" in output:
            match = re.search(r"Connecting to (\d+\.\d+\.\d+\.\d+) on TCP (\d+):", output)
            if match:
                ip_address = match.group(1)
                port = match.group(2)
                print(f"⚠  {ip_address}, Port: {port}")
                print("\n")
            else:
                print("No IP address found.")

    if "Cannot resolve host" in output:
        results['attempted'] = 1
        results['connected'] = 0
        results['failed'] = 1
        results['packet_loss_rate'] = 100.0
        results['min_time'] = 0.0
        results['max_time'] = 0.0
        results['avg_time'] = 0.0
        return results

    for line in lines:
        if "Attempted" in line:
            parts = line.split(',')
            results['attempted'] = int(parts[0].split('=')[1].strip())
            results['connected'] = int(parts[1].split('=')[1].strip())
            results['failed'] = int(parts[2].split('=')[1].strip().split(' ')[0])
            results['packet_loss_rate'] = float(parts[2].split('(')[1].split(')')[0].replace('%', '').strip())
            if paping_debug:
                print("--- line start ---")
                print(line)
                print("- parsed -")
                print(results)
                print("--- line end ---")
                print("\n")
        if "Minimum" in line:
            parts = line.split(',')
            results['min_time'] = float(parts[0].split('=')[1].replace('ms', '').strip())
            results['max_time'] = float(parts[1].split('=')[1].replace('ms', '').strip())
            results['avg_time'] = float(parts[2].split('=')[1].replace('ms', '').strip())
            if paping_debug:
                print("--- line start ---")
                print(line)
                print("- parsed -")
                print(results)
                print("--- line end ---")
                print("\n")
    if paping_debug:
        print("👆 ***** task end ***** 👆")
        print("\n")
    return results

# Test TCP port connectivity of a single target using paping
def test_port_with_paping(target, port, attempts, interval, progress_counter, lock):
    ip = resolve_domain(target)
    domain = target if not is_valid_ip(target) else ''

    try:
        cmd = ['paping', ip, '-p', str(port), '-c', str(attempts)]
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        output = ""
        while True:
            line = process.stdout.readline()
            if not line:
                break
            output += line
            with lock:
                progress_counter.value += 1

        process.wait()
    except Exception as e:
        output = str(e)

    results = parse_paping_output(output)
    results['domain'] = domain
    results['ip'] = ip
    results['port'] = port

    return results

# Test TCP port connectivity of a single target using Python
def test_port_with_python(target, port, attempts, interval, progress_counter, lock):
    ip = resolve_domain(target)
    domain = target if not is_valid_ip(target) else ''
    results = {
        'domain': domain,
        'ip': ip,
        'port': port,
        'attempted': attempts,
        'connected': 0,
        'failed': 0,
        'packet_loss_rate': 0.0,
        'min_time': float('inf'),
        'max_time': float('-inf'),
        'avg_time': 0
    }

    total_time = 0

    for _ in range(attempts):
        try:
            start_time = time.time()
            with socket.create_connection((ip, port), timeout=2) as sock:
                pass
            end_time = time.time()
            elapsed_time = (end_time - start_time) * 1000

            results['connected'] += 1
            total_time += elapsed_time

            if elapsed_time < results['min_time']:
                results['min_time'] = elapsed_time
            if elapsed_time > results['max_time']:
                results['max_time'] = elapsed_time
        except socket.error:
            results['failed'] += 1

        time.sleep(interval)

        with lock:
            progress_counter.value += 1

    if results['connected'] > 0:
        results['avg_time'] = total_time / results['connected']
    else:
        results['min_time'] = 0
        results['max_time'] = 0

    results['packet_loss_rate'] = (results['failed'] / results['attempted']) * 100

    return results

# Batch testing
def run_tests(filename, attempts, interval, max_processes, default_ports, use_paping, output_file, print_results):
    targets = read_targets(filename, default_ports)
    total_pings = len(targets) * attempts

    with Manager() as manager:
        progress_counter = manager.Value('i', 0)
        lock = manager.Lock()

        results = []

        # Select the test function
        test_func = test_port_with_paping if use_paping else test_port_with_python

        # Create a process pool executor
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_processes) as executor:
            future_to_target = {
                executor.submit(
                    test_func, 
                    target, 
                    port, 
                    attempts, 
                    interval, 
                    progress_counter, 
                    lock
                ): (target, port)
                for target, port in targets
            }

            # Use tqdm progress bar
            with tqdm(total=total_pings, desc="Testing progress") as pbar:
                while progress_counter.value < total_pings:
                    with lock:
                        pbar.update(progress_counter.value - pbar.n)
                    time.sleep(0.1)

                # Handle completed tasks
                for future in concurrent.futures.as_completed(future_to_target):
                    target, port = future_to_target[future]
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as exc:
                        print(f'{target}:{port} generated an exception: {exc}')

        # Sort results by IP address and port, handle None values
        results.sort(key=lambda x: (x['ip'] if x['ip'] is not None else '', x['port']))

        # Print results
        if print_results:
            for result in results:
                domain_display = result['domain'] if result['domain'] else '-'
                print(f"{domain_display} ({result['ip']}):{result['port']} - Attempted = {result['attempted']}, "
                      f"Connected = {result['connected']}, Failed = {result['failed']}, "
                      f"Loss = {result['packet_loss_rate']:.2f}%, "
                      f"Min = {result['min_time']:.2f}ms, Max = {result['max_time']:.2f}ms, "
                      f"Avg = {result['avg_time']:.2f}ms")

        # Save results to Excel file
        df = pd.DataFrame(results)
        df = df[['domain', 'ip', 'port', 'attempted', 'connected', 'failed', 'packet_loss_rate', 'min_time', 'max_time', 'avg_time']]
        df.to_excel(output_file, index=False)

        # Display completion message
        print("\033[92m" + "Results have been saved to the Excel file successfully." + "\033[0m")

# Main function
if __name__ == '__main__':
    filename = input("Please enter the target file name (default: targets.txt): ") or 'targets.txt'
    attempts = input("Please enter the number of attempts (default: 4): ")
    attempts = int(attempts) if attempts.isdigit() else 4
    interval = input("Please enter the interval time between attempts (seconds) (default: 1): ")
    interval = float(interval) if interval.replace('.', '', 1).isdigit() else 1.0
    max_processes = input("Please enter the maximum number of concurrent processes (default: 4): ")
    max_processes = int(max_processes) if max_processes.isdigit() else 4
    default_ports_input = input("Please enter the default ports (comma separated, default: 80,443): ")
    default_ports = [int(port) for port in default_ports_input.split(',')] if default_ports_input else [80, 443]
    use_paping_input = input("Do you want to use paping for testing (y/n, default: n, selecting no will use native method for testing): ").lower()
    use_paping = use_paping_input in ['y', 'yes']
    if use_paping:
        paping_debug_input = input("Debug outputs (y/n, default: n): ").lower()
        paping_debug = paping_debug_input in ['y', 'yes']
    print_results_input = input("Do you want to print results to the screen after testing (y/n, default: y): ").lower()
    print_results = print_results_input not in ['n', 'no']
    output_file = input("Please enter the output Excel file name (default: results.xlsx): ") or 'results.xlsx'

    run_tests(filename, attempts, interval, max_processes, default_ports, use_paping, output_file, print_results)
