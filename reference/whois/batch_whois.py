import os
import subprocess
import argparse
import pandas as pd
from openpyxl import Workbook

def read_rr_list(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            return [line.strip() for line in f]
    else:
        return []

def query_asn(asn, rr):
    command = f"whois -h {rr} -- '-i origin {asn}'"
    return subprocess.getoutput(command)

def query_ip(ip, rr):
    command = f"whois -h {rr} -- '{ip}'"
    return subprocess.getoutput(command)

def parse_whois_output(output):
    results = set()  # Use a set to automatically handle duplicates
    for block in output.split('\n\n'):
        route = None
        descr = None
        origin = None
        for line in block.split('\n'):
            if line.startswith('route:') or line.startswith('route6:'):
                route = line.split(':', 1)[1].strip()
            elif line.startswith('descr:'):
                descr = line.split(':', 1)[1].strip()
            elif line.startswith('origin:'):
                origin = line.split(':', 1)[1].strip()
        if route and descr and origin:
            results.add((origin, route, descr))  # Adding to a set ensures uniqueness
    return list(results)

def extract_asn_from_ip_output(output):
    for line in output.split('\n'):
        if line.startswith('origin:'):
            return line.split(':', 1)[1].strip()
    return None

def ensure_directory_exists(path):
    if path and not os.path.exists(path):
        os.makedirs(path)

def write_to_text(output_path, identifier, ipv4_data, ipv6_data, asn=None):
    ensure_directory_exists(output_path)

    if asn:
        identifier = f"{identifier}_{asn}".lower()
    else:
        identifier = identifier.lower()

    ipv4_path = os.path.join(output_path, f"{identifier}_ipv4.txt")
    ipv6_path = os.path.join(output_path, f"{identifier}_ipv6.txt")

    with open(ipv4_path, 'w') as f:
        for route in ipv4_data:
            f.write(f"{route}\n")

    with open(ipv6_path, 'w') as f:
        for route in ipv6_data:
            f.write(f"{route}\n")

def write_to_excel(output_path, data):
    ensure_directory_exists(os.path.dirname(output_path))
    df = pd.DataFrame(data, columns=['Origin', 'Route', 'Description'])
    df.to_excel(output_path.lower(), index=False)

def read_items_from_file(file_path):
    with open(file_path, 'r') as f:
        return [line.strip() for line in f]

def handle_queries(items, rr, query_type, output_format, output_path):
    data = set()  # Use a set to automatically handle duplicates

    for item in items:
        if query_type == 'AS':
            if not item.upper().startswith('AS'):
                item = 'AS' + item.upper()
            output = query_asn(item, rr)
            parsed_data = parse_whois_output(output)
        elif query_type == 'IP':
            output = query_ip(item, rr)
            asn = extract_asn_from_ip_output(output)
            if asn:
                output = query_asn(asn, rr)
                parsed_data = parse_whois_output(output)
            else:
                print(f"No ASN found for the IP/CIDR: {item}")
                continue

        ipv4_data = [route for origin, route, descr in parsed_data if ':' not in route]
        ipv6_data = [route for origin, route, descr in parsed_data if ':' in route]

        if output_format == 'text':
            write_to_text(output_path, item, ipv4_data, ipv6_data, asn if query_type == 'IP' else None)
        elif output_format == 'excel':
            data.update(parsed_data)  # Use update to add parsed_data to the set
    
    if output_format == 'excel':
        write_to_excel(output_path, list(data))  # Convert set back to list for writing
    
    print("Query and export complete.")

def interactive_mode(rr_list):
    print("Routing Registries (Whois servers) available:")
    for i, rr in enumerate(rr_list, start=1):
        print(f"{i}. {rr}")
    
    rr_choice = input("Choose a Routing Registry by number or type the RR directly: ")
    if rr_choice.isdigit():
        rr = rr_list[int(rr_choice) - 1]
    else:
        rr = rr_choice.strip()

    query_type = input("Choose query type (1: ASN, 2: IP/CIDR): ").strip()
    if query_type == '1':
        query_type = 'AS'
    elif query_type == '2':
        query_type = 'IP'
    else:
        print("Invalid choice.")
        return

    items = input("Enter ASNs or IP/CIDR (comma-separated) or file path: ").strip().split(',')
    output_format = input("Choose output format (1: Text, 2: Excel): ").strip()
    if output_format == '1':
        output_format = 'text'
        output_path = input("Enter output directory: ").strip()
    elif output_format == '2':
        output_format = 'excel'
        output_path = input("Enter output path (with .xlsx extension): ").strip()
    else:
        print("Invalid choice.")
        return

    if os.path.isfile(items[0]):
        items = read_items_from_file(items[0])
    
    handle_queries(items, rr, query_type, output_format, output_path)

def command_line_mode(args):
    if os.path.isfile(args.items[0]):
        items = read_items_from_file(args.items[0])
    else:
        items = args.items

    handle_queries(items, args.rr, args.query_type, args.output_format, args.output)

def main():
    parser = argparse.ArgumentParser(description="Batch query routing information using Whois.")
    parser.add_argument('-r', '--rr', type=str, help="Routing Registry (Whois server)")
    parser.add_argument('-i', '--items', nargs='+', help="List of ASNs or IP/CIDR or file containing them")
    parser.add_argument('-o', '--output', type=str, help="Output directory or file path")
    parser.add_argument('-f', '--output_format', choices=['text', 'excel'], type=str.lower, help="Output format (text or excel)")
    parser.add_argument('-t', '--query_type', choices=['AS', 'IP'], type=str.upper, help="Type of queries (AS or IP)")
    
    args = parser.parse_args()

    if args.rr and args.items and args.output and args.output_format and args.query_type:
        command_line_mode(args)
    else:
        rr_list = read_rr_list('routing_registries')
        interactive_mode(rr_list)

if __name__ == "__main__":
    main()
