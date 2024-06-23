#!/usr/bin/python3
import pandas as pd
import ipaddress
import openpyxl
from tqdm import tqdm
import threading
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
import re
import argparse
import gzip
import zipfile
import csv
import os
import sys

class LoadingAnimation:
    def __init__(self):
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self.animate)

    def start(self):
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        self.thread.join()

    def animate(self):
        while not self.stop_event.is_set():
            for frame in "|/-\\":
                sys.stderr.write(f"\r\033[2KLoading GeoIP data {frame}")
                time.sleep(0.1)
        sys.stderr.write(f"\r\033[2KLoading GeoIP data done\n")

def load_geoip_data(geoip_file):
    """Load GeoIP data from the CSV file into a DataFrame, and separate IPv4 and IPv6 addresses."""
    geoip_data = read_csv_file(geoip_file)
    geoip_data['start_ip'] = geoip_data['start_ip'].apply(ipaddress.ip_address)
    geoip_data['end_ip'] = geoip_data['end_ip'].apply(ipaddress.ip_address)
    ipv4_data = geoip_data[geoip_data['start_ip'].apply(lambda x: x.version) == 4]
    ipv6_data = geoip_data[geoip_data['start_ip'].apply(lambda x: x.version) == 6]
    return ipv4_data, ipv6_data

def read_csv_file(file_path):
    """Read CSV file with support for gzip and zip formats."""
    if file_path.endswith('.csv.gz'):
        with gzip.open(file_path, mode='rt', newline='', encoding='utf-8') as gzfile:
            geoip_data = pd.read_csv(gzfile)
    elif file_path.endswith('.csv.zip'):
        with zipfile.ZipFile(file_path, 'r') as zipf:
            for file_name in zipf.namelist():
                if file_name.endswith('.csv'):
                    with zipf.open(file_name) as csvfile:
                        geoip_data = pd.read_csv(csvfile)
    else:
        geoip_data = pd.read_csv(file_path)
    
    return detect_format_and_load(geoip_data)

def detect_format_and_load(geoip_data):
    """Detect CSV format and return standardized DataFrame."""
    if 'start_ip' in geoip_data.columns and 'end_ip' in geoip_data.columns and 'country' in geoip_data.columns:
        sys.stderr.write(f"\r\033[2Kipinfo.io country.csv format detected\n")
    elif geoip_data.shape[1] == 3 and all('.' in str(field) for field in geoip_data.iloc[0, :2]):
        geoip_data.columns = ['start_ip', 'end_ip', 'country']
        sys.stderr.write(f"\r\033[2Kdbip-country-lite format detected\n")
    elif 'ip_version' in geoip_data.columns and 'start_ip' in geoip_data.columns and 'end_ip' in geoip_data.columns and 'country_code' in geoip_data.columns:
        geoip_data = geoip_data.rename(columns={'country_code': 'country'})
        sys.stderr.write(f"\r\033[2Kipapi.is csv format detected\n")
    else:
        raise ValueError("Unknown CSV format.")
    
    return geoip_data

def find_country_continent_cidr(ip_series, ipv4_data, ipv6_data, start_index, progress_bar, is_text_file=False):
    """Find country, continent, and CIDR for each IP address or CIDR block in the series."""
    countries, continents, cidrs, errors = [], [], [], []

    for i, ip in enumerate(ip_series):
        try:
            # Handle CIDR blocks
            if '/' in ip:
                network = ipaddress.ip_network(ip, strict=False)
                if network.version == 4:
                    match = ipv4_data[(ipv4_data['start_ip'] <= network.network_address) &
                                      (ipv4_data['end_ip'] >= network.broadcast_address)]
                else:
                    match = ipv6_data[(ipv6_data['start_ip'] <= network.network_address) &
                                      (ipv6_data['end_ip'] >= network.broadcast_address)]
            else:
                ip_addr = ipaddress.ip_address(ip)
                if ip_addr.version == 4:
                    match = ipv4_data[(ipv4_data['start_ip'] <= ip_addr) &
                                      (ipv4_data['end_ip'] >= ip_addr)]
                else:
                    match = ipv6_data[(ipv6_data['start_ip'] <= ip_addr) &
                                      (ipv6_data['end_ip'] >= ip_addr)]
            
            if not match.empty:
                row = match.iloc[0]
                start_ip, end_ip = row['start_ip'], row['end_ip']
                network = ipaddress.summarize_address_range(start_ip, end_ip)
                cidr = ', '.join(str(net) for net in network)
                countries.append(row['country'])
                continents.append(row['continent_name'])
                cidrs.append(cidr)
            else:
                countries.append(None)
                continents.append(None)
                cidrs.append(None)
                errors.append(f"No match found for IP/CIDR '{ip}'")
        except ValueError as e:
            row_number = start_index + i + 1 if is_text_file else start_index + i + 2  # Adjust for zero-based index and header row
            errors.append(f"[Skipped] Error processing IP '{ip}' at row {row_number}: {e}")
            countries.append(None)
            continents.append(None)
            cidrs.append(None)
        
        # Update progress bar
        progress_bar.update(1)
    
    return countries, continents, cidrs, errors

def column_letter_to_index(letter):
    """Convert Excel column letter to zero-based column index."""
    letter = letter.upper()
    index = 0
    for char in letter:
        index = index * 26 + (ord(char) - ord('A')) + 1
    return index - 1

def process_chunk(chunk, ipv4_data, ipv6_data, start_index, position, is_text_file=False):
    """Process a chunk of the DataFrame."""
    ip_series = chunk['IP']
    description = f"Processing IPs in chunk starting at {'line' if is_text_file else 'row'} {start_index + 1}"
    with tqdm(total=len(ip_series), desc=description, position=position) as progress_bar:
        countries, continents, cidrs, errors = find_country_continent_cidr(ip_series, ipv4_data, ipv6_data, start_index, progress_bar, is_text_file)
    chunk['Country'] = countries
    chunk['Continent'] = continents
    chunk['CIDR'] = cidrs
    chunk['OriginalIndex'] = chunk.index  # Add original index
    return chunk, errors

def save_output(dataframe, output_file, file_format):
    """Save the processed DataFrame to the specified file format."""
    if file_format == 'excel':
        dataframe.to_excel(output_file, index=False, engine='openpyxl')
    elif file_format == 'csv':
        dataframe.to_csv(output_file, index=False)

def process_excel(input_excel, output_file, ip_column_letter, geoip_file, chunk_size, file_format):
    """Process the Excel file, add country, continent, and CIDR columns."""
    df = pd.read_excel(input_excel, engine='openpyxl')

    ip_column_index = column_letter_to_index(ip_column_letter)
    ip_column = df.columns[ip_column_index]
    df.rename(columns={ip_column: 'IP'}, inplace=True)

    animation = LoadingAnimation()
    animation.start()

    ipv4_data, ipv6_data = load_geoip_data(geoip_file)

    animation.stop()

    df['IP'] = df['IP'].astype(str)

    chunks = [df.iloc[i:i + chunk_size] for i in range(0, df.shape[0], chunk_size)]
    results, all_errors = [], []

    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(process_chunk, chunk, ipv4_data, ipv6_data, chunk.index.start, position) for position, chunk in enumerate(chunks)]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing chunks"):
            chunk_result, chunk_errors = future.result()
            results.append(chunk_result)
            all_errors.extend(chunk_errors)

    processed_df = pd.concat(results).sort_values(by='OriginalIndex').drop(columns=['OriginalIndex'])
    processed_df = processed_df.dropna(subset=['Country'])
    save_output(processed_df, output_file, file_format)

    if all_errors:
        sys.stderr.write("\033c")  # ANSI escape sequence to clear the screen
        sys.stderr.write("Errors encountered during processing:\n")
        for error in all_errors:
            sys.stderr.write(f"{error}\n")

    sys.stderr.write("-" * 80)
    print(f"Processed data has been written to {output_file}")

def process_text_file(text_file, output_file, geoip_file, chunk_size, file_format):
    """Process a text file with IP addresses, add country, continent, and CIDR columns."""
    with open(text_file, 'r') as file:
        lines = file.readlines()
    
    ip_addresses = [re.split(r'[:\s]', re.sub(r'#.*', '', line).strip())[0] for line in lines if re.split(r'[:\s]', re.sub(r'#.*', '', line).strip())[0]]

    df = pd.DataFrame(ip_addresses, columns=['IP'])

    animation = LoadingAnimation()
    animation.start()

    ipv4_data, ipv6_data = load_geoip_data(geoip_file)

    animation.stop()

    df['IP'] = df['IP'].astype(str)

    chunks = [df.iloc[i:i + chunk_size] for i in range(0, df.shape[0], chunk_size)]
    results, all_errors = [], []

    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(process_chunk, chunk, ipv4_data, ipv6_data, chunk.index.start, position, True) for position, chunk in enumerate(chunks)]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing chunks"):
            chunk_result, chunk_errors = future.result()
            results.append(chunk_result)
            all_errors.extend(chunk_errors)

    processed_df = pd.concat(results).sort_values(by='OriginalIndex').drop(columns=['OriginalIndex'])
    processed_df = processed_df.dropna(subset=['Country'])
    save_output(processed_df, output_file, file_format)

    if all_errors:
        sys.stderr.write("\033c")  # ANSI escape sequence to clear the screen
        sys.stderr.write("Errors encountered during processing:\n")
        for error in all_errors:
            sys.stderr.write(f"{error}\n")

    sys.stderr.write("-" * 80)
    print(f"Processed data has been written to {output_file}")

def parse_arguments():
    parser = argparse.ArgumentParser(description="Process IP addresses to add country, continent, and CIDR information.")
    parser.add_argument('--input_type', choices=['excel', 'text'], help="Input type ('excel' or 'text', default: text).", default='text')
    parser.add_argument('--output_format', choices=['excel', 'csv'], help="Output file format ('excel' or 'csv', default: csv).", default='csv')
    parser.add_argument('--input_file', help="Input file path.")
    parser.add_argument('--output_file', help="Output file path.")
    parser.add_argument('--ip_column_letter', help="Column letter for IP addresses (only for Excel).")
    parser.add_argument('--geoip_file', help="GeoIP data file path (default: 'country.csv').", default='country.csv')
    parser.add_argument('--chunk_size', type=int, help="Number of rows per chunk (default: 500).", default=500)
    return parser.parse_args()

def main():
    args = parse_arguments()

    if args.input_type and args.output_format and args.input_file and args.output_file:
        if args.input_type == 'excel':
            ip_column_letter = args.ip_column_letter or 'L'
            process_excel(args.input_file, args.output_file, ip_column_letter, args.geoip_file, args.chunk_size, args.output_format)
        elif args.input_type == 'text':
            process_text_file(args.input_file, args.output_file, args.geoip_file, args.chunk_size, args.output_format)
    else:
        input_type = input("Input type ('excel' or 'text'), default: text: ").strip().lower() or args.input_type
        if input_type == 'excel':
            input_excel = input("Input Excel file path (default: 'input.xlsx'): ") or 'input.xlsx'
            ip_column_letter = input("Column letter for IP addresses (default: 'L'): ") or 'L'
        elif input_type == 'text':
            text_file = input("Input text file path (default: 'input.txt'): ") or 'input.txt'

        output_format = input("Output file format ('excel' or 'csv'), default: csv: ").strip().lower() or args.output_format

        default_output_extension = 'xlsx' if output_format == 'excel' else 'csv'
        output_file = input(f"Output file path (default: 'output.{default_output_extension}'): ") or f'output.{default_output_extension}'
 
        geoip_file = input("GeoIP data file path (default: 'country.csv'): ") or 'country.csv'
        chunk_size = input("Number of rows per chunk (default: 500): ") or 500

        chunk_size = int(chunk_size)  # Convert chunk_size to integer

        if input_type == 'excel':
            process_excel(input_excel, output_file, ip_column_letter, geoip_file, chunk_size, output_format)
        elif input_type == 'text':
            process_text_file(text_file, output_file, geoip_file, chunk_size, output_format)

if __name__ == "__main__":
    main()
