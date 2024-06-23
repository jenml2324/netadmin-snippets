#!/usr/bin/python3
import pandas as pd
import ipaddress
import openpyxl
from tqdm import tqdm
import threading
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
import re

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
                print(f"\rLoading GeoIP data {frame}", end="", flush=True)
                time.sleep(0.1)
        print("\rLoading GeoIP data done!   ", flush=True)  # Clear the line after stopping

def load_geoip_data(geoip_file):
    """Load GeoIP data from the CSV file into a DataFrame, and separate IPv4 and IPv6 addresses."""
    geoip_data = pd.read_csv(geoip_file)
    geoip_data['start_ip'] = geoip_data['start_ip'].apply(ipaddress.ip_address)
    geoip_data['end_ip'] = geoip_data['end_ip'].apply(ipaddress.ip_address)
    ipv4_data = geoip_data[geoip_data['start_ip'].apply(lambda x: x.version) == 4]
    ipv6_data = geoip_data[geoip_data['start_ip'].apply(lambda x: x.version) == 6]
    return ipv4_data, ipv6_data

def find_country_continent_cidr(ip_series, ipv4_data, ipv6_data, start_index, progress_bar, is_text_file=False):
    """Find country, continent, and CIDR for each IP address in the series."""
    countries = []
    continents = []
    cidrs = []
    errors = []

    for i, ip in enumerate(ip_series):
        try:
            ip_addr = ipaddress.ip_address(ip)
            if ip_addr.version == 4:
                match = ipv4_data[(ipv4_data['start_ip'] <= ip_addr) &
                                  (ipv4_data['end_ip'] >= ip_addr)]
            else:
                match = ipv6_data[(ipv6_data['start_ip'] <= ip_addr) &
                                  (ipv6_data['end_ip'] >= ip_addr)]
            
            if not match.empty:
                row = match.iloc[0]
                start_ip = row['start_ip']
                end_ip = row['end_ip']
                network = ipaddress.summarize_address_range(start_ip, end_ip)
                cidr = ', '.join(str(net) for net in network)
                countries.append(row['country'])
                continents.append(row['continent_name'])
                cidrs.append(cidr)
            else:
                countries.append(None)
                continents.append(None)
                cidrs.append(None)
        except ValueError as e:
            row_number = start_index + i + 1 if is_text_file else start_index + i + 2  # Adjust for zero-based index and header row
            errors.append(f"[Skiped] Error processing IP '{ip}' at row {row_number}: {e}")
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
    # Load the Excel file
    df = pd.read_excel(input_excel, engine='openpyxl')

    # Convert column letter to index
    ip_column_index = column_letter_to_index(ip_column_letter)
    ip_column = df.columns[ip_column_index]
    df.rename(columns={ip_column: 'IP'}, inplace=True)

    # Start loading animation
    animation = LoadingAnimation()
    animation.start()

    # Load GeoIP data
    ipv4_data, ipv6_data = load_geoip_data(geoip_file)

    # Stop loading animation
    animation.stop()

    # Ensure the IP column is treated as string
    df['IP'] = df['IP'].astype(str)

    # Process the DataFrame in chunks
    chunks = [df.iloc[i:i + chunk_size] for i in range(0, df.shape[0], chunk_size)]
    results = []
    all_errors = []

    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(process_chunk, chunk, ipv4_data, ipv6_data, chunk.index.start, position) for position, chunk in enumerate(chunks)]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing chunks"):
            chunk_result, chunk_errors = future.result()
            results.append(chunk_result)
            all_errors.extend(chunk_errors)

    # Combine the processed chunks and sort by OriginalIndex to maintain original order
    processed_df = pd.concat(results).sort_values(by='OriginalIndex').drop(columns=['OriginalIndex'])

    # Save the updated DataFrame to the specified file format
    save_output(processed_df, output_file, file_format)

    # Clear screen and print errors if any
    if all_errors:
        print("\033c", end="")  # ANSI escape sequence to clear the screen
        print("Errors encountered during processing:")
        for error in all_errors:
            print(error)

    # Print a line of dashes
    print("-" * 80)
    print(f"Processed data has been written to {output_file}")

def process_text_file(text_file, output_file, geoip_file, chunk_size, file_format):
    """Process a text file with IP addresses, add country, continent, and CIDR columns."""
    with open(text_file, 'r') as file:
        lines = file.readlines()
    
    ip_addresses = []
    for line in lines:
        # Remove comments and extract IP addresses
        line = re.sub(r'#.*', '', line).strip()
        ip = re.split(r'[:\s]', line)[0]
        if ip:
            ip_addresses.append(ip)
    
    # Create a DataFrame from the IP addresses
    df = pd.DataFrame(ip_addresses, columns=['IP'])

    # Start loading animation
    animation = LoadingAnimation()
    animation.start()

    # Load GeoIP data
    ipv4_data, ipv6_data = load_geoip_data(geoip_file)

    # Stop loading animation
    animation.stop()

    # Ensure the IP column is treated as string
    df['IP'] = df['IP'].astype(str)

    # Process the DataFrame in chunks
    chunks = [df.iloc[i:i + chunk_size] for i in range(0, df.shape[0], chunk_size)]
    results = []
    all_errors = []

    with ProcessPoolExecutor() as executor:
        futures = [executor.submit(process_chunk, chunk, ipv4_data, ipv6_data, chunk.index.start, position, True) for position, chunk in enumerate(chunks)]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing chunks"):
            chunk_result, chunk_errors = future.result()
            results.append(chunk_result)
            all_errors.extend(chunk_errors)

    # Combine the processed chunks and sort by OriginalIndex to maintain original order
    processed_df = pd.concat(results).sort_values(by='OriginalIndex').drop(columns=['OriginalIndex'])

    # Save the updated DataFrame to the specified file format
    save_output(processed_df, output_file, file_format)

    # Clear screen and print errors if any
    if all_errors:
        print("\033c", end="")  # ANSI escape sequence to clear the screen
        print("Errors encountered during processing:")
        for error in all_errors:
            print(error)

    # Print a line of dashes
    print("-" * 80)
    print(f"Processed data has been written to {output_file}")

if __name__ == "__main__":
    input_type = input("Input type ('excel' or 'text'): ").strip().lower()
    output_format = input("Output file format ('excel' or 'csv'): ").strip().lower()
    
    if input_type == 'excel':
        input_excel = input("Input Excel file path (default: 'input.xlsx'): ") or 'input.xlsx'
        default_output_extension = 'xlsx' if output_format == 'excel' else 'csv'
        output_file = input(f"Output file path (default: 'output.{default_output_extension}'): ") or f'output.{default_output_extension}'
        ip_column_letter = input("Column letter for IP addresses (default: 'L'): ") or 'L'
        geoip_file = input("GeoIP data file path (default: 'country.csv'): ") or 'country.csv'
        chunk_size = input("Number of rows per chunk (default: 500): ") or 500

        chunk_size = int(chunk_size)  # Convert chunk_size to integer

        process_excel(input_excel, output_file, ip_column_letter, geoip_file, chunk_size, output_format)
    
    elif input_type == 'text':
        text_file = input("Input text file path (default: 'input.txt'): ") or 'input.txt'
        default_output_extension = 'xlsx' if output_format == 'excel' else 'csv'
        output_file = input(f"Output file path (default: 'output.{default_output_extension}'): ") or f'output.{default_output_extension}'
        geoip_file = input("GeoIP data file path (default: 'country.csv'): ") or 'country.csv'
        chunk_size = input("Number of rows per chunk (default: 500): ") or 500

        chunk_size = int(chunk_size)  # Convert chunk_size to integer

        process_text_file(text_file, output_file, geoip_file, chunk_size, output_format)
