#!/usr/bin/python3
import csv
import ipaddress
import pandas as pd
import os
import argparse
import gzip
import zipfile
import sys

def ip_range_to_cidr(start_ip, end_ip):
    """Convert an IP range to CIDR format."""
    start_ip = ipaddress.ip_address(start_ip)
    end_ip = ipaddress.ip_address(end_ip)
    return [str(cidr) for cidr in ipaddress.summarize_address_range(start_ip, end_ip)]

def filter_and_convert_to_cidr(input_file, output_path, continent_codes=None, country_codes=None, ip_version=None, output_format='txt', separate_countries=False, separate_ip_versions=False, reverse=False):
    """Filter IP ranges by continent or country and convert to CIDR format."""
    with open(input_file, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        filtered_cidrs = []

        for row in reader:
            row_continent = row['continent'].upper()
            row_country = row['country'].upper()

            matches_continent = continent_codes and row_continent in continent_codes
            matches_country = country_codes and row_country in country_codes

            matches = not (matches_continent or matches_country) if reverse else (matches_continent or matches_country)

            if matches or (not continent_codes and not country_codes):
                start_ip = row['start_ip']
                end_ip = row['end_ip']

                if (ip_version is None) or (ip_version == 4 and ':' not in start_ip) or (ip_version == 6 and ':' in start_ip):
                    cidrs = ip_range_to_cidr(start_ip, end_ip)
                    for cidr in cidrs:
                        filtered_cidrs.append({
                            'CIDR': cidr,
                            'Country': row['country'],
                            'Continent': row['continent'],
                            'IP_Version': 'IPv4' if ':' not in start_ip else 'IPv6'
                        })

    if output_format == 'txt':
        save_to_text(filtered_cidrs, output_path, separate_countries, separate_ip_versions)
    elif output_format == 'excel':
        df = pd.DataFrame(filtered_cidrs)
        df.to_excel(output_path, index=False)

def save_to_text(filtered_cidrs, output_path, separate_countries, separate_ip_versions):
    if separate_countries or separate_ip_versions:
        if not os.path.exists(output_path):
            os.makedirs(output_path)

        country_files = {}
        for entry in filtered_cidrs:
            country = entry['Country']
            ip_version = entry['IP_Version']
            filename = os.path.join(output_path, f"{country}_{ip_version}.txt".lower()) if separate_countries and separate_ip_versions else \
                       os.path.join(output_path, f"{country}.txt".lower()) if separate_countries else \
                       os.path.join(output_path, f"{ip_version}.txt".lower()) if separate_ip_versions else \
                       os.path.join(output_path, "output_cidrs.txt".lower())

            if filename not in country_files:
                country_files[filename] = []

            country_files[filename].append(entry['CIDR'])

        for filename, cidrs in country_files.items():
            with open(filename, 'w') as outfile:
                for cidr in cidrs:
                    outfile.write(f"{cidr}\n")
    else:
        with open(output_path.lower(), 'w') as outfile:
            for entry in filtered_cidrs:
                outfile.write(f"{entry['CIDR']}\n")

def parse_arguments():
    parser = argparse.ArgumentParser(description="Filter IP ranges and convert to CIDR format.")
    parser.add_argument('--input_file', type=str, required=False, help='Path to the input CSV file', default='country.csv')
    parser.add_argument('--output_path', type=str, required=False, help='Path to the output file or directory')
    parser.add_argument('--continent_codes', type=str, required=False, help='Comma-separated list of continent codes')
    parser.add_argument('--country_codes', type=str, required=False, help='Comma-separated list of country codes')
    parser.add_argument('--ip_version', type=int, choices=[4, 6], required=False, help='IP version (4 or 6)')
    parser.add_argument('--output_format', type=str, choices=['txt', 'excel'], required=False, help='Output format (txt or excel)', default='txt')
    parser.add_argument('--separate_countries', action='store_true', help='Separate output by countries')
    parser.add_argument('--separate_ip_versions', action='store_true', help='Separate output by IP versions')
    parser.add_argument('--reverse', action='store_true', help='Reverse the filtering logic to exclude specified continents or countries')
    parser.add_argument('--ignore_first_row', action='store_true', help='Ignore the first row of the CSV file')
    parser.add_argument('--start_ip_col', type=int, default=0, help='Column index for start IP')
    parser.add_argument('--end_ip_col', type=int, default=1, help='Column index for end IP')
    parser.add_argument('--country_code_col', type=int, default=2, help='Column index for country code')
    parser.add_argument('csv_files', nargs='*', help='CSV files or directories')
    
    return parser.parse_args()

def detect_format_and_collect(csv_file, ignore_first_row, start_ip_col, end_ip_col, country_code_col, existing_country_data):
    country = existing_country_data
    
    first_row = next(csv_file)

    if first_row == ['start_ip', 'end_ip', 'country', 'country_name', 'continent', 'continent_name']:
        sys.stdout.write("ipinfo.io country.csv format matched!\n")
        ignore_first_row = True
        start_ip_col = 0
        end_ip_col = 1
        country_code_col = 2
    elif len(first_row) == 3 and all('.' in field for field in first_row[:2]):
        sys.stdout.write("dbip-country-lite format matched!\n")
        start_ip_col = 0
        end_ip_col = 1
        country_code_col = 2
    elif first_row == ['ip_version', 'start_ip', 'end_ip', 'continent', 'country_code', 'country', 'state', 'city', 'zip', 'timezone', 'latitude', 'longitude', 'accuracy']:
        sys.stdout.write("ipapi.is csv format matched!\n")
        ignore_first_row = True
        start_ip_col = 1
        end_ip_col = 2
        country_code_col = 4

    if not ignore_first_row:
        csv_file = iter([first_row] + list(csv_file))

    return collect(csv_file, start_ip_col, end_ip_col, country_code_col, country)

def collect(csv_file, start_ip_col, end_ip_col, country_code_col, existing_country_data):
    country = existing_country_data
    line_num = 1

    for row in csv_file:
        line_num += 1

        if len(row) <= max(start_ip_col, end_ip_col, country_code_col):
            sys.stderr.write(f"\nError: Skipping row {line_num}: insufficient columns\n")
            continue

        start_ip, end_ip, country_code = row[start_ip_col], row[end_ip_col], row[country_code_col]
        if country_code not in country:
            country[country_code] = {
                'name': country_code,
                'pool_v4': [],
                'pool_v6': []
            }
        c = country[country_code]
        if ':' in start_ip:
            c['pool_v6'].append((ipaddress.IPv6Address(start_ip).packed, ipaddress.IPv6Address(end_ip).packed))
        else:
            c['pool_v4'].append((int(ipaddress.IPv4Address(start_ip)), int(ipaddress.IPv4Address(end_ip))))

        if line_num % 4096 == 0:
            sys.stderr.write(f"\r\033[2K{line_num} entries")
    sys.stderr.write(f"\r\033[2K{line_num} entries total\n")
    return country

def read_csv_file(file_path, ignore_first_row, start_ip_col, end_ip_col, country_code_col, country_data):
    if file_path.endswith('.csv.gz'):
        with gzip.open(file_path, mode='rt', newline='', encoding='utf-8') as gzfile:
            csvreader = csv.reader(gzfile, delimiter=',', quotechar='"')
            country_data = detect_format_and_collect(csvreader, ignore_first_row, start_ip_col, end_ip_col, country_code_col, country_data)
    elif file_path.endswith('.csv.zip'):
        with zipfile.ZipFile(file_path, 'r') as zipf:
            for file_name in zipf.namelist():
                if file_name.endswith('.csv'):
                    with zipf.open(file_name) as csvfile:
                        csvreader = csv.reader(csvfile.read().decode('utf-8').splitlines(), delimiter=',', quotechar='"')
                        country_data = detect_format_and_collect(csvreader, ignore_first_row, start_ip_col, end_ip_col, country_code_col, country_data)
    else:
        with open(file_path, newline='', encoding='utf-8') as csvfile:
            csvreader = csv.reader(csvfile, delimiter=',', quotechar='"')
            country_data = detect_format_and_collect(csvreader, ignore_first_row, start_ip_col, end_ip_col, country_code_col, country_data)
    return country_data

def interactive_input():
    input_file = input("Enter the path to the input CSV file: ")
    continent_code_input = input("Enter the continent codes (e.g., AS, EU) or leave blank to specify countries or all: ")
    continent_codes = [code.strip().upper() for code in continent_code_input.split(',')] if continent_code_input else None
    
    country_codes = None
    if not continent_codes:
        country_code_input = input("Enter the country codes (e.g., CN, US) or leave blank for all: ")
        country_codes = [code.strip().upper() for code in country_code_input.split(',')] if country_code_input else None
    
    ip_version_input = input("Enter IP version (4 for IPv4, 6 for IPv6, leave blank for both): ").strip()
    ip_version = int(ip_version_input) if ip_version_input in ['4', '6'] else None
    
    output_format = input("Enter output format (txt or excel, default: txt): ").lower() or 'txt'

    separate_countries = False
    separate_ip_versions = False
    if output_format == 'txt':
        separate_countries = input("Do you want to separate the output by countries (yes/no, default: no)? ").lower() in ['yes', 'y']
        if ip_version is None:
            separate_ip_versions = input("Do you want to separate the output by IP versions (yes/no, default: no)? ").lower() in ['yes', 'y']

    reverse = input("Do you want to reverse the filtering logic to exclude specified continents or countries (yes/no, default: no)? ").lower() in ['yes', 'y']

    output_path = input("Enter the output directory path (leave blank for current directory): ") if separate_countries or separate_ip_versions else \
                  input("Enter the output file name (e.g., output_cidrs.txt or output_cidrs.xlsx): ")
    if not output_path:
        if output_format == 'txt':
            output_path = 'output_cidrs.txt'
        elif output_format == 'excel':
            output_path = 'output_cidrs.xlsx'

    return input_file, output_path or '.', continent_codes, country_codes, ip_version, output_format, separate_countries, separate_ip_versions, reverse

def main():
    args = parse_arguments()
    
    country_data = {}
    for csv_file in args.csv_files:
        country_data = read_csv_file(csv_file, args.ignore_first_row, args.start_ip_col, args.end_ip_col, args.country_code_col, country_data)
    
    if not args.output_path:
        input_file, output_path, continent_codes, country_codes, ip_version, output_format, separate_countries, separate_ip_versions, reverse = interactive_input()
    else:
        input_file = args.input_file
        continent_codes = [code.strip().upper() for code in args.continent_codes.split(',')] if args.continent_codes else None
        country_codes = [code.strip().upper() for code in args.country_codes.split(',')] if args.country_codes else None
        ip_version = args.ip_version
        output_format = args.output_format
        separate_countries = args.separate_countries
        separate_ip_versions = args.separate_ip_versions
        reverse = args.reverse
        output_path = args.output_path
        if (separate_countries or separate_ip_versions) and not os.path.exists(output_path):
            os.makedirs(output_path)

    filter_and_convert_to_cidr(input_file, output_path, continent_codes, country_codes, ip_version, output_format, separate_countries, separate_ip_versions, reverse)
    print(f"Filtered CIDR ranges have been written to {output_path}")

if __name__ == "__main__":
    main()
