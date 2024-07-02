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
    
    return parser.parse_args()

def main():
    args = parse_arguments()

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
