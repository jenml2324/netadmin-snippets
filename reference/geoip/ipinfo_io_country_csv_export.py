#!/usr/bin/python3
import csv
import ipaddress
import pandas as pd
import os

def ip_range_to_cidr(start_ip, end_ip):
    """Convert an IP range to CIDR format."""
    start_ip = ipaddress.ip_address(start_ip)
    end_ip = ipaddress.ip_address(end_ip)
    return [str(cidr) for cidr in ipaddress.summarize_address_range(start_ip, end_ip)]

def filter_and_convert_to_cidr(input_file, output_path, continent_codes=None, country_codes=None, ip_version=None, output_format='txt', separate_countries=False, separate_ip_versions=False):
    """Filter IP ranges by continent or country and convert to CIDR format."""
    with open(input_file, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        filtered_cidrs = []

        for row in reader:
            # Check if row matches any continent or country code, or if no filters are applied
            if ((continent_codes and row['continent'] in continent_codes) or
                (country_codes and row['country'] in country_codes) or
                (not continent_codes and not country_codes)):
                
                start_ip = row['start_ip']
                end_ip = row['end_ip']

                # Filter by IP version
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
        if separate_countries or separate_ip_versions:
            if not os.path.exists(output_path):
                os.makedirs(output_path)

            country_files = {}
            for entry in filtered_cidrs:
                country = entry['Country']
                ip_version = entry['IP_Version']
                if separate_countries and separate_ip_versions:
                    filename = os.path.join(output_path, f"{country}_{ip_version}.txt".lower())
                elif separate_countries:
                    filename = os.path.join(output_path, f"{country}.txt".lower())
                elif separate_ip_versions:
                    filename = os.path.join(output_path, f"{ip_version}.txt".lower())
                else:
                    filename = os.path.join(output_path, "output_cidrs.txt".lower())
                
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
    elif output_format == 'excel':
        df = pd.DataFrame(filtered_cidrs)
        df.to_excel(output_path, index=False)

if __name__ == "__main__":
    input_file = 'country.csv'

    continent_code_input = input("Enter the continent codes (e.g., AS, EU) or leave blank to specify countries or all: ")
    continent_codes = [code.strip() for code in continent_code_input.split(',')] if continent_code_input else None
    
    country_codes = None
    if not continent_codes:
        country_code_input = input("Enter the country codes (e.g., CN, US) or leave blank for all: ")
        country_codes = [code.strip() for code in country_code_input.split(',')] if country_code_input else None
    
    ip_version_input = input("Enter IP version (4 for IPv4, 6 for IPv6, leave blank for both): ").strip()
    ip_version = int(ip_version_input) if ip_version_input in ['4', '6'] else None
    
    output_format = input("Enter output format (txt or excel): ").lower()

    separate_countries = False
    separate_ip_versions = False
    if output_format == 'txt':
        separate_countries = input("Do you want to separate the output by countries (yes/no, default: no)? ").lower() in ['yes', 'y']
        if ip_version is None:
            separate_ip_versions = input("Do you want to separate the output by IP versions (yes/no, default: no)? ").lower() in ['yes', 'y']

    if separate_countries or separate_ip_versions:
        output_path = input("Enter the output directory path (leave blank for current directory): ")
        if not output_path:
            output_path = '.'
        if not os.path.exists(output_path):
            os.makedirs(output_path)
    else:
        output_file = input("Enter the output file name (e.g., output_cidrs.txt or output_cidrs.xlsx): ")
        if not output_file:
            output_file = 'output_cidrs.xlsx'
        else:
            if not (output_file.endswith('.txt') or output_file.endswith('.xlsx')):
                print("Please enter a file name with the correct extension (.txt or .xlsx)")
                exit()
        output_path = output_file

    filter_and_convert_to_cidr(input_file, output_path, continent_codes, country_codes, ip_version, output_format, separate_countries, separate_ip_versions)
    print(f"Filtered CIDR ranges have been written to {output_path}")
