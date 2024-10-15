import pdb
import os
import re
import posixpath
import sys
import argparse
import urllib.request
import zipfile
import httpx
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon
from dotenv import load_dotenv
import rasterio
import numpy as np
import json
import shutil
from datetime import datetime
import calendar
import boto3

# Load environment variables from a .env file (make sure to create one with your credentials)
load_dotenv()

# Constants
HOST = 'https://api.gfm.eodc.eu/v2'
CRS = 'EPSG:4326'
DOWNLOAD_DIR = os.path.join(os.path.expanduser('~'), 'Downloads', 'gfm_downloads')

def parse_arguments():
    parser = argparse.ArgumentParser(description='GFM data pulling script designed to find likely floods.')
    parser.add_argument('--date', required=True, help='Date in YYYY-MM format to query the entire month')
    parser.add_argument('--threshold', required=True, type=float, help='Threshold value for flood extent percentage')
    parser.add_argument('--aoi_file', required=True, help='Path to the AOI coordinates file (JSON format)')
    parser.add_argument('--key_root', required=True, help='Root key for S3 upload')
    parser.add_argument('--bucket_name', required=True, help='Name of the S3 bucket to upload files to')
    args = parser.parse_args()

    # Parse date argument
    try:
        date_input = args.date
        date_obj = datetime.strptime(date_input, "%Y-%m")
        year = date_obj.year
        month = date_obj.month
        first_day = 1
        last_day = calendar.monthrange(year, month)[1]  # Returns tuple (weekday of first day, number of days in month)
        date_from = f"{year}-{month:02d}-{first_day:02d}T00:00:00"
        date_to = f"{year}-{month:02d}-{last_day:02d}T23:59:59"
    except ValueError as e:
        print(f"Error parsing date: {e}")
        sys.exit(1)

    # Read AOI coordinates from the provided file
    try:
        with open(args.aoi_file, 'r') as f:
            aoi_data = json.load(f)
            if 'coordinates' in aoi_data and 'type' in aoi_data:
                if aoi_data['type'] != 'Polygon':
                    raise ValueError("AOI geometry must be of type 'Polygon'.")
                aoi_coordinates = aoi_data['coordinates']
            else:
                raise ValueError("AOI file must contain 'type' and 'coordinates' keys.")
    except Exception as e:
        print(f"Error reading AOI file: {e}")
        sys.exit(1)

    return date_from, date_to, args.threshold, aoi_coordinates, args.key_root, args.bucket_name

def authenticate():
    email = os.getenv('GFM_EMAIL')
    password = os.getenv('GFM_PASSWORD')

    if not email or not password:
        raise ValueError("Please set your GFM_EMAIL and GFM_PASSWORD in the project's .env file.")

    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }
    params = {
        'email': email,
        'password': password
    }
    auth_response = httpx.post(f'{HOST}/auth/login', headers=headers, json=params)
    auth_response.raise_for_status()

    user_id = auth_response.json()['client_id']
    access_token = f"Bearer {auth_response.json()['access_token']}"
    return user_id, access_token

def create_headers(access_token):
    headers = {
        'accept': 'application/json',
        'Authorization': access_token,
        'Content-Type': 'application/json'
    }
    return headers

def create_aoi(user_id, headers, aoi_coordinates):
    aoi_params = {
        "aoi_name": "Example AOI",
        "description": "An example region of interest",
        "user_id": user_id,
        "geoJSON": {
            "type": "Polygon",
            "coordinates": aoi_coordinates
        },
        "region": "AUT/Tirol",
        "skip_aoi_check": "false"
    }
    aoi_create_response = httpx.post(f'{HOST}/aoi/create', headers=headers, json=aoi_params, timeout=600)
    aoi_create_response.raise_for_status()
    aoi_id = aoi_create_response.json()['aoi_id']
    return aoi_id

def get_product_list(headers, aoi_id, date_from, date_to):
    params = {
        'from': date_from,
        'to': date_to,
        'time': 'range',  # options: range, latest, all
    }
    products_response = httpx.get(f'{HOST}/aoi/{aoi_id}/products', headers=headers, params=params, timeout=1000)
    products_response.raise_for_status()
    df = pd.json_normalize(
        products_response.json(),
        record_path=['products'],
        meta=['aoi_id']
    )
    return df

def download_product_rasters(headers, aoi_id, product_ids, threshold, key_root, date_from, bucket_name):
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    for product_id in product_ids:
        try:
            # Download the product
            download_response = httpx.get(
                f'{HOST}/download/scene-product/{product_id}/{aoi_id}', headers=headers, timeout=60
            )
            download_response.raise_for_status()
            download_link = download_response.json()['download_link']

            # Define download and extraction paths
            download_path = os.path.join(DOWNLOAD_DIR, f'{product_id}.zip')
            extract_path = os.path.join(DOWNLOAD_DIR, f'{product_id}')

            # Download the file
            urllib.request.urlretrieve(download_link, download_path)

            # Extract the zip file
            with zipfile.ZipFile(download_path, 'r') as zip_ref:
                zip_ref.extractall(extract_path)

            # Process subtiles
            flood_detected, flood_fractions = process_subtiles(extract_path, threshold)

            if flood_detected:
                upload_to_s3(key_root, extract_path, threshold, date_from, bucket_name, flood_fractions)
            else:
                # Remove the extracted files if below threshold
                shutil.rmtree(extract_path)
                os.remove(download_path)

        except Exception as e:
            print(f"Failed to process product {product_id}: {e}")
            continue

def process_subtiles(extract_path, threshold):
    # Patterns to match filenames
    observed_pattern = re.compile(
        r'^(?P<continent>\w+)_(?P<tile_id>\w+)_ENSEMBLE_FLOOD_(?P<date>\d{8}T\d{6})_.*\.tif$'
    )
    reference_pattern = re.compile(
        r'^(?P<continent>\w+)_(?P<tile_id>\w+)_REFERENCE_WATER_OUT_.*\.tif$'
    )

    # Dictionaries to hold file paths for each tile_id
    observed_files = {}
    reference_files = {}

    # List all files in extract_path
    for filename in os.listdir(extract_path):
        filepath = os.path.join(extract_path, filename)
        if os.path.isfile(filepath):
            observed_match = observed_pattern.match(filename)
            if observed_match:
                tile_id = observed_match.group('tile_id')
                observed_files.setdefault(tile_id, []).append(filepath)
            else:
                reference_match = reference_pattern.match(filename)
                if reference_match:
                    tile_id = reference_match.group('tile_id')
                    reference_files[tile_id] = filepath  # Assume one reference file per tile_id

    flood_fractions = {}
    flood_detected = False

    # For each tile_id, process the corresponding files
    for tile_id in observed_files:
        ref_file = reference_files.get(tile_id)
        if ref_file:
            for obs_file in observed_files[tile_id]:
                try:
                    # Calculate flood extent percentage
                    flood_fraction = calculate_flood_fraction(obs_file, ref_file)
                    obs_basename = os.path.basename(obs_file)
                    flood_fractions[obs_basename] = flood_fraction

                    if flood_fraction > threshold:
                        flood_detected = True

                    print(f"Tile {tile_id}: Flood extent percentage {flood_fraction:.2%}.")

                except Exception as e:
                    print(f"Error processing tile {tile_id}: {e}")
        else:
            print(f"Tile {tile_id}: Missing reference water mask file.")

    return flood_detected, flood_fractions

def calculate_flood_fraction(observed_flood_path, reference_water_path):
    # Open observed flood extent raster
    with rasterio.open(observed_flood_path) as src_obs:
        obs_data = src_obs.read(1)
        # Sum of pixels equal to 1
        obs_flood_pixels = np.sum(obs_data == 1)

    # Open reference water mask raster
    with rasterio.open(reference_water_path) as src_ref:
        ref_data = src_ref.read(1)
        # Sum of pixels equal to 1 or 2
        ref_water_pixels = np.sum((ref_data == 1) | (ref_data == 2))

    if ref_water_pixels == 0:
        # Avoid division by zero
        return 0.0

    # Calculate ratio
    ratio = obs_flood_pixels / ref_water_pixels

    return ratio

def upload_to_s3(key_root, extract_path, threshold, date_from, bucket_name, flood_fractions):
    s3_client = boto3.client('s3')

    # Extract the last subdirectory in extract_path
    last_subdirectory = os.path.basename(os.path.normpath(extract_path))

    # Strip leading/trailing slashes from key_root
    key_root = key_root.strip('/')

    # Generate date_input from date_from
    date_input = date_from[:7]  # 'YYYY-MM'

    # Create the final key and append date_input
    final_key = posixpath.join(key_root, f"fraction_threshold_{threshold}", date_input, last_subdirectory)

    # Save flood fractions to a JSON file in the extract_path
    flood_fractions_file = os.path.join(extract_path, 'flood_fractions.json')
    with open(flood_fractions_file, 'w') as f:
        json.dump(flood_fractions, f, indent=4)

    # Upload all files in the directory to S3
    for root, dirs, files in os.walk(extract_path):
        for filename in files:
            file_path = os.path.join(root, filename)
            # Compute the relative path and replace backslashes with forward slashes
            relative_path = os.path.relpath(file_path, extract_path).replace('\\', '/')
            s3_key = posixpath.join(final_key, relative_path)
            try:
                s3_client.upload_file(file_path, bucket_name, s3_key)
            except Exception as e:
                print(f"Failed to upload {file_path} to S3: {e}")
    print("Upload complete.")

def main():
    start_time = datetime.now()
    print(f"Script started at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    date_from, date_to, threshold, aoi_coordinates, key_root, bucket_name = parse_arguments()

    try:
        user_id, access_token = authenticate()
    except Exception as e:
        print(f"Authentication failed: {e}")
        sys.exit(1)

    headers = create_headers(access_token)

    try:
        aoi_id = create_aoi(user_id, headers, aoi_coordinates)
    except Exception as e:
        print(f"AOI creation failed: {e}")
        sys.exit(1)

    try:
        products_df = get_product_list(headers, aoi_id, date_from, date_to)
    except Exception as e:
        print(f"Failed to retrieve product list: {e}")
        sys.exit(1)

    if products_df.empty:
        print("No products found for the given AOI and date range.")
        sys.exit(0)

    product_ids = products_df['cell_code'].tolist()

    download_product_rasters(headers, aoi_id, product_ids, threshold, key_root, date_from, bucket_name)

    print("Processing complete.")

    end_time = datetime.now()
    print(f"Script ended at {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    total_time = end_time - start_time
    print(f"Total processing time: {total_time}")

if __name__ == "__main__":
    main()
