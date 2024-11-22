import pdb
import pickle
import argparse
import multiprocessing
from multiprocessing import Pool, cpu_count 
from functools import partial
import os
import sys
import json
import csv
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from time import sleep
from random import uniform
from typing import Dict, Optional
import pandas as pd
import geopandas as gpd
import rasterio
import boto3
import httpx
import re  
import urllib.request  
import requests
from shapely.geometry import shape, Polygon  
from typing import List  
import tempfile
import zipfile
import shutil
from pathlib import Path
import posixpath
from google.cloud import storage
import xarray as xr
from dotenv import load_dotenv

#setup nwm features so can be used across multiple processes
shared_features = None

@dataclass
class Config:
    flood_threshold: float
    s3_bucket: str
    key_root: str
    credentials: Dict[str, str]
    paths: Dict[str, str]

    @classmethod
    def from_env(cls):
        # Load .env file 
        load_dotenv()  

        """Create Config from environment variables."""
        required_vars = {
            'FLOOD_THRESHOLD': float,
            'S3_BUCKET': str,
            'KEY_ROOT': str,
            'GFM_EMAIL': str,
            'GFM_PASSWORD': str,
            'NWM_MAIN_HYDROFABRIC_PATH': str,
            'NWM_AK_HYDROFABRIC_PATH': str
        }
    
        # Check for missing variables
        missing = [var for var in required_vars if not os.getenv(var)]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
        # Validate types
        env_vars = {}
        for var, var_type in required_vars.items():
            try:
                env_vars[var.lower()] = var_type(os.getenv(var))
            except ValueError:
                raise ValueError(f"Invalid value for {var}: must be {var_type.__name__}")

        return cls(
            flood_threshold=env_vars['flood_threshold'],
            s3_bucket=env_vars['s3_bucket'],
            key_root=env_vars['key_root'].strip('/'),
            credentials={
                'gfm_email': env_vars['gfm_email'],
                'gfm_password': env_vars['gfm_password']
            },
            paths={
                'main_hydrofabric': env_vars['nwm_main_hydrofabric_path'],
                'ak_hydrofabric': env_vars['nwm_ak_hydrofabric_path'],
                'temp': os.getenv('TEMP_PATH', 'temp')
            }
        )

class S3Uploader:
    """Handles all S3 upload operations."""
    def __init__(self, config: Config):
        # Verify AWS credentials are available
        if not os.getenv('AWS_ACCESS_KEY_ID') or not os.getenv('AWS_SECRET_ACCESS_KEY'):
            print("ERROR: AWS credentials not found in environment variables.")
            print("Please ensure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set.")
            sys.exit(1)

        self.bucket = config.s3_bucket
        self.key_root = config.key_root
        self.threshold = config.flood_threshold
        
        try:
            self.s3_client = boto3.client('s3')
            # Test connection by trying to access the bucket
            self.s3_client.head_bucket(Bucket=self.bucket)
        except Exception as e:
            print(f"ERROR: Failed to initialize S3 connection: {str(e)}")
            print("Please check your AWS credentials and bucket configuration.")
            sys.exit(1)

    def upload_product_files(self, product_path: str, flood_ratios: dict, flowfile_info: Optional[tuple[pd.DataFrame, str]] = None):
        """
        Upload all product files, flood ratios, and optional flowfile to S3.
        
        Args:
            product_path: Path to extracted product directory
            flood_ratios: Dictionary of flood to baseline ratios by tile
            flowfile_info: Optional tuple of (DataFrame containing flow data, NWM version)
        """
        if not os.path.exists(product_path):
            raise ValueError(f"Product path does not exist: {product_path}")
        
        if not isinstance(flood_ratios, dict):
            raise ValueError("flood_ratios must be a dictionary")

        # Get product ID and date from path
        product_id = os.path.basename(product_path)
        date_input = self._extract_date_from_product_id(product_id)
        
        # Construct the base key for this product
        base_key = self.construct_key(product_id, date_input)
        
        try:
            # Save and upload flood ratios
            flood_ratios_path = os.path.join(product_path, 'flood_ratios.json')
            with open(flood_ratios_path, 'w') as f:
                json.dump(flood_ratios, f, indent=4)
            
            # Upload all files in the product directory
            for root, _, files in os.walk(product_path):
                for filename in files:
                    file_path = os.path.join(root, filename)
                    # Compute relative path from product directory
                    relative_path = os.path.relpath(file_path, product_path)
                    s3_key = posixpath.join(base_key, relative_path)
                    
                    self.s3_client.upload_file(file_path, self.bucket, s3_key)
            
            # Upload flowfile if provided
            if flowfile_info is not None:
                flowfile, nwm_version = flowfile_info
                if not isinstance(flowfile, pd.DataFrame):
                    raise ValueError("flowfile must be a pandas DataFrame")
                    
                filename = f"NWM_{nwm_version}_flowfile.csv"
                flowfile_path = os.path.join(product_path, filename)
                flowfile.to_csv(flowfile_path, index=False)
                s3_key = posixpath.join(base_key, filename)
                self.s3_client.upload_file(flowfile_path, self.bucket, s3_key)

        except Exception as e:
            logging.error(f"Failed to upload files for {product_id}: {str(e)}")
            raise

    def construct_key(self, product_id: str, date_input: str) -> str:
        """Construct S3 key for product."""
        return posixpath.join(
            self.key_root,
            date_input,
            product_id
        )

    @staticmethod
    def _extract_date_from_product_id(product_id: str) -> str:
        """Extract YYYY-MM date from product ID."""
        date_match = re.search(r"(\d{8})T", product_id)
        if date_match:
            date_str = date_match.group(1)
            return f"{date_str[:4]}-{date_str[4:6]}"
        raise ValueError(f"Could not extract date from product ID: {product_id}")

class GFMClient:
    """Handles all GFM API interactions and data download."""
    def __init__(self, config: Config):
        self.config = config
        self.headers = None
        self.user_id = None
        self.host = 'https://api.gfm.eodc.eu/v2'
        self._authenticate()

    def _authenticate(self):
        """Authenticate with GFM API."""
        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json'
        }
        params = {
            'email': self.config.credentials['gfm_email'],
            'password': self.config.credentials['gfm_password']
        }
        
        auth_response = httpx.post(f'{self.host}/auth/login', headers=headers, json=params)
        auth_response.raise_for_status()
        
        self.user_id = auth_response.json()['client_id']
        self.headers = {
            'accept': 'application/json',
            'Authorization': f"Bearer {auth_response.json()['access_token']}",
            'Content-Type': 'application/json'
        }

    def get_products(self, date_range: tuple, aoi_id: str) -> list:
        """
        Get list of products for date range and AOI.
        
        Args:
            date_range: Tuple of (start_date, end_date)
            aoi_coordinates: List of coordinates forming polygon
        """
    
        # Get products for AOI
        params = {
            'from': date_range[0],
            'to': date_range[1],
            'time': 'range'
        }
        
        products_response = httpx.get(
            f'{self.host}/aoi/{aoi_id}/products',
            headers=self.headers,
            params=params,
            timeout=2000
        )
        products_response.raise_for_status()
        
        return products_response.json().get('products', [])

    def _create_aoi(self, coordinates: list) -> str:
        """
        Create AOI.
    
        Args:
            coordinates: List of coordinates forming polygon
        
        Returns:
            str: AOI ID
        
        Raises:
            Exception: If AOI creation fails after all retries
        """
        if not coordinates:
            raise ValueError("Empty coordinates provided")
    
        coords_to_use = [coordinates]
        max_retries = 2
        last_exception = None
    
        aoi_params = {
            "aoi_name": "Example AOI",
            "description": "An example region of interest",
            "user_id": self.user_id,
            "geoJSON": {
                "type": "Polygon",
                "coordinates": coords_to_use
            },
            "skip_aoi_check": "false"
        }

        for attempt in range(max_retries + 1):  # +1 for initial attempt
            try:
                aoi_response = httpx.post(
                    f'{self.host}/aoi/create',
                    headers=self.headers,
                    json=aoi_params,
                    timeout=600
                )
                aoi_response.raise_for_status()
                return aoi_response.json()['aoi_id']
            
            except Exception as e:
                last_exception = e
                if attempt < max_retries:
                    # Calculate delay with exponential backoff
                    delay = (2 ** attempt) * uniform(1, 2)  # Random delay between 1-2, 2-4, 4-8 seconds
                    sleep(delay)
                    logging.warning(f"AOI creation attempt {attempt + 1} failed. Retrying after {delay:.1f} seconds. Error: {str(e)}")
                    continue
    
        # If we get here, all retries failed
        logging.error(f"AOI creation failed after {max_retries + 1} attempts")
        raise last_exception

    def download_product(self, product_id: str, aoi_id: str) -> 'Product':
        """
        Download and extract a product with enhanced error handling and timeout handling.
    
        Args:
            product_id: ID of the product to download
            aoi_id: ID of the AOI
        
        Returns:
            Product object
        
        Raises:
            ValueError: If download link is invalid or download fails
            RuntimeError: If extraction fails
        """
        download_path = None
        extract_path = None
        max_retries = 5
    
        # Longer timeouts for large files
        CONNECT_TIMEOUT = 160    # seconds to establish connection
        READ_TIMEOUT = 1800    # 30 minutes to download file

        try:
            # Get download link with retry
            download_link = None
            for attempt in range(max_retries):
                try:
                    download_response = httpx.get(
                        f'{self.host}/download/scene-product/{product_id}/{aoi_id}',
                        headers=self.headers,
                        timeout=1200  
                    )
                    download_response.raise_for_status()
                    download_link = download_response.json().get('download_link')
                
                    if not download_link:
                        raise ValueError("Received empty download link")
                    
                    break
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise ValueError(f"Failed to get download link after {max_retries} attempts: {str(e)}")
                    sleep(uniform(5, 15) * (attempt + 1))
                    continue
    
            # Create temp directory for this product
            extract_path = os.path.join(self.config.paths['temp'], product_id)
            os.makedirs(extract_path, exist_ok=True)
    
            # Download with retries
            download_path = os.path.join(self.config.paths['temp'], f'{product_id}.zip')
    
            for attempt in range(max_retries):
                try:
                    print(f"Process {os.getpid()} downloading {product_id} to {download_path} (Attempt {attempt + 1}/{max_retries})")
                
                    # Create a session with custom timeout settings
                    session = requests.Session()
                    session.mount('https://', requests.adapters.HTTPAdapter(
                        max_retries=3,
                        pool_connections=10,
                        pool_maxsize=10
                    ))
                
                    # Stream the response with increased timeouts
                    response = session.get(
                        download_link, 
                        stream=True,
                        timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
                    )
                    response.raise_for_status()
                
                    # Get total file size if available
                    total_size = int(response.headers.get('content-length', 0))
                
                    # Open file in binary write mode
                    with open(download_path, 'wb') as f:
                        if total_size == 0:  # No content length header
                            f.write(response.content)
                        else:
                            for chunk in response.iter_content(chunk_size=8192):
                                if chunk:
                                    f.write(chunk)
                
                    # Verify the download
                    if not os.path.exists(download_path):
                        raise ValueError("Download file not created")
                    
                    file_size = os.path.getsize(download_path)
                    if file_size == 0:
                        raise ValueError("Downloaded file is empty")
                    
                    if total_size > 0 and file_size != total_size:
                        raise ValueError(f"Incomplete download: got {file_size} bytes, expected {total_size}")
                
                    print(f"Download completed for {product_id}. File size: {file_size}")
                    break
                
                except requests.Timeout as timeout_error:
                    print(f"Timeout error for {product_id} (Attempt {attempt + 1}): {str(timeout_error)}")
                    if os.path.exists(download_path):
                        os.remove(download_path)
                    if attempt < max_retries - 1:
                        # Increase timeout for next attempt
                        READ_TIMEOUT *= 1.5
                        sleep(uniform(10, 30) * (attempt + 1))
                    else:
                        raise ValueError(f"Failed to download after {max_retries} attempts due to timeouts")
            
                except Exception as download_error:
                    print(f"Download error for {product_id} (Attempt {attempt + 1}): {str(download_error)}")
                    if os.path.exists(download_path):
                        os.remove(download_path)
                    if attempt < max_retries - 1:
                        sleep(uniform(10, 30) * (attempt + 1))
                    else:
                        raise ValueError(f"Failed to download after {max_retries} attempts: {str(download_error)}")

            # Verify zip file integrity before extraction
            try:
                with zipfile.ZipFile(download_path, 'r') as zip_ref:
                    # Test zip file integrity
                    if zip_ref.testzip() is not None:
                        raise zipfile.BadZipFile("Zip file is corrupted")
                    # Extract the zip file
                    zip_ref.extractall(extract_path)
            except zipfile.BadZipFile as e:
                raise RuntimeError(f"Invalid or corrupted zip file: {str(e)}")

            # Clean up zip file
            os.remove(download_path)
        
            # Verify extraction succeeded
            if not os.path.exists(extract_path) or not os.listdir(extract_path):
                raise RuntimeError("Extraction produced no files")

            return Product(
                id=product_id,
                date=self._parse_date_from_product_id(product_id),
                extract_path=extract_path
            )

        except Exception as e:
            # Clean up any partial downloads/extractions
            if download_path and os.path.exists(download_path):
                os.remove(download_path)
            if extract_path and os.path.exists(extract_path):
                shutil.rmtree(extract_path)
            raise RuntimeError(f"Failed to process product {product_id}: {str(e)}") from e

    @staticmethod
    def _parse_date_from_product_id(product_id: str) -> datetime:
        """Parse date from product ID."""
        date_str = re.search(r"\d{8}T\d{6}", product_id).group()
        return datetime.strptime(date_str, "%Y%m%dT%H%M%S")

class Product:
    """Product class that handles data processing."""
    def __init__(self, id: str, date: datetime, extract_path: str):
        self.id = id
        self.date = date
        self.extract_path = extract_path

    def calculate_flood_ratios(self) -> dict:
        """Calculate flood ratios for all tiles in the product."""
        ratios = {}

        # Check if any flood files exist
        flood_files = [f for root, _, files in os.walk(self.extract_path) 
                      for f in files if 'ENSEMBLE_FLOOD' in f and 'tif' in f]
        if not flood_files:
            raise ValueError(f"No flood raster files found in {self.extract_path}")

        # Process each flood file
        for root, _, files in os.walk(self.extract_path):
            for file in files:
                if 'ENSEMBLE_FLOOD' in file and 'tif' in file:
                    tile_id = self._extract_tile_id(file)
                    flood_path = os.path.join(root, file)
                    
                    # First try to find reference water file
                    ref_path = self._find_reference_file(root, tile_id)
                    
                    if ref_path:
                        # Use traditional reference water method
                        ratio = self._calculate_tile_ratio(flood_path, ref_path)
                    else:
                        # Try to derive reference water from OBSWATER
                        obswater_path = self._find_obswater_file(root, tile_id)
                        if not obswater_path:
                            # Get all files in the extract path for debugging
                            all_files = []
                            for r, _, fs in os.walk(self.extract_path):
                                all_files.extend(fs)
                            
                            print(f"\nDebug information for tile {tile_id}:")
                            print("\nFiles with 'flood' in them:")
                            for f in [f for f in all_files if 'flood' in f.lower()]:
                                print(f"  {f}")
                            print("\nFiles with 'reference' in them:")
                            for f in [f for f in all_files if 'reference' in f.lower()]:
                                print(f"  {f}")
                            print("\nFiles with 'obswater' in them:")
                            for f in [f for f in all_files if 'obswater' in f.lower()]:
                                print(f"  {f}")
                            print("\n")
                            
                            raise ValueError(f"Neither reference water nor obswater raster found for tile {tile_id}")
                        
                        ratio = self._calculate_tile_ratio_from_obswater(flood_path, obswater_path)
                    
                    ratios[tile_id] = ratio

        return ratios

    def _find_obswater_file(self, root: str, tile_id: str) -> Optional[str]:
        """Find OBSWATER file for given tile ID."""
        patterns = [
            f"*{tile_id}_ENSEMBLE_OBSWATER_*.tif",  
            f"ENSEMBLE_OBSWATER_*{tile_id}*.tif",   
        ]
    
        for pattern in patterns:
            matches = list(Path(root).glob(pattern))
            if matches:
                return str(matches[0])
    
        return None

    def _calculate_tile_ratio_from_obswater(self, flood_path: str, obswater_path: str) -> float:
        """
        Calculate flood ratio using OBSWATER raster when reference water is not available.
        
        The reference water is derived by masking out flood pixels from the OBSWATER raster.
        """
        with rasterio.open(flood_path) as flood_ds, rasterio.open(obswater_path) as obs_ds:
            flood_data = flood_ds.read(1)
            obs_data = obs_ds.read(1)
            
            # Create reference water mask by removing flood pixels from obswater
            ref_data = obs_data.copy()
            ref_data[flood_data != 0] = 0  # Mask out flood pixels
            
            # Calculate ratios
            flood_pixels = (flood_data == 1).sum()
            ref_pixels = (ref_data == 1).sum()  # Count only definite water pixels
            
            return flood_pixels / ref_pixels if ref_pixels > 0 else 0.0

    def get_scene_info(self) -> dict:
        """Get scene polygon and datetime."""
        footprint_file = next(
            (f for f in os.listdir(self.extract_path) if 'footprint' in f.lower()),
            None
        )
        
        if footprint_file:
            with open(os.path.join(self.extract_path, footprint_file)) as f:
                footprint = json.load(f)
                return {
                    'datetime': self.date,
                    'polygon': shape(footprint['geometry'])
                }
        return None

    @staticmethod
    def _extract_tile_id(filename: str) -> str:
        """Extract tile ID from filename."""
        match = re.search(r'_([E]\d{3}[N]\d{3}T\d)_ENSEMBLE_FLOOD_', filename)
        if not match:
            # Try second pattern: ID near end of filename
            match = re.search(r'_([E]\d{3}[N]\d{3}T\d)_\d{8}\.', filename)
            if not match:
                print(f"\n\nOops trouble finding a tile id in filename: {filename}\n\n")
        return match.group(1) if match else None

    def _find_reference_file(self, root: str, tile_id: str) -> Optional[str]:
        """Find reference water file for given tile ID."""
        pattern1 = f"*{tile_id}_REFERENCE_WATER_OUT_*.tif"
        # Pattern 2: Files starting with REFERENCE_WATER_OUT containing tile_id
        pattern2 = f"REFERENCE_WATER_OUT_*{tile_id}*.tif"
    
        matches = list(Path(root).glob(pattern1))
        if not matches:
            matches = list(Path(root).glob(pattern2))
   
        return str(matches[0]) if matches else None

    @staticmethod
    def _calculate_tile_ratio(flood_path: str, ref_path: str) -> float:
        """Calculate flood ratio for a single tile using reference water raster."""
        with rasterio.open(flood_path) as flood_ds, rasterio.open(ref_path) as ref_ds:
            flood_data = flood_ds.read(1)
            ref_data = ref_ds.read(1)
            
            flood_pixels = (flood_data == 1).sum()
            ref_pixels = ((ref_data == 1) | (ref_data == 2)).sum()
            
            return flood_pixels / ref_pixels if ref_pixels > 0 else 0.0

class FlowProcessor:
    """Handles all flow data processing and NWM data extraction."""
    VALID_REGIONS = {'conus', 'alaska', 'hawaii'}

    def __init__(self, features: Dict[str, gpd.GeoDataFrame]):
        self.features = features
        self.gcs_client = storage.Client.create_anonymous_client()
        self.nwm_bucket = self.gcs_client.bucket("national-water-model")

    @classmethod
    def from_shared_features(cls):
        """Create FlowProcessor using shared features."""
        global shared_features
        return cls(shared_features)

    def create_flowfile(self, polygon: Polygon, target_datetime: datetime, region: str = 'conus') -> tuple[Optional[pd.DataFrame], Optional[str]]:
        """
        Create flowfile for a given polygon and datetime.
    
        Args:
            polygon: Shapely polygon of the area of interest
            target_datetime: Target datetime for flow data
    
        Returns:
            Tuple of (DataFrame with feature_id and streamflow columns, NWM version number)
            or (None, None) if no data found
        """
        if region not in self.VALID_REGIONS:
            raise ValueError(f"Invalid region. Must be one of {self.VALID_REGIONS}")
        
        try:
            # Get features in polygon using region-specific hydrofabric
            feature_ids = self.get_features_in_polygon(polygon, region)
            if not feature_ids:
                logging.warning(f"No NWM features found in scene polygon for region {region}")
                return None, None

            # Get flow data for features
            flow_data_result = self.get_flow_data(target_datetime, region)
        
            if flow_data_result is None:
                logging.warning(f"No NWM flow data found for datetime in region {region}")
                return None, None

            flow_data, nwm_version = flow_data_result
        
            # Filter flow data to features in polygon
            filtered_flow_data = flow_data[flow_data['feature_id'].isin(feature_ids)]
        
            return filtered_flow_data, nwm_version
        
        except Exception as e:
            logging.error(f"Error creating flowfile: {str(e)}")
            return None, None

    def get_features_in_polygon(self, polygon: Polygon, region: str) -> List[str]:
        """Get feature IDs that intersect with or are within the polygon."""
        if region not in self.VALID_REGIONS:
            raise ValueError(f"Invalid region. Must be one of {self.VALID_REGIONS}")
            
        features = self.features[region]
        mask = features.intersects(polygon) | features.within(polygon)
        return features[mask]['ID'].tolist()

    def get_flow_data(self, target_datetime: datetime, region: str = 'conus') -> Optional[tuple[pd.DataFrame, str]]:
        """
        Get NWM flow data for a specific datetime and region.
        
        Args:
            target_datetime: Target datetime
            region: 'conus', 'alaska', or 'hawaii'
        
        Returns:
            Tuple of (DataFrame with feature_id and streamflow columns, NWM version number)
            or None if no data found
        """
        # Get closest hour
        closest_hour = self._get_closest_hour(target_datetime)
        
        # Construct file pattern
        file_pattern = self._construct_file_pattern(closest_hour, region)
        
        # Get blob
        blob = self.nwm_bucket.blob(file_pattern)
        if not blob.exists():
            logging.warning(f"No NWM file found for {closest_hour}")
            return None

        try:
            # Create temporary file for NetCDF data
            with tempfile.NamedTemporaryFile(suffix='.nc') as temp_file:
                # Download blob
                blob.download_to_filename(temp_file.name)
                
                # Open NetCDF file
                with xr.open_dataset(temp_file.name) as ds:
                    # Extract NWM version number
                    nwm_version = str(ds.attrs.get('NWM_version_number', 'version_unknown'))
                    
                    # Extract feature_id and streamflow
                    df = pd.DataFrame({
                        'feature_id': ds['feature_id'].values,
                        'streamflow': ds['streamflow'].values
                    })
                    
                    return df, nwm_version

        except Exception as e:
            logging.error(f"Error processing NWM data: {str(e)}")
            return None

    @staticmethod
    def _get_closest_hour(target_datetime: datetime) -> datetime:
        """Get the closest hour in zulu time."""
        rounded = (target_datetime + timedelta(minutes=30)).replace(
            minute=0,
            second=0,
            microsecond=0
        )
        return rounded

    @staticmethod
    def _construct_file_pattern(datetime_obj: datetime, region: str) -> str:
        """
        Construct the NWM file pattern based on datetime and region.
        
        Args:
            datetime_obj: The datetime object
            region: 'conus', 'alaska', or 'hawaii'
        """
        region_map = {
            'conus': {
                'directory': 'analysis_assim',
                'tm_format': 'tm00',
                'suffix': 'conus'
            },
            'alaska': {
                'directory': 'analysis_assim_alaska',
                'tm_format': 'tm00',
                'suffix': 'alaska'
            },
            'hawaii': {
                'directory': 'analysis_assim_hawaii',
                'tm_format': 'tm0000',
                'suffix': 'hawaii'
            }
        }

        if region not in region_map:
            raise ValueError(f"Invalid region. Must be one of {list(region_map.keys())}")

        region_info = region_map[region]
        date_str = datetime_obj.strftime('%Y%m%d')
        hour_str = datetime_obj.strftime('%H')

        return (
            f"nwm.{date_str}/{region_info['directory']}/"
            f"nwm.t{hour_str}z.analysis_assim.channel_rt."
            f"{region_info['tm_format']}.{region_info['suffix']}.nc"
        )

@dataclass
class ProcessingRecord:
    """Holds processing record for each product."""
    product_id: str
    start_time: datetime
    region: str  
    status: str = ""
    error: str = ""
    has_flowfile: bool = False
    was_uploaded: bool = False
    flood_ratio_max: float = 0.0
    end_time: datetime = field(default_factory=datetime.now)

    def update_on_error(self, error_type: str, error_message: str):
        """Update record when processing fails."""
        self.end_time = datetime.now()
        self.error = error_type
        self.status = "failed"
        self.error = error_message

    def update_on_success(self, flood_ratios: dict, has_flowfile: bool, was_uploaded: bool):  
        """Update record when processing succeeds."""
        self.end_time = datetime.now()
        self.status = "success"
        self.has_flowfile = has_flowfile
        self.was_uploaded = was_uploaded
        self.flood_ratio_max = max(flood_ratios.values(), default=0.0)

class Logger:
    """Enhanced logging class with both file and CSV logging."""
    def __init__(self, year: int, month: int):
        self.year = year
        self.month = month
        self.run_timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        self.log_dir = self._setup_log_directory()
        self.processing_records: List[ProcessingRecord] = []
        self._setup_logging()
        self._setup_csv()

    def _setup_log_directory(self) -> Path:
        """Create and return log directory path."""
        log_dir = Path(f"logs/processing-{self.year}_{self.month:02d}_runtime-{self.run_timestamp}")
        log_dir.mkdir(parents=True, exist_ok=True)
        return log_dir

    def _setup_logging(self):
        """Set up file-based logging."""
        log_file = self.log_dir / f"processing.log"
        
        # Create file handler
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(
            logging.Formatter(
                "%(asctime)s - %(levelname)s - %(message)s",
                "%Y-%m-%d %H:%M:%S"
            )
        )

        # Create logger
        self._logger = logging.getLogger(f"gfm_processor_{self.run_timestamp}")
        self._logger.setLevel(logging.INFO)
        self._logger.addHandler(file_handler)
        self._logger.propagate = False

    def info(self, message: str):
        """Log an info message."""
        self._logger.info(message)

    def error(self, message: str):
        """Log an error message."""
        self._logger.error(message)

    def warning(self, message: str):
        """Log a warning message."""
        self._logger.warning(message)

    def _setup_csv(self):
        """Set up CSV logging."""
        self.csv_path = self.log_dir / f"product_status.csv"
        
        with open(self.csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "product_id",
                "region",
                "status",
                "error",
                "has_flowfile",
                "was_uploaded",
                "max_flood_ratio",
                "start_time",
                "end_time",
                "processing_duration_seconds"
            ])

    def start_product_processing(self, product_id: str, region: str) -> ProcessingRecord:
        record = ProcessingRecord(
            product_id=product_id,
            start_time=datetime.now(),
            region=region
        )
        self.processing_records.append(record)
        self.info(f"Starting processing of product: {product_id}")
        return record

    def log_product_success(self, record: ProcessingRecord, flood_ratios: dict, has_flowfile: bool, was_uploaded: bool):
        """Log successful product processing."""
        record.update_on_success(flood_ratios, has_flowfile, was_uploaded)
        self.info(
            f"Successfully processed product {record.product_id}. "
            f"Max flood ratio: {record.flood_ratio_max:.2%}, "
            f"Flowfile created: {has_flowfile}, "
            f"Was uploaded: {was_uploaded}"
        )
        self._write_record(record)

    def log_product_error(self, record: ProcessingRecord, error_type: str, error_message: str):
        """Log product processing error."""
        record.update_on_error(error_type, error_message)
        self.error(
            f"Failed to process product {record.product_id}. "
            f"Error type: {error_type}, Message: {error_message}"
        )
        self._write_record(record)

    def log_monthly_start(self):
        """Log start of monthly processing."""
        self.info(
            f"Starting processing for {self.year}-{self.month:02d}. "
            f"Run ID: {self.run_timestamp}"
        )

    def log_monthly_end(self, total_products: int):
        """Log end of monthly processing and write summary."""
        self.info(
            f"Completed processing for {self.year}-{self.month:02d}. "
            f"Total products attempted: {total_products}"
        )

    def _write_record(self, record: ProcessingRecord):
        """Write a processing record to CSV."""
        duration = (record.end_time - record.start_time).total_seconds()
        
        with open(self.csv_path, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                record.product_id,
                record.region,
                record.status,
                record.error,
                record.has_flowfile,
                record.was_uploaded,  
                f"{record.flood_ratio_max:.4f}",
                record.start_time.isoformat(),
                record.end_time.isoformat(),
                f"{duration:.1f}"
            ])

class Controller:
    def __init__(self, config: Config, debug_mode: bool = False):
        self.config = config
        self.debug_mode = debug_mode 
        # Create necessary directories
        os.makedirs(config.paths['temp'], exist_ok=True)

        # Load region AOIs
        self.region_aois = self._load_region_aois()

        # Load hydrofabric features
        self._load_hydrofabric()

    def _load_hydrofabric(self):
        """Load hydrofabric features into shared_features."""
        global shared_features
        print("Loading hydrofabric features...")
        # Load main hydrofabric
        main_features = gpd.read_file(self.config.paths['main_hydrofabric'])
        if main_features.crs != "EPSG:4326":
            main_features = main_features.to_crs("EPSG:4326")
        # Load Alaska hydrofabric
        ak_features = gpd.read_file(self.config.paths['ak_hydrofabric'])
        if ak_features.crs != "EPSG:4326":
            ak_features = ak_features.to_crs("EPSG:4326")
        # Create dictionary of features
        shared_features = {
            'conus': main_features,
            'hawaii': main_features,
            'alaska': ak_features
        }
        print("Finished loading hydrofabric features.")

    def _load_region_aois(self) -> Dict[str, List]:
        """Load AOI coordinates for each region from the AOI directory."""
        aois = {}
        aoi_dir = Path("AOI")
        if not aoi_dir.exists():
            raise ValueError("AOI directory not found")

        for region in ['conus', 'alaska', 'hawaii']:
            aoi_file = aoi_dir / f"{region}.geojson"
            if not aoi_file.exists():
                continue
            
            try:
                with open(aoi_file) as f:
                    geojson = json.load(f)
                    # Extract coordinates from GeoJSON
                    if geojson['type'] == 'Feature':
                        coords = geojson['geometry']['coordinates'][0]
                    else:
                        coords = geojson['coordinates'][0]
                    aois[region] = coords
            except Exception as e:
                logging.error(f"Error loading AOI for {region}: {str(e)}")
                continue

        if not aois:
            raise ValueError("No valid AOI files found")
        return aois

    def process_monthly_data(self, year: int, month: int):
        """Process data for a given month for all regions."""
        self.logger = Logger(year, month)
        self.logger.log_monthly_start()

        # Create GFM client for getting products (will not be pickled)
        gfm_client = GFMClient(self.config)

        # set number of processes for parallel mode. 
        num_cores = cpu_count()
        num_processes = max(1, num_cores - 1)  # Use N-1 cores, minimum of 1
        self.logger.info(f"Using {num_processes} processes out of {num_cores} available cores")

        try:
            date_range = self._get_date_range(year, month)
            all_products = []
    
            for region, aoi_coords in self.region_aois.items():
                try:
                    self.logger.info(f"Processing region: {region}")
                    aoi_id = gfm_client._create_aoi(aoi_coords)
                    products = gfm_client.get_products(date_range, aoi_id)
                    all_products.extend(products)
                    self.logger.info(
                        f"Found {len(products)} products for region {region}"
                    )

                    if self.debug_mode:
                        # Sequential processing for debugging
                        for product_info in products:
                            try:
                                self.handle_product(product_info['cell_code'], aoi_id, region)
                            except Exception as e:
                                self.logger.error(f"Failed to process product {product_info['cell_code']}: {str(e)}")
                                # Continue with next product
                                continue
                    else:
                        # Parallel processing
                        process_args = [(product_info['cell_code'], aoi_id, region)
                                      for product_info in products]
                        with Pool(processes=num_processes) as pool:
                            # Use starmap_async to prevent exceptions from stopping other processes
                            results = [pool.apply_async(self.handle_product, args) 
                                     for args in process_args]
                            
                            # Wait for all processes and collect any errors
                            for result, (product_id, _, _) in zip(results, process_args):
                                try:
                                    result.get()
                                except Exception as e:
                                    self.logger.error(f"Failed to process product {product_id}: {str(e)}")
                                    # Continue with next product
                                    continue

                except Exception as e:
                    pdb.set_trace()
                    self.logger.error(f"Failed to process region {region}: {str(e)}")
                    continue

            self.logger.log_monthly_end(len(all_products))

        except Exception as e:
            self.logger.error(f"Monthly processing failed: {str(e)}")
            raise

    def handle_product(self, product_id: str, aoi_id: str, region: str):
        """Handle single product processing for specific region."""
        record = self.logger.start_product_processing(product_id, region)

        try:
            # Create new instances for this process
            gfm_client = GFMClient(self.config)
    
            # Create FlowProcessor using shared hydrofabric features
            flow_processor = FlowProcessor.from_shared_features()
    
            # Download and process product using the AOI ID
            product = gfm_client.download_product(product_id, aoi_id)
    
            try:
                # Calculate flood ratios
                flood_ratios = product.calculate_flood_ratios()
            except ValueError as e:
                if "No reference water raster found" in str(e):
                    self.logger.log_product_error(
                        record,
                        error_type="NoReferenceWaterError",
                        error_message=str(e)
                    )
                    if hasattr(product, 'extract_path'):
                        shutil.rmtree(product.extract_path)
                    return  # Exit the function but don't raise the exception
                raise  # Re-raise other ValueError exceptions
    
            # Create flowfile if needed
            flowfile_data = None
            was_uploaded = False
    
            if max(flood_ratios.values(), default=0.0) > self.config.flood_threshold:
                scene_info = product.get_scene_info()
                if scene_info and scene_info.get('polygon'):
                    flow_df, nwm_version = flow_processor.create_flowfile(
                        scene_info['polygon'],
                        scene_info['datetime'],
                        region=region  
                    )
                
                    # Only create flowfile tuple if we have data
                    if flow_df is not None and nwm_version is not None:
                        flowfile_data = (flow_df, nwm_version)
    
                    s3_uploader = S3Uploader(self.config)
            
                    # Upload all files
                    s3_uploader.upload_product_files(
                        product.extract_path,
                        flood_ratios,
                        flowfile_data
                    )
                    was_uploaded = True

            # Log success
            self.logger.log_product_success(
                record,
                flood_ratios,
                flowfile_data is not None,  # Check if we have flowfile data
                was_uploaded
            )
    
            # Clean up
            shutil.rmtree(product.extract_path)
    
        except Exception as e:
            self.logger.log_product_error(
                record,
                error_type=type(e).__name__,
                error_message=str(e)
            )
            if 'product' in locals() and hasattr(product, 'extract_path'):
                try:
                    shutil.rmtree(product.extract_path)
                except Exception as cleanup_error:
                    self.logger.error(
                        f"Failed to clean up product directory: {str(cleanup_error)}"
                    )

    @staticmethod
    def _get_date_range(year: int, month: int) -> tuple:
        """Get start and end dates for given month."""
        start = f"{year}-{month:02d}-01T00:00:00"
        if month == 12:
            end = f"{year + 1}-01-01T00:00:00"
        else:
            end = f"{year}-{month + 1:02d}-01T00:00:00"
        return start, end

def main():
    parser = argparse.ArgumentParser(description='GFM Processing to obtain products that might contain flood observations and their associated flowfiles')
    parser.add_argument('--year', type=int, required=True)
    parser.add_argument('--month', type=int, required=True)
    parser.add_argument('--debug', action='store_true', help='Run in debug mode (sequential processing)')
    args = parser.parse_args()

    config = Config.from_env()
    controller = Controller(config, args.debug)
    controller.process_monthly_data(args.year, args.month)

if __name__ == "__main__":
    # set multiprocessing start method explicitely to handle global nwm features
    multiprocessing.set_start_method('fork')
    main()


