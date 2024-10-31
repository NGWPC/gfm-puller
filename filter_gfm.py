import os
import json
import csv
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Dict, Optional
import pandas as pd
import geopandas as gpd
import rasterio
import boto3
import httpx
import tempfile
import zipfile
import shutil
from pathlib import Path
import posixpath
from google.cloud import storage
import xarray as xr
import tempfile

@dataclass
class Config:
    """Simplified configuration class."""
    flood_threshold: float
    s3_bucket: str
    key_root: str
    credentials: Dict[str, str]
    paths: Dict[str, str]

    @classmethod
    def from_env(cls):
        return cls(
            flood_threshold=float(os.getenv('FLOOD_THRESHOLD')),
            s3_bucket=os.getenv('S3_BUCKET'),
            key_root=os.getenv('KEY_ROOT').strip('/'),
            credentials={
                'gfm_email': os.getenv('GFM_EMAIL'),
                'gfm_password': os.getenv('GFM_PASSWORD')
            },
            paths={
                'hydrofabric': os.getenv('NWM_HYDROFABRIC_PATH'),
                'output': os.getenv('OUTPUT_PATH', 'output'),
                'temp': os.getenv('TEMP_PATH', 'temp')
            }
        )

class S3Uploader:
    """Handles all S3 upload operations."""
    def __init__(self, config: Config):
        self.bucket = config.s3_bucket
        self.key_root = config.key_root
        self.threshold = config.flood_threshold
        self.s3_client = boto3.client('s3')

    def upload_product_files(self, product_path: str, flood_fractions: dict, flowfile: Optional[pd.DataFrame] = None):
        """
        Upload all product files, flood fractions, and optional flowfile to S3.
        
        Args:
            product_path: Path to extracted product directory
            flood_fractions: Dictionary of flood fractions by tile
            flowfile: Optional DataFrame containing flow data
        """
        # Get product ID and date from path
        product_id = os.path.basename(product_path)
        date_input = self._extract_date_from_product_id(product_id)
        
        # Construct the base key for this product
        base_key = self.construct_key(product_id, date_input)
        
        try:
            # Save and upload flood fractions
            flood_fractions_path = os.path.join(product_path, 'flood_fractions.json')
            with open(flood_fractions_path, 'w') as f:
                json.dump(flood_fractions, f, indent=4)
            
            # Upload all files in the product directory
            for root, _, files in os.walk(product_path):
                for filename in files:
                    file_path = os.path.join(root, filename)
                    # Compute relative path from product directory
                    relative_path = os.path.relpath(file_path, product_path)
                    s3_key = posixpath.join(base_key, relative_path)
                    
                    self.s3_client.upload_file(file_path, self.bucket, s3_key)
            
            # Upload flowfile if provided
            if flowfile is not None:
                flowfile_path = os.path.join(product_path, 'flowfile.csv')
                flowfile.to_csv(flowfile_path, index=False)
                s3_key = posixpath.join(base_key, 'flowfile.csv')
                self.s3_client.upload_file(flowfile_path, self.bucket, s3_key)

        except Exception as e:
            logging.error(f"Failed to upload files for {product_id}: {str(e)}")
            raise

    def construct_key(self, product_id: str, date_input: str) -> str:
        """Construct S3 key for product."""
        return posixpath.join(
            self.key_root,
            f"fraction_threshold_{self.threshold}",
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
        self._authenticate()

    def _authenticate(self):
        """Authenticate with GFM API."""
        response = httpx.post(
            'https://api.gfm.eodc.eu/v2/auth/login',
            json=self.config.credentials
        )
        self.headers = {'Authorization': f"Bearer {response.json()['access_token']}"}

    def get_products(self, date_range: tuple) -> list:
        """Get list of products for date range."""
        response = httpx.get(
            'https://api.gfm.eodc.eu/v2/products',
            headers=self.headers,
            params={'from': date_range[0], 'to': date_range[1]}
        )
        return response.json()['products']

    def download_product(self, product_id: str) -> 'Product':
        """Download and extract product, return Product instance."""
        # Get download link
        response = httpx.get(
            f'https://api.gfm.eodc.eu/v2/download/{product_id}',
            headers=self.headers
        )
        download_link = response.json()['download_link']
        
        # Create temp directory for this product
        extract_path = os.path.join(self.config.paths['temp'], product_id)
        os.makedirs(extract_path, exist_ok=True)
        
        # Download and extract
        download_path = os.path.join(self.config.paths['temp'], f'{product_id}.zip')
        urllib.request.urlretrieve(download_link, download_path)
        
        with zipfile.ZipFile(download_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
        
        # Clean up zip file
        os.remove(download_path)
        
        return Product(
            id=product_id,
            date=self._parse_date_from_product_id(product_id),
            extract_path=extract_path
        )

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

    def calculate_flood_fractions(self) -> dict:
        """Calculate flood fractions for all tiles in the product."""
        fractions = {}
        
        # Find and process all flood and reference water files
        for root, _, files in os.walk(self.extract_path):
            for file in files:
                if 'ENSEMBLE_FLOOD' in file:
                    tile_id = self._extract_tile_id(file)
                    flood_path = os.path.join(root, file)
                    ref_path = self._find_reference_file(root, tile_id)
                    
                    if ref_path:
                        fraction = self._calculate_tile_fraction(flood_path, ref_path)
                        fractions[tile_id] = fraction
        
        return fractions

    def get_scene_info(self) -> dict:
        """Get scene polygon and datetime."""
        metadata_file = next(
            (f for f in os.listdir(self.extract_path) if f.endswith('_metadata.json')),
            None
        )
        
        if metadata_file:
            with open(os.path.join(self.extract_path, metadata_file)) as f:
                metadata = json.load(f)
                return {
                    'datetime': self.date,
                    'polygon': shape(metadata['geometry'])
                }
        return None

    @staticmethod
    def _extract_tile_id(filename: str) -> str:
        """Extract tile ID from filename."""
        match = re.search(r'_(\w+)_ENSEMBLE_FLOOD_', filename)
        return match.group(1) if match else None

    def _find_reference_file(self, root: str, tile_id: str) -> Optional[str]:
        """Find reference water file for given tile ID."""
        pattern = f"*{tile_id}_REFERENCE_WATER_OUT_*.tif"
        matches = list(Path(root).glob(pattern))
        return str(matches[0]) if matches else None

    @staticmethod
    def _calculate_tile_fraction(flood_path: str, ref_path: str) -> float:
        """Calculate flood fraction for a single tile."""
        with rasterio.open(flood_path) as flood_ds, rasterio.open(ref_path) as ref_ds:
            flood_data = flood_ds.read(1)
            ref_data = ref_ds.read(1)
            
            flood_pixels = (flood_data == 1).sum()
            ref_pixels = ((ref_data == 1) | (ref_data == 2)).sum()
            
            return flood_pixels / ref_pixels if ref_pixels > 0 else 0.0

class FlowProcessor:
    """Handles all flow data processing and NWM data extraction."""
    def __init__(self, hydrofabric_path: str):
        """
        Initialize the flow processor.
        
        Args:
            hydrofabric_path: Path to the NWM hydrofabric GeoPackage
        """
        # Load hydrofabric features
        self.features = gpd.read_file(hydrofabric_path)
        if 'ID' not in self.features.columns:
            raise ValueError("GeoPackage must contain 'ID' column")
        
        # Ensure proper CRS
        if self.features.crs != "EPSG:4326":
            self.features = self.features.to_crs("EPSG:4326")
        
        # Initialize GCS client for NWM data
        self.gcs_client = storage.Client.create_anonymous_client()
        self.nwm_bucket = self.gcs_client.bucket("national-water-model")

    def create_flowfile(self, polygon: Polygon, target_datetime: datetime) -> pd.DataFrame:
        """
        Create flowfile for a given polygon and datetime.
        
        Args:
            polygon: Shapely polygon of the area of interest
            target_datetime: Target datetime for flow data
        
        Returns:
            DataFrame with feature_id and streamflow columns
        """
        # Get features in polygon
        feature_ids = self.get_features_in_polygon(polygon)
        if not feature_ids:
            logging.warning("No features found in polygon")
            return pd.DataFrame(columns=['feature_id', 'streamflow'])

        # Get flow data for features
        flow_data = self.get_flow_data(target_datetime, 'conus')
        
        if flow_data is None:
            logging.warning("No flow data found for datetime")
            return pd.DataFrame(columns=['feature_id', 'streamflow'])

        # Filter flow data to features in polygon
        return flow_data[flow_data['feature_id'].isin(feature_ids)]

    def get_features_in_polygon(self, polygon: Polygon) -> List[str]:
        """Get feature IDs that intersect with or are within the polygon."""
        mask = self.features.intersects(polygon) | self.features.within(polygon)
        return self.features[mask]['ID'].tolist()

    def get_flow_data(self, target_datetime: datetime, region: str = 'conus') -> Optional[pd.DataFrame]:
        """
        Get NWM flow data for a specific datetime and region.
        
        Args:
            target_datetime: Target datetime
            region: 'conus', 'alaska', or 'hawaii'
        
        Returns:
            DataFrame with feature_id and streamflow columns
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
                    # Extract feature_id and streamflow
                    df = pd.DataFrame({
                        'feature_id': ds['feature_id'].values,
                        'streamflow': ds['streamflow'].values
                    })
                    
                    return df

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
    status: str = ""
    error: str = ""
    has_flowfile: bool = False
    flood_fraction_max: float = 0.0
    end_time: datetime = field(default_factory=datetime.now)

    def update_on_error(self, error_type: str, error_message: str):
        """Update record when processing fails."""
        self.end_time = datetime.now()
        self.error = error_type
        self.status = "failed"
        self.error = error_message

    def update_on_success(self, flood_fractions: dict, has_flowfile: bool):
        """Update record when processing succeeds."""
        self.end_time = datetime.now()
        self.status = "success"
        self.has_flowfile = has_flowfile
        self.flood_fraction_max = max(flood_fractions.values(), default=0.0)

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
        log_dir = Path(f"logs/{self.year}_{self.month:02d}_{self.run_timestamp}")
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
        self.logger = logging.getLogger(f"gfm_processor_{self.run_timestamp}")
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(file_handler)
        self.logger.propagate = False

    def _setup_csv(self):
        """Set up CSV logging."""
        self.csv_path = self.log_dir / f"processing_records.csv"
        
        with open(self.csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                "product_id",
                "status",
                "error",
                "has_flowfile",
                "max_flood_fraction",
                "start_time",
                "end_time",
                "processing_duration_seconds"
            ])

    def start_product_processing(self, product_id: str) -> ProcessingRecord:
        """Start tracking a product's processing."""
        record = ProcessingRecord(
            product_id=product_id,
            start_time=datetime.now()
        )
        self.processing_records.append(record)
        self.logger.info(f"Starting processing of product: {product_id}")
        return record

    def log_product_success(self, record: ProcessingRecord, flood_fractions: dict, has_flowfile: bool):
        """Log successful product processing."""
        record.update_on_success(flood_fractions, has_flowfile)
        self.logger.info(
            f"Successfully processed product {record.product_id}. "
            f"Max flood fraction: {record.flood_fraction_max:.2%}, "
            f"Flowfile created: {has_flowfile}"
        )
        self._write_record(record)

    def log_product_error(self, record: ProcessingRecord, error_type: str, error_message: str):
        """Log product processing error."""
        record.update_on_error(error_type, error_message)
        self.logger.error(
            f"Failed to process product {record.product_id}. "
            f"Error type: {error_type}, Message: {error_message}"
        )
        self._write_record(record)

    def log_monthly_start(self):
        """Log start of monthly processing."""
        self.logger.info(
            f"Starting processing for {self.year}-{self.month:02d}. "
            f"Run ID: {self.run_timestamp}"
        )

    def log_monthly_end(self, total_products: int):
        """Log end of monthly processing and write summary."""
        self.logger.info(
            f"Completed processing for {self.year}-{self.month:02d}. "
            f"Total products attempted: {total_products}"
        )
        self._write_summary()

    def _write_record(self, record: ProcessingRecord):
        """Write a processing record to CSV."""
        duration = (record.end_time - record.start_time).total_seconds()
        
        with open(self.csv_path, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                record.product_id,
                record.status,
                record.error,
                record.has_flowfile,
                f"{record.flood_fraction_max:.4f}",
                record.start_time.isoformat(),
                record.end_time.isoformat(),
                f"{duration:.1f}"
            ])

    def _write_summary(self):
        """Write processing summary to log file."""
        total = len(self.processing_records)
        successful = sum(1 for r in self.processing_records if r.status == "success")
        failed = sum(1 for r in self.processing_records if r.status == "failed")
        with_flowfiles = sum(1 for r in self.processing_records if r.has_flowfile)
        
        summary = (
            f"\nProcessing Summary\n"
            f"=================\n"
            f"Total products processed: {total}\n"
            f"Successful: {successful}\n"
            f"Failed: {failed}\n"
            f"Products with flowfiles: {with_flowfiles}\n"
            f"Success rate: {(successful/total*100):.1f}%\n"
        )
        
        self.logger.info(summary)

class Controller:
    """Main controller with enhanced logging."""
    def __init__(self, config: Config):
        self.config = config
        self.gfm_client = GFMClient(config)
        self.flow_processor = FlowProcessor(config.paths['hydrofabric'])
        self.s3_uploader = S3Uploader(config)
        os.makedirs(config.paths['output'], exist_ok=True)
        os.makedirs(config.paths['temp'], exist_ok=True)

    def process_monthly_data(self, year: int, month: int):
        """Process data for a given month."""
        self.logger = Logger(year, month)
        self.logger.log_monthly_start()

        try:
            # Get date range for the month
            date_range = self._get_date_range(year, month)

            # Get list of products for the month
            products = self.gfm_client.get_products(date_range)
            self.logger.logger.info(f"Found {len(products)} products to process")

            # Process each product
            for product_info in products:
                try:
                    self.handle_product(product_info['id'])
                except Exception as e:
                    self.logger.logger.error(
                        f"Failed to process product {product_info['id']}: {str(e)}"
                    )
                    continue  # Continue with next product even if one fails

            self.logger.log_monthly_end(len(products))

        except Exception as e:
            self.logger.logger.error(f"Monthly processing failed: {str(e)}")
            raise

    def handle_product(self, product_id: str):
        """Handle single product processing."""
        record = self.logger.start_product_processing(product_id)
        
        try:
            # Download and process product
            product = self.gfm_client.download_product(product_id)
            
            # Calculate flood fractions
            flood_fractions = product.calculate_flood_fractions()
            
            # Create flowfile if needed
            flowfile = None
            if max(flood_fractions.values()) > self.config.flood_threshold:
                scene_info = product.get_scene_info()
                if scene_info and scene_info.get('polygon'):
                    flowfile = self.flow_processor.create_flowfile(
                        scene_info['polygon'],
                        scene_info['datetime']
                    )
            
            # Upload all files
            self.s3_uploader.upload_product_files(
                product.extract_path,
                flood_fractions,
                flowfile if flowfile is not None and not flowfile.empty else None
            )
            
            # Log success
            self.logger.log_product_success(
                record,
                flood_fractions,
                flowfile is not None and not flowfile.empty
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
                    self.logger.logger.error(
                        f"Failed to clean up product directory: {str(cleanup_error)}"
                    )
            raise

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
    """Main execution function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='GFM Processing System')
    parser.add_argument('--year', type=int, required=True)
    parser.add_argument('--month', type=int, required=True)
    args = parser.parse_args()

    config = Config.from_env()
    controller = Controller(config)
    controller.process_monthly_data(args.year, args.month)

if __name__ == "__main__":
    main()


