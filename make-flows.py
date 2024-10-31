import pdb
import datetime
import os
from google.cloud import storage
import xarray as xr
import pandas as pd
import geopandas as gpd
import json
from shapely.geometry import shape
from datetime import datetime, timedelta
import tempfile

class NWMDataExtractor:
    def __init__(self):
        """Initialize GCS"""
        self.client = storage.Client.create_anonymous_client()
        self.bucket_name = "national-water-model"
        self.bucket = self.client.bucket(self.bucket_name)
        self.feature_gdf = None
        
    def set_feature_locations(self, feature_gdf):
        """
        Set the GeoDataFrame containing feature_id locations
        Ensures the GeoDataFrame is in WGS84 (EPSG:4326)
        
        Parameters:
        feature_gdf (GeoDataFrame): NWM hydrofabric GeoDataFrame with "ID" and geometry columns
        """
        if not isinstance(feature_gdf, gpd.GeoDataFrame):
            raise ValueError("Input must be a GeoDataFrame")
            
        if 'ID' not in feature_gdf.columns:
            raise ValueError("GeoDataFrame must contain 'ID' column")
        
        # Transform to WGS84 if needed
        if feature_gdf.crs is None:
            raise ValueError("Input GeoDataFrame must have a defined CRS")
        if feature_gdf.crs != "EPSG:4326":
            print(f"Converting from {feature_gdf.crs} to WGS84 (EPSG:4326)")
            feature_gdf = feature_gdf.to_crs("EPSG:4326")
            
        self.feature_gdf = feature_gdf
    
    def get_features_in_polygon(self, polygon):
        """
        Get feature_ids that intersect with or are contained within the input polygon
        
        Parameters:
        polygon: Shapely polygon or GeoJSON-style geometry dict
        
        Returns:
        list: List of feature_ids that intersect or are within the polygon
        """
        if self.feature_gdf is None:
            raise ValueError("Feature locations not set. Call set_feature_locations first.")
            
        # Convert GeoJSON to shapely geometry if necessary
        if isinstance(polygon, dict):
            polygon = shape(polygon)
            
        # Spatial query to find features within or intersecting polygon
        mask = self.feature_gdf.intersects(polygon) | self.feature_gdf.within(polygon)
        return self.feature_gdf[mask]['ID'].tolist()
    
    def get_closest_hour(self, target_datetime):
        """Get the closest hour in zulu time"""
        rounded = (target_datetime + timedelta(minutes=30)).replace(
            minute=0, 
            second=0, 
            microsecond=0
        )
        return rounded

    def construct_file_pattern(self, datetime_obj, region):
        """
        Construct the file pattern based on datetime and region
        
        Parameters:
        datetime_obj (datetime): The datetime object
        region (str): 'conus', 'alaska', or 'hawaii'
        
        Returns:
        str: The complete file pattern for the specified region and time
        """
        # Format the date and hour strings
        date_str = datetime_obj.strftime('%Y%m%d')  # This formats to YYYYMMDD
        hour_str = datetime_obj.strftime('%H')      # This formats to HH
        
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
        
        # Construct the full file pattern
        file_pattern = (
            f"nwm.{date_str}/{region_info['directory']}/"
            f"nwm.t{hour_str}z.analysis_assim.channel_rt."
            f"{region_info['tm_format']}.{region_info['suffix']}.nc"
        )
        
        return file_pattern
    
    def get_data(self, target_datetime, region, polygon=None):
        """
        Get streamflow data for the specified datetime and region,
        optionally filtered by a polygon
        
        Parameters:
        target_datetime (datetime): Target datetime
        region (str): 'conus', 'alaska', or 'hawaii'
        polygon (optional): Shapely polygon or GeoJSON-style geometry dict
        
        Returns:
        pandas.DataFrame: DataFrame with feature_id and streamflow
        """
        # Get closest hour
        closest_hour = self.get_closest_hour(target_datetime)
        
        # Construct file pattern
        file_pattern = self.construct_file_pattern(closest_hour, region)
        
        # Check if blob exists
        blob = self.bucket.blob(file_pattern)
        if not blob.exists():
            raise FileNotFoundError(f"No file found matching pattern: {file_pattern}")
        
        # Create a temporary file to store the NetCDF data
        with tempfile.NamedTemporaryFile(suffix='.nc') as temp_file:
            # Download the blob to the temporary file
            blob.download_to_filename(temp_file.name)
            
            # Open the NetCDF file
            ds = xr.open_dataset(temp_file.name)
            
            # Extract feature_id and streamflow
            df = pd.DataFrame({
                'feature_id': ds['feature_id'].values,
                'streamflow': ds['streamflow'].values
            })
            
            # Filter by polygon if provided
            if polygon is not None:
                if self.feature_gdf is None:
                    raise ValueError("Feature locations not set. Call set_feature_locations first.")
                    
                feature_ids = self.get_features_in_polygon(polygon)
                df = df[df['feature_id'].isin(feature_ids)]
            
            return df
    
    def save_to_csv(self, df, output_path):
        """Save the DataFrame to CSV"""
        df.to_csv(output_path, index=False)

def main(target_datetime, region, output_path, feature_locations_gpkg=None, polygon=None):
    """
    Main function to extract and save NWM data
    
    Parameters:
    target_datetime (datetime): Target datetime
    region (str): 'conus', 'alaska', or 'hawaii'
    output_path (str): Path to save the CSV file
    feature_locations_gpkg (str, optional): Path to GeoPackage with feature locations
    polygon (optional): Shapely polygon or GeoJSON-style geometry dict
    """
    extractor = NWMDataExtractor()
    
    try:
        # Load feature locations if provided
        if feature_locations_gpkg and polygon:
            feature_gdf = gpd.read_file(feature_locations_gpkg)
            extractor.set_feature_locations(feature_gdf)
        
        # Get data with optional spatial filtering
        df = extractor.get_data(target_datetime, region, polygon)
        extractor.save_to_csv(df, output_path)
        print(f"Data successfully saved to {output_path}")
        print(f"Retrieved {len(df)} features")
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")

def test_extractor(gpkg_path=None, geojson_path=None):
    """
    Test the NWM data extractor functionality with spatial filtering for CONUS
    
    Parameters:
    gpkg_path (str): Path to the NWM flows GeoPackage file
    geojson_path (str): Path to the polygon GeoJSON file
    """
    print("Starting NWM Data Extractor test...")
    
    # Set specific test datetime - August 1, 2024 at 12:00 UTC
    test_datetime = datetime(2024, 8, 1, 12, 0, 0)
    print(f"\nTesting with datetime: {test_datetime} UTC")
    
    # Load spatial data if provided
    feature_gdf = None
    polygon = None
    
    if gpkg_path:
        try:
            print(f"\nLoading NWM flows from: {gpkg_path}")
            feature_gdf = gpd.read_file(gpkg_path)
            print(f"Loaded {len(feature_gdf):,} features")
            print(f"Original CRS: {feature_gdf.crs}")
            
        except Exception as e:
            print(f"Error loading GeoPackage: {str(e)}")
            return
    
    if geojson_path:
        try:
            print(f"\nLoading polygon from: {geojson_path}")
            with open(geojson_path, 'r') as f:
                geojson_data = json.load(f)
                # Handle both Feature and FeatureCollection formats
                if geojson_data['type'] == 'FeatureCollection':
                    polygon = shape(geojson_data['features'][0]['geometry'])
                else:
                    polygon = shape(geojson_data['geometry'])
            print(f"Polygon type: {polygon.geom_type}")
            print(f"Polygon bounds: {polygon.bounds}")
        except Exception as e:
            print(f"Error loading GeoJSON: {str(e)}")
            return
    
    # Create a temporary directory for test outputs
    with tempfile.TemporaryDirectory() as temp_dir:
        extractor = NWMDataExtractor()
        
        # Set feature locations if available
        if feature_gdf is not None:
            extractor.set_feature_locations(feature_gdf)
        
        # Test only CONUS
        region = 'conus'
        print(f"\nTesting {region} region...")
        print("-" * 50)
        
        # Test file pattern construction
        try:
            pattern = extractor.construct_file_pattern(test_datetime, region)
            print(f"File pattern: {pattern}")
        except Exception as e:
            print(f"Error constructing file pattern: {str(e)}")
            return
        
        # Test data extraction
        try:
            output_file = os.path.join(temp_dir, f"test_{region}.csv")
            
            # Get data with spatial filtering
            df = extractor.get_data(test_datetime, region, polygon)
            
            # Basic data validation
            print(f"Features retrieved: {len(df):,}")
            if polygon is not None:
                print(f"Features filtered by polygon: {len(df):,}")
            print(f"Columns present: {df.columns.tolist()}")
            
            if len(df) > 0:
                print("\nStreamflow statistics:")
                print(f"  Min: {df['streamflow'].min():.2f} m³/s")
                print(f"  Max: {df['streamflow'].max():.2f} m³/s")
                print(f"  Mean: {df['streamflow'].mean():.2f} m³/s")
                print(f"  Median: {df['streamflow'].median():.2f} m³/s")
            
            pdb.set_trace()
            # Test CSV writing
            extractor.save_to_csv(df, output_file)
            print(f"\nData saved to: {output_file}")
            
            # Verify saved file
            df_read = pd.read_csv(output_file)
            assert len(df_read) == len(df), "CSV file length mismatch"
            print("CSV file verification: ✓ Successful")
            
        except FileNotFoundError as e:
            print(f"Warning: No data file found. This is expected for future dates.")
            print(f"Details: {str(e)}")
        except Exception as e:
            print(f"Error during processing: {str(e)}")
        
        print("-" * 50)

if __name__ == "__main__":
    # Specify paths to your data files
    gpkg_path = "./Data/nwm_flows.gpkg"
    geojson_path = "./Data/test-scene.geojson"
    
    # Run test with spatial data
    test_extractor(gpkg_path, geojson_path)
