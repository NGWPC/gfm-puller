## Obtaining GFM data likely to contain flood observations

This repo is designed to find Global Flood Monitoring (GFM) scenes likely to contain observations of floods. This is necessary because the GFM data is fully automated and many scenes contain no flood observations. Floods are identified by a value called the flood to baseline ratio that is calculated by dividing the number of flooded pixels in a region by the number of reference inunandation pixels. If this ratio is above a threshold value then the rasters for the GFM tiles within a scene are kept and uploaded to an S3 bucket.

This repositories also attempts to create NWM style flowfiles for the observed flooded areas and times using the NWM analysis and assimilation (ANA) data. This data is currently pulled from a public google cloud bucket. The hourly ANA output that is closest to the start of the scene data take time is used to compute a flowfile estimating the flows present during the GFM observation. 

Data is obtained, analyzed, and uploaded  in 1 month increments.

To obtain GFM data using this repository you need to: 

0. Clone the repository to "pull-gfm"
1. create a directory called "hydrofabrics" inside the repo and copy "nwm_flows.gpkg" and "nwm_flows_alaska_nwmV3_ID.gpkg" to it. This can be done by navigating to "./pull-gfm/hydrofabrics" and then running `aws s3 cp s3://noaa-nws-owp-fim/hand_fim/inputs/nwm_hydrofabric/nwm_flows_alaska_nwmV3_ID.gpkg . --request-payer requester` and `aws s3 cp s3://noaa-nws-owp-fim/hand_fim/inputs/nwm_hydrofabric/nwm_flows_alaska_nwmV3_ID.gpkg . --request-payer requester`  
2. Create and activate a [virtual environment](#environment-setup) using the instructions below.
3. Load AWS credentials into ~/.aws/credentials or your bash environment for the s3 bucket you want to upload data to.
4. Construct a valid [configuration file](#config-file). Instructions for doing this are below.
5. Frow within the "pull-gfm" directory run `python filter_gfm.py --month XX --year YYYY`. XX is a 2 digit month so 01 is January, 02 is February, etc. YYYY is a 4 digit year.

### Virtual environment setup

To setup the virtual environment from which to run the code navigate to the repo directory and run the following code: 

```
# Create virtual environment
python -m venv .venv

# Activate virtual environment
source .venv/bin/activate

# Install dependencies from requirements.txt
pip install -r requirements.txt
```

### Config file

A config file should be created to hold the users GLOFAS GFM account login information and other settings necessary to run the script. A GLOFAS GFM account can be created by going to [the GFM portal](https://portal.gfm.eodc.eu/) and creating a free account. 

Here is an example configuration file:

```
# GFM API Credentials
GFM_EMAIL='EMAIL_HERE_SURROUNDED_BY_SINGLE_QUOTES'
GFM_PASSWORD='PASSWORD_HERE_SURROUNDED_BY_SINGLE_QUOTES'

# Threshold value (0-1) representing the minimum flood to baseline ratio to process a scene
# Example: 0.5 means 50% of reference water pixels must show flooding
FLOOD_THRESHOLD=0.01

# S3 bucket name where processed data will be stored
S3_BUCKET=fimc-data

# S3 root
KEY_ROOT=benchmark/rs/PI4

# NWM Hydrofabric Paths
NWM_MAIN_HYDROFABRIC_PATH=./hydrofabrics/nwm_flows.gpkg
NWM_AK_HYDROFABRIC_PATH=./hydrofabrics/nwm_flows_alaska_nwmV3_ID.gpkg

# Directory for temporary file storage (default: './temp')
TEMP_PATH=./temp
```

## Logs

When a run is initiated a directory is created to store that runs logs inside the `./logs' directory inside the repo. The directory for a given run follows the naming convention: processing-YYYY_MM_runtime-YYYY_MM_DD_HH_MM_SS where the year and month after "processing" indicate the time range being analyzed and the timestamp after "runtime" indicate when the data was acquired. 

Inside a run directory there are two files: "processing.log" and "product_status.csv". "processing.log" contains general INFO, WARNING, and ERROR messages about the run and "product_status.csv" contains a table that lists the processing status of individual GFM scene data directories obtained from the GFM API.

## Workflow results

Results of this workflow can be viewed at [this confluence page](https://confluence.nextgenwaterprediction.com/display/NGWPC/PI4+GFM+data+puller). The page also contains a flowchart outlining the basic algorithm for determining when to keep a directory of GFM data and a class diagram showing how pull-gfm is currently modularized.

## Automation

The script "run_monthly.sh" is designed to be called from cron at the 1st of the month. "run_monthly.sh" is designed run "filter_gfm.py" with the month argument set to the previous month. So for example the script run on Nov 1st, 2024 would obtain data for Oct, 2024. **Note**: The current way of obtaining credentials to the NGWPC bucket `fimc-data` that the data is currently being written to produces credentials that expire after 8hrs. This makes it impossible to sucessfully use "run_monthly.sh" in an automated way. "run_monthly.sh" is being included in the repository as an example of how one would go about automating data acquisition using longer lived AWS credentials.

## Known issues

* The NWM features only cover part of Alaska. If there are observations of flooded scenes in Alaska without NWM features then no flowfiles are created for those scenes.
 
## TODO
