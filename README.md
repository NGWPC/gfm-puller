## Obtaining GFM data likely to contain flood observations

This repo is designed to find Global Flood Monitoring (GFM) scenes likely to contain observations of floods. This is necessary because the GFM data is fully automated and many scenes contain no flood observations. Floods are identified by a value called the flood to baseline ratio that is calculated by dividing the number of flooded pixels in a region by the number of reference inundation pixels. If this ratio is above a threshold value then the rasters for the GFM tiles within a scene are kept and uploaded to an S3 bucket.

This repository also attempts to create NWM style flowfiles for the observed flooded areas and times using the NWM analysis and assimilation (ANA) data. This data is currently pulled from a public google cloud bucket. The hourly ANA output that is closest to the start of the scene data take time is used to compute a flowfile estimating the flows present during the GFM observation. 

Data is obtained, analyzed, and uploaded in 1 month increments.

To obtain GFM data using this repository you need to: 

0. Clone the repository to "pull-gfm"
1. create a directory called "hydrofabrics" inside the repo and copy "nwm_flows.gpkg" and "nwm_flows_alaska_nwmV3_ID.gpkg" to it. This can be done by navigating to "./pull-gfm/hydrofabrics" and then running `aws s3 cp s3://noaa-nws-owp-fim/hand_fim/inputs/nwm_hydrofabric/nwm_flows_alaska_nwmV3_ID.gpkg . --request-payer requester` and `aws s3 cp s3://noaa-nws-owp-fim/hand_fim/inputs/nwm_hydrofabric/nwm_flows_alaska_nwmV3_ID.gpkg . --request-payer requester`  
2. Build and enter the docker container using the instructions below.
3. Load AWS credentials into docker containers  ~/.aws/credentials or the containers bash environment for the s3 bucket you want to upload data to.
4. Construct a valid [configuration file](#config-file) inside the running container. Instructions for doing this are below.
5. From within the containers "app" directory run `python filter_gfm.py --month XX --year YYYY`. XX is a 2 digit month so 01 is January, 02 is February, etc. YYYY is a 4 digit year.

### Building and entering docker environment

To setup the container from which to run the code navigate to the repo directory and run the following code: 

```
# build docker image
docker build -t gfm-puller .
# mount script directory and enter container shell
sudo docker run -v $(pwd):/app -it gfm-puller-image bash
```

From their you can run the main python script and inspect the output or run one of the accessory scripts in the "scripts" directory.

### .env file

A .env config file is used to hold config settings for the script that aren't set with an argument. A default .env file is included with the repository. It assumes that you have a GLOFAS GFM account email address and password. A GLOFAS GFM account can be created by going to [the GFM portal](https://portal.gfm.eodc.eu/) and creating a free account. Once you have obtained an account then set the GFM_EMAIL and GFM_PASSWORD in the containers bash environment variables using:

```
export GFM_EMAIL='"<GFM_account_email>"'
export GFM_PASSWORD='"<GFM_account_password>"'
```

AWS credentials capable of accessing the s3 bucket where the filtered GFM data will be uploaded should also be entered into the default profile of the containers .aws/credentials file or in the shell environment that the script is being run within.

## Logs

When a run is initiated a directory is created to store that runs logs inside the `./logs' directory inside the repo. The directory for a given run follows the naming convention: processing-YYYY_MM_runtime-YYYY_MM_DD_HH_MM_SS where the year and month after "processing" indicate the time range being analyzed and the timestamp after "runtime" indicate when the data was acquired. 

Inside a run directory there are two files: "processing.log" and "product_status.csv". "processing.log" contains general INFO, WARNING, and ERROR messages about the run and "product_status.csv" contains a table that lists the processing status of individual GFM scene data directories obtained from the GFM API.

## Workflow results

Results of this workflow can be viewed at [this confluence page](https://confluence.nextgenwaterprediction.com/display/NGWPC/PI4+GFM+data+puller). The page also contains a flowchart outlining the basic algorithm for determining when to keep a directory of GFM data and a class diagram showing how pull-gfm is currently modularized.

## Automation

The script "run_monthly.sh" inside the scripts directory is designed to be called from cron at the 1st of the month. "run_monthly.sh" is designed run "filter_gfm.py" with the month argument set to the previous month. So for example the script run on Nov 1st, 2024 would obtain data for Oct, 2024. **Note**: The current way of obtaining credentials to the NGWPC bucket `fimc-data` that the data is currently being written to produce credentials that expire after 8hrs. This makes it impossible to successfully use "run_monthly.sh" in an automated way. "run_monthly.sh" is being included in the repository as an example of how one would go about automating data acquisition using longer lived AWS credentials.

## Known issues

* The NWM features only cover part of Alaska. If there are observations of flooded scenes in Alaska without NWM features then no flowfiles are created for those scenes.
 
