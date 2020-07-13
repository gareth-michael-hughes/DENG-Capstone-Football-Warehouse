# Import necessary packages
import requests
import pandas as pd
import json
import itertools
from pandas.io.json import json_normalize
import time
from datetime import datetime
import shutil
import logging
import boto3
from botocore.exceptions import ClientError
import os
from os import listdir
from os.path import isfile, join
from api_upload_helpers import api_extract, response_format, upload_file_s3
import configparser
import sys

def main():
    
    # Read in the configuration specifications
    config = configparser.ConfigParser()
    config.read('/home/workspace/gh_project/Configs/ars.cfg')
    
    # Set the API URl
    api_url = "https://api-football-v1.p.rapidapi.com/v2/fixtures/rounds/2"
    headers = {
    'x-rapidapi-host': config["API"]['X-RAPIDAPI-HOST'],
    'x-rapidapi-key': config["API"]['X-RAPIDAPI-KEY']
              }
    
    # Set the output file path variables
    file_name = 'fixture-rounds-'
    folder_name = '/home/workspace/gh_project/Data/rounds/'

    # Set wipe to True if you wish to delete the file locally after uploading to S3 
    store_type = sys.argv[1]
        
    # Set / read in the access credentials for the S3 bucket
    ACCESS_KEY = config["CREDENTIALS"]['AWS_ACCESS_KEY_ID']
    SECRET_KEY = config["CREDENTIALS"]['AWS_SECRET_ACCESS_KEY']
    my_bucket = config["S3"]["DESTINATION_BUCKET"]
    s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)
    
    # extract the api response data
    data = api_extract(api_url, headers)
    flattened = data["api"]["fixtures"]
    
    print('Shaping json format data for file write')
    # Flatten the json if heavily nested
    df = pd.DataFrame(flattened)
        
    # Quick null value check for a bad read
    if (float(round(((df.size - df.count().sum()) / df.size),3)) * 100) > 50.0:
        print('File dumped due to {}% missing values'.format(float(round(((df.size - df.count().sum()) / df.size),3)) * 100))
        sys.exit()
    else:
        print('Data quality passed at: {}%'.format(float(round(((df.size - df.count().sum()) / df.size),3)) * 100))
        
    # Use the values orientation to create arrays of json values
    values = df.to_json(orient='values')
    # Clean the json format to comply redshift json COPY from s3 to Redshift
    clean = values.replace('[[', '[').replace('],[', '][').replace(']]', ']')
    print('Data formatting completed')
    
    # create file path
    timestamp = datetime.today().strftime('%Y-%m-%d')
    ext = '.json'
    path = folder_name+file_name+'-'+timestamp+ext
    file_return = file_name+'-'+timestamp+ext

    # Write the file to folder
    file = open(path, "w")
    file.write(clean)
    file.close()
    print('{} created successfully'.format(path))
    
    # format the json file and write it to local store
    load_file_name = path
    object_file_name = file_return
    
    object_name="rounds/{}".format(object_file_name)
    
    upload_file_s3(load_file_name, my_bucket, s3_client, object_name=object_name)
    
    # This will remove the generated local json file if set to 'wipe'
    if store_type == 'wipe':
        os.unlink(load_file_name)
        
    print("Uploading to s3 complete")
    
if __name__ == "__main__":
    main()