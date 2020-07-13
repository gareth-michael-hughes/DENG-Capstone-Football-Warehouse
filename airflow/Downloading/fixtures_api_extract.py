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
    config.read('/home/workspace/airflow/Configs/ars.cfg')
    
    # Set the API URl
    api_url = "https://api-football-v1.p.rapidapi.com/v2/fixtures/league/2?timezone=Europe/London"
    headers = {
    'x-rapidapi-host': config["API"]['X-RAPIDAPI-HOST'],
    'x-rapidapi-key': config["API"]['X-RAPIDAPI-KEY']
              }
    # Set the output file path variables
    file_name = 'fixtures-'
    folder_name = '/home/workspace/airflow/Data/fixtures/'

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
    data_nested = data['api']['fixtures']
    
    # format the json file and write it to local store
    load_file_name, object_file_name = response_format(data_nested, file_name, folder_name, iterable='')
    object_name="fixture_lists/{}".format(object_file_name)
    
    upload_file_s3(load_file_name, my_bucket, s3_client, object_name=object_name)
    
    # This will remove the generated local json file if set to 'wipe'
    if store_type == 'wipe':
        os.unlink(load_file_name)
        
    print("Uploading to s3 complete")
    
if __name__ == "__main__":
    main()