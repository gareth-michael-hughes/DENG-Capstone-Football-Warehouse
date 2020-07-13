# Import necessary packages
import pandas as pd
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
from api_upload_helpers import upload_file_s3, file_lister
import configparser
import sys

def main():
    
    # Set the output file path variables
    file_name = "/home/workspace/airflow/Data/fantasy-ratings/overall/{}"
    folder_name = "/home/workspace/airflow/Data/fantasy-ratings/overall/"

    # Set wipe to True if you wish to delete the file locally after uploading to S3 
    store_type = sys.argv[1]
    
    # Set / read in the access credentials for the S3 bucket
    config = configparser.ConfigParser()
    config.read('/home/workspace/airflow/Configs/ars.cfg')
    
    # Set / read in the access credentials for the S3 bucket
    ACCESS_KEY = config["CREDENTIALS"]['AWS_ACCESS_KEY_ID']
    SECRET_KEY = config["CREDENTIALS"]['AWS_SECRET_ACCESS_KEY']
    my_bucket = config["S3"]["DESTINATION_BUCKET"]
    s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)
    
    # Create bucket looping lists for upload
    file_list = file_lister(folder_name)

    # Uploading fifa ratings    
    for file in file_list:
        upload_file_s3(file_name.format(file),bucket=my_bucket, \
                       object_name="fantasy_overall_ratings/{}".format(file), s3_client=s3_client)
            # This will remove the generated local json file if True
        if store_type == 'wipe':
            os.unlink(load_file_name)
    
    print("File loading complete")
        
        
if __name__ == "__main__":
    main()