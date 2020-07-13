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
# from os import listdir path
# from os.path import isfile, join
import sys
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import posixpath as path


def api_extract(api_url, headers):
    """Extracts data from a supplied api endpoint url and extracts the json response 

    Parameters:
    api_url: the api endpoint to query data from
    headers: dictionary that passes Authorization and key string for endpoint authentication
    
    Returns:
    data: extracted data from the api endpoint in json format
    
   """

    # Query the API with headers and params set
    print('Requesting data from {}'.format(api_url))
    response = requests.get(api_url, headers=headers)
    print('Response collected from {}'.format(api_url))

    # Extract JSON data from the response
    print('Extracting json data from response')
    data = response.json()
    print('Data extracted')
    
    # Return the data in json format
    return data


def response_format(data_nested, file_name, folder_name, iterable='', loop_col=''):
    """Custom formats a json file for Redshift loading and returns it 

    Parameters:
    data_nested: a json object, usually nested
    file_name: the name of the file to be created
    folder_name: the folder of the local or workspace location to write the file to
    iterable: a unique value to tag to the file name to ensure s3 object is unique
    loop_col: an additional column to be added to the output file as a unique identifer
    
    Returns:
    path: the full path of the file generated 
    file_return: the unique name of the file generated not including the directory
   """

    print('Shaping json format data for file write')
    # Flatten the json if heavily nested
    flattened = json_normalize(data_nested, sep = "_")
    df = pd.DataFrame(flattened)
    
    if loop_col != '':
        df[loop_col] = int(iterable)
    
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
    path = folder_name+file_name+iterable+'-'+timestamp+ext
    file_return = file_name+iterable+'-'+timestamp+ext

    # Write the file to folder
    file = open(path, "w")
    file.write(clean)
    file.close()
    print('{} created successfully'.format(path))
    
    return path, file_return


def bucket_cleaner(dir_list, home_bucket, s3_client, ACCESS_KEY, SECRET_KEY):
    """Deletes all objects and directories in an s3 bucket and replaces
       each with an empty key

    Parameters:
    dir_list: a list of key names to be recreated in s3
    home_bucket: the name of the s3 bucket to be cleaned
    s3_client: boto3 s3 client for communicating with AWS S3
    ACCESS_KEY: Access key credential for AWS account
    SECRET_KEY: Secret Access key credential for AWS account
    
    Returns:
    None
   """
    
    # Set the home bucket
    s3 = boto3.resource('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)
    bucket = s3.Bucket(home_bucket)
    
    # Delete all objects in the s3 home bucket
    bucket.objects.all().delete()
    
    # Recreate each of the provided object keys
    for dir_path in dir_list:
        s3_client.put_object(Bucket=home_bucket, Key=(dir_path+'/'))


def file_deleter(dir_list):
    """Deletes all local files in a given directory

    Parameters:
    dir_list: a list of directory paths in which to delete all files and sub directories 
    
    Returns:
    None
   """
    for folder in dir_list:
        if os.path.isdir(folder):
            for filename in os.listdir(folder):
                file_path = os.path.join(folder, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    print('Failed to delete %s. Reason: %s' % (file_path, e))
            

def directory_setup(dir_list):
    """Resets the local directory structure with empty folders for storing data

    Parameters:
    dir_list: a list of directory paths to be created for storing extracted data 
    
    Returns:
    None
   """
    
    for dir_path in dir_list:
        try:
            os.mkdir(dir_path)
        except OSError:
            print ("Creation of the directory %s failed" % dir_path)
        else:
            print ("Successfully created the directory %s " % dir_path)
            

def upload_file_s3(load_file_name, bucket, s3_client, object_name=None):
    """Upload a file to an S3 bucket

    Parameters:
    load_file_name: File to upload
    bucket: Bucket to upload to
    s3_client: Client for communicating with s3
    object_name: S3 object name. If not specified then file_name is used
    
    Returns:
    True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = load_file_name

    try:
        print('Attempting to upload file: {} to S3 bucket: {} as {}'.format(load_file_name, bucket, object_name))
        response = s3_client.upload_file(load_file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    print('File successfully uploaded to S3')
    return True


# Create bucket looping lists for upload
def file_lister(path):
    """Generates a list of all files in a given directory

    Parameters:
    path: directory path
    
    Returns:
    file_list: The list of all files in the provided file path
    """
    
    file_list = [f for f in listdir(path) if isfile(join(path, f))]
    return file_list

def explore_data(df):
    """Provides an overview of data in a dataframe including null values distribution

    Parameters:
    df: a pandas dataframe
    
    Returns:
    None
    """
    # Print a summary of the dataframe    
    print(df.info())
    print(df.describe())
    
    # Investigate missing value distribution
    percent_missing = df.isnull().sum() * 100 / len(df)
    missing_value_df = pd.DataFrame({'column_name': df.columns,
                                 'percent_missing': percent_missing})
    missing_value_df.sort_values('percent_missing', inplace=True)
    print('Total number of duplicate rows in Data: {}'.format(len(df)-len(df.drop_duplicates())))
    print('Total percentage missing in Data: {}%'.format(float(round(((df.size - df.count().sum()) / df.size),3)) * 100))
    
    # Plot the missing values in a heatmap
    sns.heatmap(df.isnull(), cbar=False)