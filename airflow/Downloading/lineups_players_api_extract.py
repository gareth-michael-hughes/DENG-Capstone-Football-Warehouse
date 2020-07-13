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
    
    # Set the API URls for both the loop call and the list of values to call
    api_url = "https://api-football-v1.p.rapidapi.com/v2/fixtures/league/2?timezone=Europe/London"
    loop_url = "https://api-football-v1.p.rapidapi.com/v2/lineups/{}"
    headers = {
    'x-rapidapi-host': config["API"]['X-RAPIDAPI-HOST'],
    'x-rapidapi-key': config["API"]['X-RAPIDAPI-KEY']
              }
    
    # Extract out the list of values to feed to the api call
    loop_data = api_extract(api_url, headers)
    flatten = json_normalize(loop_data["api"]['fixtures'], sep = "_")
    loop_values = flatten['fixture_id'].tolist()

    # Set the output file path variables
    file_name = 'lineup-players-'
    folder_name = '/home/workspace/airflow/Data/fixture-lineups/players/'
    
    # Set wipe to True if you wish to delete the file locally after uploading to S3 
    store_type = sys.argv[2]
        
    # Set / read in the access credentials for the S3 bucket
    ACCESS_KEY = config["CREDENTIALS"]['AWS_ACCESS_KEY_ID']
    SECRET_KEY = config["CREDENTIALS"]['AWS_SECRET_ACCESS_KEY']
    my_bucket = config["S3"]["DESTINATION_BUCKET"]
    s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)
    
    # Call the API on a loop, feeding each fixture as a param to extract match stats
    run_type = sys.argv[1]
    if run_type == 'prod':
        loop_list = list(range(0, (len(loop_values)))) # This for a production ready run        
    else:
        loop_list = list(range(0, 2)) # This is for a test run

    for val in loop_list:
        
        # format the url with the correct value from the list
        url_formatted = loop_url.format(loop_values[val])
        
        # extract the data from the api
        data = api_extract(url_formatted, headers)
        data_nested = data['api']['lineUps']
        
        flattened = json_normalize(data_nested, sep = "_")
        df = pd.DataFrame(flattened)
    
        # Step 1 seperate the team line ups into team 1 and team 2
        team_1_df = df. iloc[:, 0:5].copy()
        team_2_df = df. iloc[:, 5:10].copy()

        # Step 2 replace the unique column name with unique names
        team_1_df['team_name'] = team_1_df.columns.values[1][:(len(team_1_df.columns.values[1])-9)]
        team_1_df.columns.values[0] = team_1_df.columns.values[0][-5:]
        team_1_df.columns.values[1] = team_1_df.columns.values[1][-8:]
        team_1_df.columns.values[2] = team_1_df.columns.values[2][-9:]
        team_1_df.columns.values[3] = team_1_df.columns.values[3][-7:]
        team_1_df.columns.values[4] = team_1_df.columns.values[4][-11:]

        team_2_df['team_name'] = team_2_df.columns.values[1][:(len(team_2_df.columns.values[1])-9)]
        team_2_df.columns.values[0] = team_2_df.columns.values[0][-5:]
        team_2_df.columns.values[1] = team_2_df.columns.values[1][-8:]
        team_2_df.columns.values[2] = team_2_df.columns.values[2][-9:]
        team_2_df.columns.values[3] = team_2_df.columns.values[3][-7:]
        team_2_df.columns.values[4] = team_2_df.columns.values[4][-11:]
        
        # Step 3 pull out the team descriptive columns for team 1 and team 2
        team_1_desc = team_1_df[['team_name', 'formation', 'coach_id', 'coach']]
        team_2_desc = team_2_df[['team_name', 'formation', 'coach_id', 'coach']]
        
        # Pull out starting elevens
        team_1_starters = team_1_df['startXI'][0]
        df = pd.DataFrame(team_1_starters)
        
        team_2_starters = team_2_df['startXI'][0]
        df2 = pd.DataFrame(team_2_starters)

        starters = df.append(df2, ignore_index=True)
        starters['type'] = 'StartingXI'

        # Pull out subs
        team_1_subs = team_1_df['substitutes'][0]
        sdf = pd.DataFrame(team_1_subs)
        
        team_2_subs = team_2_df['substitutes'][0]
        sdf2 = pd.DataFrame(team_2_subs)

        subs = sdf.append(sdf2, ignore_index=True)
        subs['type'] = 'Sub'

        players = starters.append(subs, ignore_index=True)
        players['fixture_id'] = loop_values[val]
        values = players.to_json(orient='values')
        clean = values.replace('[[', '[').replace('],[', '][').replace(']]', ']')

        # create file path
        file_name = 'lineup-players-'
        folder_name = '/home/workspace/airflow/Data/fixture-lineups/players/'
        timestamp = datetime.today().strftime('%Y-%m-%d')
        iterable = str(loop_values[val])
        ext = '.json'
        
        path = folder_name+file_name+iterable+'-'+timestamp+ext
        object_file_name = file_name+iterable+'-'+timestamp+ext
    
        # convert back to json
        file = open(path, "w")
        file.write(clean)
        file.close()
    
        load_file_name = path
        
        # set the object name and upload the file to the s3 bucket
        object_name="fixture_lineup_players/{}".format(object_file_name)
        upload_file_s3(load_file_name, my_bucket, s3_client, object_name=object_name)
    
        # This will remove the generated local json file if set to 'wipe'
        if store_type == 'wipe':
            os.unlink(load_file_name)
            
#         time.sleep(3) # used to avoid hitting free tier api rate limit
        
    print("Uploading to s3 complete")

            
if __name__ == "__main__":
    main()