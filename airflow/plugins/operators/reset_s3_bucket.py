from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import boto3
from botocore.exceptions import ClientError
import os
from os import listdir
from os.path import isfile, join

class BucketResetOperator(BaseOperator):
    ui_color = '#80BD9E'
    
    # Define your operators params (with defaults) here
    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 s3_bucket="",
                 dir_list = [],
                 *args, **kwargs):

        # Map params here        
        super(BucketResetOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id=aws_credentials_id
        self.s3_bucket=s3_bucket
        self.dir_list=dir_list

    def execute(self, context):
        # Extract and set all crednetials for s3 authentication
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        s3_client = boto3.client('s3', aws_access_key_id=credentials.access_key,
                      aws_secret_access_key=credentials.secret_key)
        s3 = boto3.resource('s3', aws_access_key_id=credentials.access_key,
                      aws_secret_access_key=credentials.secret_key)
        home_bucket = s3.Bucket(self.s3_bucket)

        self.log.info("Deleting all objects from the S3 bucket: {}".format(self.s3_bucket))        
        # Delete all objects in the s3 home bucket
        home_bucket.objects.all().delete()
    
        # Recreate each of the provided object keys
        self.log.info("Recreating all object keys")
        for dir_path in self.dir_list:
            s3_client.put_object(Bucket=self.s3_bucket, Key=(dir_path+'/'))
            self.log.info("Created key: {}".format(dir_path+'/'))

        
        self.log.info("Bucket reset with empty keys: {}".format(self.s3_bucket))