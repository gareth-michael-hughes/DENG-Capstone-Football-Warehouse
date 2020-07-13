from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import os
from os import listdir
from os.path import isfile, join
import shutil

class LocalResetOperator(BaseOperator):
    ui_color = '#80BD9E'
    
    # Define your operators params (with defaults) here
    @apply_defaults
    def __init__(self,
                 dir_list = [],
                 *args, **kwargs):

        # Map params here        
        super(LocalResetOperator, self).__init__(*args, **kwargs)
        self.dir_list=dir_list

    def execute(self, context):
        # Extract and set all crednetials for s3 authentication
        for folder in self.dir_list:
            if os.path.isdir(folder):
                self.log.info("Deleting all local files from : {}".format(folder))
        
            # Delete all objects in the s3 home bucket
                for filename in os.listdir(folder):
                    if os.path.isdir(folder):
                        file_path = os.path.join(folder, filename)
                        try:
                            if os.path.isfile(file_path) or os.path.islink(file_path):
                                os.unlink(file_path)
                            elif os.path.isdir(file_path):
                                shutil.rmtree(file_path)
                        except Exception as e:
                            print('Failed to delete %s. Reason: %s' % (file_path, e))

        self.log.info("All local files deleted")
        
        self.log.info("Recreating directory structure for data capture")
        for dir_path in self.dir_list:
            try:
                os.mkdir(dir_path)
            except OSError:
                print ("Creation of the directory %s failed" % dir_path)
            else:
                print ("Successfully created the directory %s " % dir_path)
        
        self.log.info("Directory structure recreated")