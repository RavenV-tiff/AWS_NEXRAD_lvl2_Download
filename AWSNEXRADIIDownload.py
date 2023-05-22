import os
import pandas as pd
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from datetime import datetime, timedelta
import time

df = pd.read_csv('2022interptornradar.csv')
df['datetime'] = pd.to_datetime(df[['year', 'month', 'day', 'hour', 'minute']])
s3 =  boto3.client('s3', config=Config(signature_version=UNSIGNED))
last_responses = {}

# AWS S3 bucket and folder information
s3_bucket = 'noaa-nexrad-level2'

# Function to download a file from AWS S3
def download_from_s3(file_key, local_path):
    s3.download_file(s3_bucket, file_key, local_path)
    print(f"Downloaded file: {file_key}")

# Function to search for the nearest valid file
def find_nearest_file(dataframe, datetime_column, radarsite):
    datetime_value = datetime_column
    year = str(datetime_value.year)
    month = str(datetime_value.month).zfill(2)
    date = str(datetime_value.day).zfill(2)
    hour = str(datetime_value.hour).zfill(2)
    minute = str(datetime_value.minute).zfill(2)
    second = str(datetime_value.second).zfill(2)

    # Check if the radarsite has changed or the date has changed
    if radarsite in last_responses:
        last_date, filtered_objects, sorted_objects = last_responses[radarsite]
        if last_date == date:
            # Reuse the filtered and sorted objects for the same radarsite and date
            pass
        else:
            # Create a new response by listing objects in the AWS S3 bucket
            response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=f"{year}/{month}/{date}/{radarsite}/")
            if 'Contents' in response:
                objects = response['Contents']
                filtered_objects = [obj for obj in objects if not obj['Key'].endswith('_MDM')]
                if filtered_objects:
                    sorted_objects = sorted(filtered_objects, key=lambda obj: obj['Key'])
                    last_responses[radarsite] = (date, filtered_objects, sorted_objects)
                else:
                    print("No valid files found." + radarsite + ' ' + year + '/' + month + '/' + date + ' ' + hour + ':' + minute)
                    return
            else:
                print("No objects found in S3 bucket.")
                return
    else:
        response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=f"{year}/{month}/{date}/{radarsite}/")
        if 'Contents' in response:
            objects = response['Contents']
            filtered_objects = [obj for obj in objects if not obj['Key'].endswith('_MDM')]
            if filtered_objects:
                sorted_objects = sorted(filtered_objects, key=lambda obj: obj['Key'])
                last_responses[radarsite] = (date, filtered_objects, sorted_objects)
            else:
                print("No valid files found." + radarsite + ' ' + year + '/' + month + '/' + date + ' ' + hour + ':' + minute)
                return
        else:
            print("No objects found in S3 bucket.")
            return

    # Find the index of the nearest file to the datetime
    nearest_index = min(range(len(sorted_objects)), key=lambda i: abs(
        datetime.strptime(sorted_objects[i]['Key'][-19:-4], '%Y%m%d_%H%M%S') - datetime_value))
    # Download the nearest file and the files one before and after it
    for index in range(nearest_index - 1, nearest_index + 2):
        if index >= 0 and index < len(sorted_objects):
            file_key = sorted_objects[index]['Key']
            # Create the local directory path to save the file
            local_directory = os.path.join("radar", radarsite)
            os.makedirs(local_directory, exist_ok=True)
            # Create the local file path
            local_path = os.path.join(local_directory, os.path.basename(file_key))
            if os.path.exists(local_path):
                print(f"File already exists: {os.path.basename(file_key)}")
            else:
                # Download the file from AWS S3
                download_from_s3(file_key, local_path)
            # Add a delay of 1 second between iterations
            #time.sleep(1)

for _, row in df.iterrows():
    datetime_value = row['datetime']
    radarsite = row['description']
    find_nearest_file(df, datetime_value, radarsite)
