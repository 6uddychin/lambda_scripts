import boto3
import csv
from io import StringIO

def get_matching_rows(bucket_name_a, key_a, bucket_name_b, key_b):
    # Connect to S3
    s3 = boto3.client('s3')

    # Download CSV files from S3
    obj_a = s3.get_object(Bucket=bucket_name_a, Key=key_a)
    obj_b = s3.get_object(Bucket=bucket_name_b, Key=key_b)

    # Parse CSV files
    csv_data_a = obj_a['Body'].read().decode('utf-8')
    csv_data_b = obj_b['Body'].read().decode('utf-8')

    # Load CSV data into dictionaries
    rows_a = csv.DictReader(StringIO(csv_data_a))
    rows_b = csv.DictReader(StringIO(csv_data_b))

    # Extract unique ids from both files
    unique_ids_b = set(row['unique id'] for row in rows_b)

    # Iterate through rows in report-sf
    matching_rows = []
    for row_a in csv.DictReader(StringIO(csv_data_a)):
        if row_a['unique id'] in unique_ids_b:
            matching_rows.append(row_a)

    return matching_rows

# Function to perform the desired action with matching rows
def do_something(matching_rows):
    # This function can be defined later based on what you want to do with the matching rows
    for row in matching_rows:
        print("Do something with matching row:", row)

# Example usage
bucket_name_a = 's3-bucket-a'
key_a = 'report-sf.csv'
bucket_name_b = 's3-bucket-b'
key_b = 'dashboard report.csv'

matching_rows = get_matching_rows(bucket_name_a, key_a, bucket_name_b, key_b)
do_something(matching_rows)
