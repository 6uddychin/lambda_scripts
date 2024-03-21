import csv
import boto3
import os

# Initialize AWS clients
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')

def lambda_handler(event, context):
    # Retrieve the bucket and object key from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    # Download CSV file from S3
    csv_file = download_file_from_s3(bucket, key)
    
    # Process CSV
    process_csv(csv_file)

def download_file_from_s3(bucket, key):
    # Download file from S3
    local_file_path = '/tmp/report.csv'  # Temporary file path
    s3_client.download_file(bucket, key, local_file_path)
    return local_file_path

def process_csv(csv_file):
    error_rows = []

    try:
        with open(csv_file, 'r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                try:
                    if row['status'] == 'offline':
                        # Publish SNS notification
                        sns_client.publish(
                            TopicArn='YOUR_SNS_TOPIC_ARN',
                            Subject='Offline Status Detected',
                            Message='Row with offline status: {}'.format(row)
                        )
                        # Execute run_this.py
                        os.system('python run_this.py')
                    # else: Move to the next row

                except Exception as e:
                    # Log error
                    print(f"Error processing row: {row}, Error: {e}")
                    error_rows.append(row)

    except Exception as e:
        # Log error
        print(f"Error processing CSV file: {e}")

    # Write error rows to a separate CSV file
    if error_rows:
        write_error_report(error_rows)

def write_error_report(error_rows):
    with open('/tmp/error_report.csv', 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=error_rows[0].keys())
        writer.writeheader()
        writer.writerows(error_rows)

    # Upload error report to S3
    s3_client.upload_file('/tmp/error_report.csv', 'YOUR_ERROR_BUCKET', 'error_report.csv')

