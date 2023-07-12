import json
import sys
import time
import os
import shutil
import boto3
import driver
from configparser import ConfigParser

config = ConfigParser()
config.read("/home/ec2-user/mpcs-cc/ann/ann_config.ini")

class Timer(object):
    def __init__(self, verbose=True):
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.secs = self.end - self.start
        if self.verbose:
            print(f"Approximate runtime: {self.secs:.2f} seconds")
            
#https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html
#https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html
#https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
# Replace hardcoded values
s3 = boto3.client('s3', region_name=config.get('aws', 'AwsRegionName'))
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(config.get('aws', 'DynamoDbTableName'))
prefix = config.get('aws', 'Prefix')  # Add this line

def upload_directory_to_s3(bucket, folder_prefix, local_directory):
    keys = []
    for root, dirs, files in os.walk(local_directory):
        for file in files:
            local_path = os.path.join(root, file)
            # get relative path from the job directory
            relative_path = os.path.relpath(local_path, local_directory)
            # join the relative path with the folder_prefix to create the s3_path
            s3_path = os.path.join(folder_prefix, relative_path)
            s3.upload_file(local_path, bucket, s3_path)
            keys.append(s3_path)
    return keys




def cleanup_directory(local_directory):
    if os.path.exists(local_directory):
        shutil.rmtree(local_directory)

def update_dynamodb(job_id, results_file_val, log_file_val):
    current_time = int(time.time())
    results_bucket = config.get('aws', 'ResultsBucket')
    table.update_item(
        Key={'job_id': job_id},
        UpdateExpression="SET s3_results_bucket = :results_bucket, s3_key_result_file = :result_file, s3_key_log_file = :log_file, complete_time = :complete_time, job_status = :status",
        ExpressionAttributeValues={
            ':results_bucket': results_bucket,
            ':result_file': results_file_val,
            ':log_file': log_file_val,
            ':complete_time': current_time,
            ':status': 'COMPLETED'
        }
    )

def send_job_complete_notification(job_id, email):
    sns = boto3.client('sns')
    topic_arn = config.get('aws', 'JobCompleteTopic')
    message = {
        'job_id': job_id,
        'email': email,
        'message': f"Job {job_id} has been completed."
    }
    sns_response = sns.publish(
        TopicArn=topic_arn,
        Message=json.dumps(message)
    )

if __name__ == '__main__':
    if len(sys.argv) > 1:
        with Timer():
            driver.run(sys.argv[1], 'vcf')
        job_id = sys.argv[2]
        email = sys.argv[3]
        file_prefix = os.path.splitext(os.path.basename(sys.argv[1]))[0]     
        results_bucket = config.get('aws', 'ResultsBucket')
        username = config.get('aws','Prefix')
        folder_prefix = f'{username}/{job_id}'
        results_file_val = f'{folder_prefix}/{file_prefix}.annot.vcf'
        log_file_val = f'{folder_prefix}/{file_prefix}.vcf.count.log'


        upload_directory_to_s3(results_bucket, folder_prefix, os.path.join('jobs', job_id))
        update_dynamodb(job_id, results_file_val, log_file_val)
        send_job_complete_notification(job_id,email)

        # Replace 'jobs' with the path to the specific directory for the completed job
        job_directory = f'jobs/{job_id}'  
        cleanup_directory(job_directory)

    else:
        print("A valid .vcf file must be provided as input to this program.")
