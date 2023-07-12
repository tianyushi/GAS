__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
import boto3
import json
import time
from configparser import ConfigParser
from botocore.exceptions import NoCredentialsError, BotoCoreError

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
config = ConfigParser(os.environ)
config.read('archive_config.ini')

# AWS general settings
AWS_REGION_NAME = config.get('aws', 'AwsRegionName')

# GAS settings
RESULTS_QUEUE_URL = config.get('gas', 'ResultsQueueUrl')
DYNAMODB_TABLE_NAME = config.get('gas', 'DynamoDbTableName')
RESULTS_BUCKET = config.get('gas', 'ResultsBucket')
JOB_COMPLETE_TOPIC = config.get('gas', 'JobCompleteTopic')
ARCHIVE_QUEUE_URL = config.get('gas', 'ArchiveQueueUrl')
PREFIX = config.get('gas', 'Prefix')
vault = config.get('gas','GlacierName')
AccountDatabase = config.get('gas','AccountDatabase')


# Initializing Boto3 clients and resources
sqs = boto3.client('sqs', region_name=AWS_REGION_NAME)
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION_NAME)
s3 = boto3.client('s3', region_name=AWS_REGION_NAME)
glacier = boto3.client('glacier', region_name=AWS_REGION_NAME)


#https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html
#https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html
#https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html
#https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
#https://docs.aws.amazon.com/sns/latest/dg/sns-sqs-as-subscriber.html
def handle_message(message):
    sns_message = json.loads(message['Body'])
    message_body = json.loads(sns_message['Message'])

    job_id = message_body['job_id']
    
    # Get user_id from DynamoDB using job_id
    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION_NAME)
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    response = table.get_item(
        Key={
            'job_id': job_id
        }
    )
    user_id = response['Item']['user_id']
    
    user_profile = helpers.get_user_profile(id=user_id, db_name=AccountDatabase)
    user_type = user_profile[4]  # Get user_type from the user_profile list

    if user_type != 'premium_user':
        archive_results_file(message)

    sqs = boto3.client('sqs', region_name=AWS_REGION_NAME)
    sqs.delete_message(
        QueueUrl=ARCHIVE_QUEUE_URL,
        ReceiptHandle=message['ReceiptHandle']
    )



#https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html
#https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html
#https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html
#https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
#https://docs.aws.amazon.com/sns/latest/dg/sns-sqs-as-subscriber.html
#https://docs.aws.amazon.com/amazonglacier/latest/dev/uploading-an-archive.html
#https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeletingObjects.html
def archive_results_file(message):

    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION_NAME)
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    sns_message = json.loads(message['Body'])
    message_body = json.loads(sns_message['Message'])

    job_id = message_body['job_id']


    # Get s3_key_result_file from DynamoDB using job_id
    response = table.get_item(
        Key={
            'job_id': job_id
        }
    )
    s3_key_result_file = response['Item']['s3_key_result_file']

    s3 = boto3.client('s3', region_name=AWS_REGION_NAME)
    try:
        s3_response = s3.get_object(Bucket=RESULTS_BUCKET, Key=s3_key_result_file)
    except Exception as e:
        print(f"Error getting file from S3: {e}")
        return

    results_file = s3_response['Body'].read()

    glacier = boto3.client('glacier', region_name=AWS_REGION_NAME)
    try:
        archive_response = glacier.upload_archive(vaultName=vault, body=results_file)
    except Exception as e:
        print(f"Error uploading file to Glacier: {e}")
        return

    results_file_archive_id = archive_response['archiveId']

    try:
        table.update_item(
            Key={
                'job_id': job_id
            },
            UpdateExpression='SET results_file_archive_id = :val1, archive_status = :val2 REMOVE s3_key_result_file',
            ExpressionAttributeValues={
                ':val1': results_file_archive_id,
                ':val2': "archived"
            }
        )
    except Exception as e:
        print(f"Error updating DynamoDB item: {e}")
        return

    try:
        s3.delete_object(Bucket=RESULTS_BUCKET, Key=s3_key_result_file)
    except Exception as e:
        print(f"Error deleting file from S3: {e}")
        return


#https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html
def main():
    sqs = boto3.client('sqs', region_name=AWS_REGION_NAME)
    
    while True:
        messages = sqs.receive_message(
            QueueUrl=ARCHIVE_QUEUE_URL,
            MaxNumberOfMessages=10,  
            WaitTimeSeconds=5  # enable long polling
        )

        if 'Messages' in messages:
            for message in messages['Messages']:
                handle_message(message)
        else:
            print("No messages to process. Sleeping for a moment...")
            

if __name__ == '__main__':
    main()



