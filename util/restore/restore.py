__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'
import time
import os
import sys
import json
import boto3
from configparser import ConfigParser
from botocore.exceptions import NoCredentialsError, BotoCoreError
from boto3.dynamodb.conditions import Key

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
config = ConfigParser(os.environ)
config.read('restore_config.ini')

# AWS general settings
AWS_REGION_NAME = config.get('aws', 'AwsRegionName')

# GAS settings
DYNAMODB_TABLE_NAME = config.get('gas', 'DynamoDbTableName')
GLACIER_VAULT = config.get('gas','GlacierName')
RESTORE_QUEUE_URL = config.get('gas', 'RestoreQueueUrl')
SNSTOPIC = config.get('gas', 'SNSTopic')

# Initializing Boto3 clients and resources
sqs = boto3.client('sqs', region_name=AWS_REGION_NAME)
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION_NAME)
glacier = boto3.client('glacier', region_name=AWS_REGION_NAME)

#https://docs.aws.amazon.com/amazonglacier/latest/dev/downloading-an-archive-two-steps.html
#https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessage.html
#https://aws.amazon.com/cn/sns/faqs/
#https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/SecondaryIndexes.html
def handle_message(message):
    message_body = json.loads(message['Body'])
    user_id = message_body['user_id']

    # Get records from DynamoDB for the user
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)

    response = table.query(
        IndexName='user_id-index',
        KeyConditionExpression=Key('user_id').eq(user_id)
    )

    for item in response['Items']:
        # Skip if 'archive_status' is not present 
        if 'archive_status' not in item:
            print(f"Skipping job_id: {item['job_id']}. No 'archive_status' present.")
            continue

        # If status is 'archived', initiate restore
        if item['archive_status'] == 'archived':
            print(f"Archive status for job_id: {item['job_id']} is 'archived'. Initiating restore.")
            initiate_restore(item['results_file_archive_id'])

    sqs.delete_message(
        QueueUrl=RESTORE_QUEUE_URL,
        ReceiptHandle=message['ReceiptHandle']
    )



#https://docs.aws.amazon.com/amazonglacier/latest/dev/downloading-an-archive-two-steps.html
def initiate_restore(archive_id):
    try:
        response = glacier.initiate_job(
            vaultName=GLACIER_VAULT,
            jobParameters={
                'Type': 'archive-retrieval',
                'ArchiveId': archive_id,
                'Tier': 'Expedited',
                'SNSTopic': SNSTOPIC
            }
        )
        job_id = response['jobId']
        print(f"Initiated expedited retrieval with job_id: {job_id}")
        
    except glacier.exceptions.InsufficientCapacityException:
        response = glacier.initiate_job(
            vaultName=GLACIER_VAULT,
            jobParameters={
                'Type': 'archive-retrieval',
                'ArchiveId': archive_id,
                'Tier': 'Standard',
                'SNSTopic': SNSTOPIC
            }
        )
        job_id = response['jobId']
        print(f"Insufficient capacity for expedited retrieval. Initiated standard retrieval with job_id: {job_id}")

        
#https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessage.html
def main():
    while True:
        messages = sqs.receive_message(
            QueueUrl=RESTORE_QUEUE_URL,
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
