import boto3
import json
from boto3.dynamodb.conditions import Attr
from configparser import ConfigParser
import os 
import sys 

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
config = ConfigParser(os.environ)
config.read('thaw_config.ini')

# AWS Settings
AWS_REGION_NAME = config.get('aws', 'AwsRegionName')
SQS_QUEUE_URL = config.get('gas', 'SQSQueueUrl')  
RESULTS_BUCKET_NAME = config.get('gas', 'ResultsBucket')  
DYNAMODB_TABLE_NAME = config.get('gas', 'DynamoDbTableName')  
GLACIER_VAULT = config.get('gas','GlacierName')

# Initialize Boto3 clients and resources
s3 = boto3.client('s3', region_name=AWS_REGION_NAME)
sqs = boto3.client('sqs', region_name=AWS_REGION_NAME)
glacier = boto3.client('glacier', region_name=AWS_REGION_NAME)
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION_NAME)

#https://docs.aws.amazon.com/amazonglacier/latest/dev/downloading-an-archive-two-steps.html
#https://aws.amazon.com/cn/sns/faqs/
#https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html
#https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
#https://docs.aws.amazon.com/amazonglacier/latest/dev/deleting-an-archive.html
def handle_message(message):
    try:
        message_content = json.loads(message['Body'])  
        message_body = json.loads(message_content['Message'])  

        job_id = message_body['JobId']
        archive_id = message_body['ArchiveId']
        job_status = message_body['StatusCode']
    except Exception as e:
        print(f"Error processing message: {e}")
        return

    if job_status == "Succeeded":
        try:
            output = glacier.get_job_output(vaultName=GLACIER_VAULT, jobId=job_id)
            file_data = output['body'].read()
        except Exception as e:
            print(f"Error retrieving Glacier job output: {e}")
            return

        try:
            table = dynamodb.Table(DYNAMODB_TABLE_NAME)
            response = table.scan(FilterExpression=Attr('results_file_archive_id').eq(archive_id))
        except Exception as e:
            print(f"Error scanning DynamoDB table: {e}")
            return

        if 'Items' in response:
            for item in response['Items']:
                try:
                   
                    user_id = item['user_id']
                    job_id = item['job_id']
                    s3_key_log_file = item['s3_key_log_file']

                    # form the s3_key_result_file name
                    s3_key_result_file = s3_key_log_file.replace('.vcf.count.log', '.annot.vcf')

                    # Puts the file back up to S3 with the s3_key_result_file name
                    s3.put_object(Bucket=RESULTS_BUCKET_NAME, Key=s3_key_result_file, Body=file_data)
                except Exception as e:
                    print(f"Error uploading to S3: {e}")
                    continue

                try:
                    # Updates the corresponding dynamodb record to delete the results_file_archive_id field
                    # and add the s3_key_results_file field.
                    table.update_item(
                        Key={'job_id': job_id},
                        UpdateExpression="SET s3_key_result_file = :val1 REMOVE results_file_archive_id, archive_status",
                        ExpressionAttributeValues={':val1': s3_key_result_file}
                    )
                except Exception as e:
                    print(f"Error updating DynamoDB table: {e}")
                    continue

                try:
                    # Deletes the archive from the Glacier
                    glacier.delete_archive(
                        vaultName=GLACIER_VAULT, 
                        archiveId=archive_id
                    )
                except Exception as e:
                    print(f"Error deleting Glacier archive: {e}")
                    continue

    elif job_status == "Failed":
        print(f"Job {job_id} failed.")

    try:
        # Deletes the message from the queue.
        sqs.delete_message(
            QueueUrl=SQS_QUEUE_URL,
            ReceiptHandle=message['ReceiptHandle']
        )
    except Exception as e:
        print(f"Error deleting message from SQS queue: {e}")

#https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessage.html
def main():
    while True:
        try:
            messages = sqs.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=10,  
                WaitTimeSeconds=5  # Enable long polling
            )
        except Exception as e:
            print(f"Error receiving messages from SQS queue: {e}")
            continue

        if 'Messages' in messages:
            for message in messages['Messages']:
                handle_message(message)

        else:
            print("No messages to process. Sleeping for a moment...")

if __name__ == '__main__':
    main()
