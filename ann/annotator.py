# annotator.py
import os
import uuid
import subprocess
import boto3
import json

# Import the ConfigParser
from configparser import ConfigParser

config = ConfigParser()
config.read("/home/ec2-user/mpcs-cc/gas/ann/ann_config.ini")



# Connect to SQS and get the message queue
sqs = boto3.client('sqs')
dynamo = boto3.resource('dynamodb')
# Replace hardcoded value
table = dynamo.Table(config.get('aws', 'DynamoDbTableName'))

# Replace hardcoded value
queue_url = config.get('aws', 'QueueUrl')

#https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html
#https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html
#https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html
#https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
#https://docs.aws.amazon.com/sns/latest/dg/sns-sqs-as-subscriber.html
# Poll the message queue in a loop
while True:
    # Attempt to read a message from the queue (using long polling)
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=['All'],
        MaxNumberOfMessages=1,
        MessageAttributeNames=['All'],
        WaitTimeSeconds=5
    )

    # If a message is read, extract job parameters from the message body
    if 'Messages' in response:
        message = response['Messages'][0]
        receipt_handle = message['ReceiptHandle']
        message_body = json.loads(message['Body'])

        # Extract the actual content from the SNS notification
        sns_message = json.loads(message_body['Message'])

        s3_bucket = sns_message.get('s3_inputs_bucket')
        s3_key = sns_message.get('s3_key_input_file')
        job_id = sns_message.get('job_id')
        email = sns_message.get('email')

        # Check if required keys are present in the message body
        if not s3_bucket or not s3_key or not job_id:
            print("Error: Missing required keys in the message body.")
            continue
        #print(s3_key)

        # Get the input file S3 object and copy it to a local file
        job_folder = os.path.join(os.getcwd(), 'jobs', job_id)
        if not os.path.exists(job_folder):
            os.makedirs(job_folder)

        input_file = os.path.join(job_folder, os.path.basename(s3_key))
        # Replace hardcoded value
        s3 = boto3.client('s3', region_name=config.get('aws', 'AwsRegionName'))
        s3.download_file(s3_bucket, s3_key, input_file)

        # Launch annotation job as a background process
        try:
            script_path = os.path.join(os.getcwd(), 'anntools', 'run.py')
            print(email)
            job = subprocess.Popen(['python', "/home/ec2-user/mpcs-cc/gas/ann/anntools/run.py", input_file, job_id,email])

            table.update_item(
            Key={'job_id': job_id},
            UpdateExpression="SET job_status = :status",
            ConditionExpression="job_status = :pending",
            ExpressionAttributeValues={':status': 'RUNNING', ':pending': 'PENDING'}
            )

            # Delete the message from the queue, if job was successfully submitted
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
        except Exception as e:
            print({str(e)})
    else:
        print("No messages in the queue.")
