# views.py
#
# Copyright (C) 2011-2020 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import json
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Key
from botocore.client import Config
from botocore.exceptions import ClientError

from flask import (abort, flash, redirect, render_template,
  request, session, url_for)

from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile


"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document.

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""
#https://s3.console.aws.amazon.com/s3/upload/mpcs-cc-students?region=us-east-1&prefix=tianyushi/
#https://docs.aws.amazon.com/AmazonS3/latest/userguide/ShareObjectPreSignedURL.html
#https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
  # Create a session client to the S3 service
  s3 = boto3.client('s3',
    region_name=app.config['AWS_REGION_NAME'],
    config=Config(signature_version='s3v4'))

  bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
  global user_id
  user_id = session['primary_identity']

  # Generate unique ID to be used as S3 key (name)
  key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
    str(uuid.uuid4()) + '~${filename}'

  # Create the redirect URL
  redirect_url = str(request.url) + '/job'

  # Define policy fields/conditions
  encryption = app.config['AWS_S3_ENCRYPTION']
  acl = app.config['AWS_S3_ACL']
  fields = {
    "success_action_redirect": redirect_url,
    "x-amz-server-side-encryption": encryption,
    "acl": acl
  }
  conditions = [
    ["starts-with", "$success_action_redirect", redirect_url],
    {"x-amz-server-side-encryption": encryption},
    {"acl": acl}
  ]

  # Generate the presigned POST call
  try:
    presigned_post = s3.generate_presigned_post(
      Bucket=bucket_name, 
      Key=key_name,
      Fields=fields,
      Conditions=conditions,
      ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  except ClientError as e:
    app.logger.error(f"Unable to generate presigned URL for upload: {e}")
    return abort(500)
    
  # Render the upload form which will parse/submit the presigned POST
  return render_template('annotate.html', s3_post=presigned_post)


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""
#https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html
#https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
#https://docs.aws.amazon.com/sns/latest/api/API_Publish.html

@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():

  
  # Get bucket name, key, and job ID from the S3 redirect URL
  bucket_name = str(request.args.get('bucket'))
  s3_key = str(request.args.get('key'))

  # Extract the job_id from key 
  user_id = s3_key.split('/')[1].split('~')[0]
  job_id = s3_key.split('/')[2].split('~')[0]
  # Extract the input file name from the S3 key
  input_file_name = s3_key.split('/')[-1].split('~')[-1]  
  submit_time = int(time.time())
  profile = get_profile(user_id)
  user_email = profile.email

  data = {
        "job_id": job_id,
        "user_id": user_id,
        "input_file_name": input_file_name,
        "s3_inputs_bucket": bucket_name,
        "s3_key_input_file": s3_key,
        "submit_time": submit_time,
        "job_status": "PENDING"
    }

  # Set up DynamoDB connection
  dynamo = boto3.resource('dynamodb')
  tablename= app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']
  table = dynamo.Table(tablename)
  # Save the data to the database
  response = table.put_item(Item=data)

  current_time = int(time.time() * 1000)
  message_deduplication_id = f"{data['job_id']}_{current_time}"

  session = boto3.Session()
  s3 = session.client('s3', region_name='us-east-1') 
  sns = boto3.client('sns')

  data_with_email = data.copy()  # create a copy of data so we don't modify the original
  data_with_email['email'] = user_email
  sns_response = sns.publish(TopicArn=app.config['AWS_SNS_JOB_REQUEST_TOPIC'], Message=json.dumps(data_with_email), MessageGroupId = 'jobs_status', MessageDeduplicationId=message_deduplication_id)

  return render_template('annotate_confirm.html', job_id=job_id)

#https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html
#AUTH TOOL 
"""List all annotations for the user
"""
@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():
    # Get the user's profile and their unique user_id
    user_id = session['primary_identity']

    # Set up DynamoDB connection
    dynamo = boto3.resource('dynamodb')
    tablename = app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']
    table = dynamo.Table(tablename)

    # Query DynamoDB for jobs submitted by the current user
    response = table.query(
        KeyConditionExpression=Key('user_id').eq(user_id),
        IndexName='user_id-index'
    )

    # Get list of annotations to display
    annotations = response['Items']

    # Convert Unix timestamps to human-readable format
    for annotation in annotations:
        annotation['submit_time'] = datetime.fromtimestamp(int(annotation['submit_time'])).strftime('%Y-%m-%d %H:%M:%S')
        if 'complete_time' in annotation and annotation['complete_time']:
            annotation['complete_time'] = datetime.fromtimestamp(int(annotation['complete_time'])).strftime('%Y-%m-%d %H:%M:%S')
        else:
            annotation['complete_time'] = None

    return render_template('annotations.html', annotations=annotations)


#https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html
#AUTH TOOL 
@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
    # Get current user id
    current_user_id = session.get('primary_identity')

    dynamo = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    profile = get_profile(identity_id=current_user_id)
    user_role = profile.role

    response = table.get_item(
        Key={
            'job_id': id
        }
    )

    # If the job doesn't exist
    if 'Item' not in response:
        return render_template('error.html', error_message='Job not found'), 404

    job = response['Item']

    # Check if the current user is authorized to view the job
    if job['user_id'] != current_user_id:
        return render_template('error.html', error_message='Not authorized to view this job'), 403


    job['submit_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(job['submit_time']))
    if 'complete_time' in job:
        job['complete_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(job['complete_time']))
    

    s3_client = boto3.client('s3', region_name='us-east-1')

    # Check if 's3_key_result_file' exists
    if 's3_key_result_file' in job:
 
        result_file_url = s3_client.generate_presigned_url('get_object', Params={'Bucket': app.config['AWS_S3_RESULTS_BUCKET'], 'Key': job['s3_key_result_file']}, ExpiresIn=3600)
        job['result_file_url'] = result_file_url
    else:
        if user_role == "free_user":
          job['result_file_url'] = url_for('subscribe')
        elif user_role == "premium_user":
          job['restore_message'] = "We are restoring your files, please check back later."

    return render_template('annotation_details.html', annotation=job)



"""Display the log file contents for an annotation job
"""
#https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
#https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html
#AUTH TOOL 
@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
  # Get current user id
  current_user_id = session.get('primary_identity')

  # Set up DynamoDB connection
  dynamo = boto3.resource('dynamodb', region_name='us-east-1')
  table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])

  # Get the job details
  response = table.get_item(
      Key={
          'job_id': id
      }
  )

  # If the job doesn't exist
  if 'Item' not in response:
    return render_template('error.html', error_message='Job not found'), 404

  job = response['Item']

  # Check if the current user is authorized to view the job
  if job['user_id'] != current_user_id:
    return render_template('error.html', error_message='Not authorized to view this job'), 403

  # Fetch log file from S3
  s3 = boto3.client('s3')
  try:
    log_file = s3.get_object(Bucket=app.config['AWS_S3_RESULTS_BUCKET'], Key=job['s3_key_log_file'])
  except ClientError as e:
    app.logger.error(f"Unable to retrieve log file: {e}")
    return abort(500)
  
  log_file_content = log_file['Body'].read().decode('utf-8')

  return render_template('view_log.html', job_id=id, log_file_contents=log_file_content)





#AUTH TOOL 
"""Subscription management handler
"""
#https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html
@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
  if (request.method == 'GET'):
    # Display form to get subscriber credit card info
    if (session.get('role') == "free_user"):
      return render_template('subscribe.html')
    else:
      return redirect(url_for('profile'))

  elif (request.method == 'POST'):
    # Update user role to allow access to paid features
    update_profile(
      identity_id=session['primary_identity'],
      role="premium_user"
    )

    # Update role in the session
    session['role'] = "premium_user"

    # Initialize boto3 client for SQS
    AWS_REGION_NAME = app.config['AWS_REGION_NAME']
    sqs = boto3.client('sqs', region_name=AWS_REGION_NAME)
    queue_url = app.config['AWS_SQS_RESTORE']

    # Prepare message body
    message_body = {
      "user_id": session['primary_identity']
    }

    # Send message to SQS queue
    sqs.send_message(
      QueueUrl=queue_url,
      MessageBody=json.dumps(message_body)
    )

    # Display confirmation page
    return render_template('subscribe_confirm.html')


"""Reset subscription
"""
@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
  # Hacky way to reset the user's role to a free user; simplifies testing
  update_profile(
    identity_id=session['primary_identity'],
    role="free_user"
  )
  return redirect(url_for('profile'))


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""
@app.route('/', methods=['GET'])
def home():
  return render_template('home.html')

"""Login page; send user to Globus Auth
"""
@app.route('/login', methods=['GET'])
def login():
  app.logger.info(f"Login attempted from IP {request.remote_addr}")
  # If user requested a specific page, save it session for redirect after auth
  if (request.args.get('next')):
    session['next'] = request.args.get('next')
  return redirect(url_for('authcallback'))

"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
  return render_template('error.html', 
    title='Page not found', alert_level='warning',
    message="The page you tried to reach does not exist. \
      Please check the URL and try again."
    ), 404

"""403 error handler
"""
@app.errorhandler(403)
def forbidden(e):
  return render_template('error.html',
    title='Not authorized', alert_level='danger',
    message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party."
    ), 403

"""405 error handler
"""
@app.errorhandler(405)
def not_allowed(e):
  return render_template('error.html',
    title='Not allowed', alert_level='warning',
    message="You attempted an operation that's not allowed; \
      get your act together, hacker!"
    ), 405

"""500 error handler
"""
@app.errorhandler(500)
def internal_error(error):
  return render_template('error.html',
    title='Server error', alert_level='danger',
    message="The server encountered an error and could \
      not process your request."
    ), 500

### EOF
