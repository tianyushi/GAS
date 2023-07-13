# gas-framework
An enhanced web framework (based on [Flask](http://flask.pocoo.org/)) for use in the capstone project. Adds robust user authentication (via [Globus Auth](https://docs.globus.org/api/auth)), modular templates, and some simple styling based on [Bootstrap](http://getbootstrap.com/).

Directory contents are as follows:
* `/web` - The GAS web app files
* `/ann` - Annotator files
* `/util` - Utility scripts for notifications, archival, and restoration
* `/aws` - AWS user data files

1. Archive Process: 
  1. The system will send the message to SNS job_complete when a job is completed and it will distribute the message to the SQS archive. There is 5 min delay in archiving the SQS message receiving to allow the free user to download 
     their result file. 
  2. The archive.py will continuously check 
  for messages in the SQS and process them. The user's role will be determined when processing the message and will archive the file if the user is 
  free. If the user is premium, the message will be deleted without further action.
  Here is the archive status: 
  a. The file will be uploaded to the glacier 
  b. The file will be deleted after the upload is complete 
  c. The database will be updated with the archive ID and archive status and S3 key result file will be removed. 
  d. Delete the message in the archive SQS 
  
 2. Restore Process: 
  1. When a POST request is sent from the /subscribe endpoint, a message will be sent to restore SQS. 
  2.restore.py will continually check for the messages and process them. If the user is premium, a restore process will be started and a message will be sent to restore the SNS subscribed to by the thaw SQS. 
  Here is the restoration process: 
  a. Once the thaw SQS retrieved the message, We will scan the database and find out if all jobs match the current user_id get from the message. 
  b. We will use the archive_id to restore each file associated with the current user and if the archive status is None, which means the file is not archived then we skip this file. The default is expediated and if exception occurs then switch to standard 
  c. An SNS will be provided to the Glacier and once the retrieval is completed, the SNS will notify the thaw SQS. 
  d. The thaw SQS will continuously check the new messages and use archive_id to match the job information in the database. 
  e. After the file is decoded, the thaw.py will restore the file using the S3 key result file get by replacing the extension of the S3 key log file to store the file in S3. 
  f. The database will be updated with the removed S3 key result file and the archive_id and archive_status column will be removed. 
  g. thaw.py will then delete the file in Glacier using the archive id if the restoration is completed. 
  h. Delete the message in thaw SQS
