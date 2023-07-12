#!/bin/bash # https://www.gnu.org/software/bash/manual/bash.html
cd /home/ec2-user/mpcs-cc
source bin/activate
ANNOTATOR_SCRIPT="/home/ec2-user/mpcs-cc/gas/ann/annotator.py"
JOBS_DIRECTORY="/home/ec2-user/mpcs-cc/gas/ann/jobs"

# Create the jobs directory if it doesn't exist
mkdir -p "$JOBS_DIRECTORY"

# Run the annotator.py script and log the output to the annotator_log.txt file
python "$ANNOTATOR_SCRIPT" 
