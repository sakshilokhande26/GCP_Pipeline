# Check your current project
gcloud config get-value project

# Output should be:
# student-data-pipeline

# If not, set it:
gcloud config set project student-data-pipeline

# List your buckets to verify they exist
gsutil ls

# Output should be:
# gs://student-pipeline-raw-incoming/
# gs://student-pipeline-staging-clean/
# gs://student-pipeline-archive/


# Create a folder for your project
mkdir data-pipeline
cd data-pipeline

# Verify you're in the folder
pwd

# Output: /home/your-username/data-pipeline

# Make sure you're in the data-pipeline directory
cd ~/data-pipeline

# Create the cloud function folder
mkdir cloud-function
cd cloud-function

# Verify location
pwd
# Output: /home/your-username/data-pipeline/cloud-function


# Open the Cloud Shell Editor
# Click "Open Editor" button at the top of Cloud Shell
# OR use the nano text editor:

nano main.py

#create requirements.txt

nano requirements.txt

# Make sure you're in the cloud-function directory
cd ~/data-pipeline/cloud-function

# Deploy the function
gcloud functions deploy student-data-pipeline \
    --gen2 \
    --runtime=python311 \
    --region=us-central1 \
    --source=. \
    --entry-point=gcs_trigger \
    --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
    --trigger-event-filters="bucket=student-pipeline-raw-incoming" \
    --memory=1GB \
    --timeout=540s
	
	
# Grant necessary permissions
gcloud projects add-iam-policy-binding student-data-pipeline \
    --member="serviceAccount:$(gcloud config get-value project)@appspot.gserviceaccount.com" \
    --role="roles/eventarc.eventReceiver"
	
gcloud projects add-iam-policy-binding precise-plane-482205-p8 \
    --member="serviceAccount:service-371034145072@gs-project-accounts.iam.gserviceaccount.com" \
    --role="roles/pubsub.publisher"

# Try deploying again