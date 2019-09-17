
#! /bin/bash
set -x

echo "please to use glocud make sure you completed authentication"
echo "gcloud config set project templates-user"
echo "gcloud auth application-default login"

PROJECT_ID=s3-dlp-experiment
GCS_STAGING_LOCATION=gs://dynamic-template/log
BUCKET_SPEC=gs://dynamic-template
API_ROOT_URL="https://dataflow.googleapis.com"
TEMPLATES_LAUNCH_API="${API_ROOT_URL}/v1b3/projects/${PROJECT_ID}/templates:launch"
JOB_NAME="streaming-benchmark-pipeline-`date +%Y%m%d-%H%M%S-%N`"
PARAMETERS_CONFIG="@streaming_benchmark_config.json"
echo JOB_NAME=$JOB_NAME

curl -X POST -H "Content-Type: application/json" \
 -H "Authorization: Bearer $(gcloud auth print-access-token)" \
 "${TEMPLATES_LAUNCH_API}"`
 `"?validateOnly=false"`
 `"&dynamicTemplate.gcsPath=${BUCKET_SPEC}/dynamic_template_streaming_benchmark.json"`
 `"&dynamicTemplate.stagingLocation=${GCS_STAGING_LOCATION}" \
 -d "${PARAMETERS_CONFIG}"