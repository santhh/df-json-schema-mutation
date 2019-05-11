# Copyright (C) 2019 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

gcp_credentials = "<your-json-file-name>.json" // your JSON service account

gcp_project = "<your-project-name>" // GCP project ID

gcp_region = "us-central1" // Default -> us-west1

gcp_zone = "us-central1-a" // Default -> us-west1-a

composer_instance_name = "<your-composer-name>" // your composer instance name

pubsub_topic_mutation = "<your-pubsub-topic>" // your pubsub topic name eg: TOPIC_SAMPLE

pubsub_topic_mutation_subscription = "<your-pubsub-subsc>" // your pubsub topic subscription

bucket_dataflow_templates = "<bucket-for-dataflow-templates>" // your bucket name eg: bucket-sample-01 (used to store dataflow templates)

bucket_dataflow_templates_location = "US" // a location for your bucket

bigquery_dataset = "<bq-dataset-name>" // alphanumeric bigquery dataset name

bigquery_dataset_table = "<bq-dataset-table-name>" // alphanumeric bigquery dataset table

bigquery_dataset_table_schema = "BigQuery_DropDeadLetter.json" // bigquery dataset table schema - do not change

dag_name_dataflow = "<dag-name>.py" // the name of the dag file in your dag folder (dataflow_dag.py)

dag_name_bigquery = "<dag-name>.py" // the name of the dag file in your dag folder (bigquery_dag.py)

dataflow_runner_mutation = "DataflowRunner"

java_folder_mutation_code = "df-json-schema-mutation-bq" // the folder name for the mutation pipeline

java_folder_import_code = "df-import-gcs-to-pubsub" // the folder name for the import pipeline

bucket_json_events = "<your-bucket-that-has-the-events>" // the name of the bucket where the JSON events are stored

dataflow_mutation_num_workers = "115"

dataflow_mutation_machine_type = "n1-standard-8"

dataflow_mutation_max_worker = "115"

dataflow_mutation_autoscaling = "NONE"

dataflow_import_num_workers = "1"

dataflow_import_machine_type = "n1-standard-1"

dataflow_import_max_worker = "1"

dataflow_import_autoscaling = "NONE"

bigtable_instance_name = "<bt-name>" // bigtable instance name

# bigtable_instance_type = "DEVELOPMENT" // bigtable instance type: eg: DEVELOPMENT

bigtable_cluster_id = "<bt-cluster-name>" // bigtable cluser name

bigtable_cluster_zone = "us-central1-b" // bigtable cluster zone

bigtable_cluster_strorage = "HDD" // bigtable storage type

bigtable_cluster_num_nodes = "3" // Only in production, the number or nodes

bigtable_table_name = "<bt-table-name>" // bigtable table name

bigtable_column_family_one = "GameEventData"
