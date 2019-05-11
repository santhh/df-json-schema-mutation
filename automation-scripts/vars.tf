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

variable "gcp_credentials" {}

variable "gcp_project" {}

variable "gcp_region" {
  default = "us-west1"
}

variable "gcp_zone" {
  default = "us-west1-a"
}

variable "composer_instance_name" {}

variable "pubsub_topic_mutation" {}

variable "pubsub_topic_mutation_subscription" {}

variable "bucket_dataflow_templates" {}

variable "bucket_dataflow_templates_location" {}

variable "bigquery_dataset" {}

variable "bigquery_dataset_table" {}

variable "bigquery_dataset_table_schema" {}

variable "dag_name_dataflow" {}

variable "dag_name_bigquery" {}

variable "dataflow_runner_mutation" {}

variable "java_folder_mutation_code" {}

variable "java_folder_import_code" {}

variable "bucket_json_events" {}

variable "dataflow_mutation_num_workers" {}

variable "dataflow_mutation_machine_type" {}

variable "dataflow_mutation_max_worker" {}

variable "dataflow_mutation_autoscaling" {}

variable "dataflow_import_num_workers" {}

variable "dataflow_import_machine_type" {}

variable "dataflow_import_max_worker" {}

variable "dataflow_import_autoscaling" {}

variable "bigtable_instance_name" {}

# variable "bigtable_instance_type" {}

variable "bigtable_cluster_id" {}

variable "bigtable_cluster_zone" {}

variable "bigtable_cluster_strorage" {}

variable "bigtable_cluster_num_nodes" {}

variable "bigtable_table_name" {}

variable "bigtable_column_family_one" {}
