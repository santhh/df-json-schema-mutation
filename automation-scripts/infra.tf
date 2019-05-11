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

resource "google_composer_environment" "composer_phoenix_labs" {
  name   = "${var.composer_instance_name}"
  region = "${var.gcp_region}"

  config {
    node_count = 3
  }
}

output "Composer_URI:" {
  value = "${lookup("${google_composer_environment.composer_phoenix_labs.config[0]}", "airflow_uri")}"
}

resource "google_storage_bucket_object" "copy_dataflow_dag_to_composer" {
  name   = "dags/${var.dag_name_dataflow}"
  source = "./dags/${var.dag_name_dataflow}"

  bucket = "${trimspace("${replace("${replace("${lookup("${google_composer_environment.composer_phoenix_labs.config[0]}", "dag_gcs_prefix")}", "gs://", "")}", "/dags", "")}")}"
}

resource "google_storage_bucket_object" "copy_bq_consolidation_dag_to_composer" {
  name   = "dags/${var.dag_name_bigquery}"
  source = "./dags/${var.dag_name_bigquery}"

  bucket = "${trimspace("${replace("${replace("${lookup("${google_composer_environment.composer_phoenix_labs.config[0]}", "dag_gcs_prefix")}", "gs://", "")}", "/dags", "")}")}"
}

resource "google_storage_bucket" "bucket_dataflow_templates" {
  name          = "${var.bucket_dataflow_templates}"
  location      = "${var.bucket_dataflow_templates_location}"
  force_destroy = "true"
}

resource "google_bigquery_dataset" "bq_dataset" {
  dataset_id = "${var.bigquery_dataset}"
}

resource "google_bigquery_table" "dead_letter" {
  dataset_id = "${google_bigquery_dataset.bq_dataset.dataset_id}"
  table_id   = "${var.bigquery_dataset_table}"
  schema     = "${file("${var.bigquery_dataset_table_schema}")}"
}

resource "google_pubsub_topic" "pubsub_topic_mutation" {
  name = "${var.pubsub_topic_mutation}"
}

resource "google_pubsub_subscription" "pubsub_topic_mutation_subscription" {
  name  = "${var.pubsub_topic_mutation_subscription}"
  topic = "${google_pubsub_topic.pubsub_topic_mutation.name}"
}

resource "null_resource" "compile_java_code_mutation" {
  provisioner "local-exec" {
    command = "cd ../${var.java_folder_mutation_code}; gradle run -DmainClass=com.google.swarm.event.UserEventProcessingPipeline -Pargs=' --streaming --project=${var.gcp_project} --runner=${var.dataflow_runner_mutation} --tempLocation=gs://${var.bucket_dataflow_templates}/temp_mutation --templateLocation=gs://${var.bucket_dataflow_templates}/template_mutation --dataSetId=${var.bigquery_dataset} --subTopic=projects/${var.gcp_project}/subscriptions/${google_pubsub_subscription.pubsub_topic_mutation_subscription.name} --outputDeadletterTable=${var.bigquery_dataset_table} --numWorkers=${var.dataflow_mutation_num_workers} --workerMachineType=${var.dataflow_mutation_machine_type} --maxNumWorkers=${var.dataflow_mutation_max_worker} --autoscalingAlgorithm=${var.dataflow_mutation_autoscaling} --experiments=shuffle_mode=service --bigtableInstanceId=${var.bigtable_instance_name} --bigtableTableId=${var.bigtable_table_name}'"
  }

  depends_on = ["google_storage_bucket.bucket_dataflow_templates", "google_bigquery_table.dead_letter", "google_pubsub_subscription.pubsub_topic_mutation_subscription"]
}

resource "null_resource" "compile_java_code_import" {
  provisioner "local-exec" {
    command = "cd ../${var.java_folder_import_code}; gradle run -Pargs=' --streaming --project=${var.gcp_project} --runner=${var.dataflow_runner_mutation} --inputFile=gs://${var.bucket_json_events}/*.json --topic=projects/${var.gcp_project}/topics/${google_pubsub_topic.pubsub_topic_mutation.name} --tempLocation=gs://${var.bucket_dataflow_templates}/temp_import --templateLocation=gs://${var.bucket_dataflow_templates}/template_import --numWorkers=${var.dataflow_import_num_workers} --workerMachineType=${var.dataflow_import_machine_type} --maxNumWorkers=${var.dataflow_import_max_worker} --autoscalingAlgorithm=${var.dataflow_import_autoscaling} --experiments=shuffle_mode=service'"
  }

  depends_on = ["google_storage_bucket.bucket_dataflow_templates", "google_pubsub_topic.pubsub_topic_mutation"]
}

resource "null_resource" "export_composer_variables" {
  provisioner "local-exec" {
    command = "gcloud composer environments run ${var.composer_instance_name} --location ${var.gcp_region} variables -- --set gcp_df_mutation_template gs://${var.bucket_dataflow_templates}/template_mutation; gcloud composer environments run ${var.composer_instance_name} --location ${var.gcp_region} variables -- --set gcp_df_import_template gs://${var.bucket_dataflow_templates}/template_import; gcloud composer environments run ${var.composer_instance_name} --location ${var.gcp_region} variables -- --set composer_zone ${var.gcp_zone}; gcloud composer environments run ${var.composer_instance_name} --location ${var.gcp_region} variables -- --set composer_project ${var.gcp_project}; gcloud composer environments run ${var.composer_instance_name} --location ${var.gcp_region} variables -- --set composer_staging gs://${var.bucket_dataflow_templates}/staging; gcloud composer environments run ${var.composer_instance_name} --location ${var.gcp_region} variables -- --set composer_dataset ${var.bigquery_dataset}"
  }

  depends_on = ["null_resource.compile_java_code_mutation", "null_resource.compile_java_code_import", "google_composer_environment.composer_phoenix_labs"]
}

resource "google_bigtable_instance" "bigtable_instance_01" {
  name = "${var.bigtable_instance_name}"

  # instance_type = "${var.bigtable_instance_type}"

  cluster {
    cluster_id   = "${var.bigtable_cluster_id}"
    zone         = "${var.bigtable_cluster_zone}"
    num_nodes    = "${var.bigtable_cluster_num_nodes}"
    storage_type = "${var.bigtable_cluster_strorage}"
  }
}

resource "google_bigtable_table" "bigtable_table" {
  name          = "${var.bigtable_table_name}"
  instance_name = "${google_bigtable_instance.bigtable_instance_01.name}"

  column_family {
    family = "${var.bigtable_column_family_one}"
  }
}
