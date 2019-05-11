"""
Copyright (C) 2019 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
"""

from __future__ import print_function
import datetime
from airflow import models
from airflow.operators import python_operator

from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator


default_args = {
    'dataflow_default_options': {
        'project': '{{var.value.composer_project}}',
        'zone': '{{var.value.composer_zone}}',
        'tempLocation': '{{var.value.composer_staging}}'
    },
    'start_date': datetime.datetime(2019, 10, 15)
}


with models.DAG('start_dataflow', schedule_interval=None, default_args=default_args, catchup=False) as dag:

    def start_greeting():
        import logging
        logging.info('Hello! Welcome to AirFlow')

    def end_greeting():
        import logging
        logging.info('Thank you, Goodbye!')

    start = python_operator.PythonOperator(task_id='start', python_callable=start_greeting)
    end = python_operator.PythonOperator(task_id='end', python_callable=end_greeting)

    df_pipeline_mutation = DataflowTemplateOperator(
        task_id='df_pipeline_mutation',
        template='{{var.value.gcp_df_mutation_template}}',
        gcp_conn_id='google_cloud_default'
    )

    df_pipeline_import = DataflowTemplateOperator(
        task_id='df_pipeline_import',
        template='{{var.value.gcp_df_import_template}}',
        gcp_conn_id='google_cloud_default'
    )
    start >> df_pipeline_mutation >> df_pipeline_import >> end
