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

# Prerequisites: pip install --upgrade google-cloud-bigquery
from __future__ import print_function
from google.cloud import bigquery
import logging
import datetime
from airflow import models
from airflow.operators import python_operator
import pytz
from itertools import islice, chain
from airflow.models import Variable

default_dag_args = {
    'start_date': datetime.datetime(2019, 7, 7),
}


with models.DAG('clean_bigquery', schedule_interval=datetime.timedelta(days=1), default_args=default_dag_args, catchup=False) as dag:

    # Edit the below to match your environment
    BQ_DATASET_NAME = str(Variable.get("composer_dataset"))  # Your BigQuery Dataset name
    WRITE_MODE = "WRITE_APPEND"  # BigQuery write_disposition mode
    BATCH_ROW_SIZE = 7000  # number of rows to insert with the streaming API at once (Max Limit is 10,000)
    DELETE_TABLES = True  # Set as 'False' to keep temporary tables and as 'True' to delete them
    BUFFER_SECONDS = 5400  # 90 minutes x 60 = 5,400 seconds

    def get_time_delta_in_seconds(table_modified_time):
        time_now = datetime.datetime.utcnow()
        timezone = pytz.timezone('UTC')
        time_now_offset_aware = timezone.localize(time_now)
        time_diff = time_now_offset_aware - table_modified_time
        return time_diff.seconds

    def batch(iterable, size):
        sourceiter = iter(iterable)
        while True:
            batchiter = islice(sourceiter, size)
            yield chain([batchiter.next()], batchiter)

    def run_table_consolidation():
        logging.basicConfig(format='[%(asctime)s] %(levelname)s - %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)

        client = bigquery.Client()
        query_tables = client.list_tables(dataset=BQ_DATASET_NAME)

        row_counter = 0
        batch_counter = 0
        for item in query_tables:
            each_temp = item.table_id
            if "_temp" in each_temp:
                logging.info("Processing table [" + each_temp + "]")
                each_original = each_temp.replace("_temp", "")
                try:
                    source_table_ref = client.dataset(BQ_DATASET_NAME).table(each_temp)
                    destination_table_ref = client.dataset(BQ_DATASET_NAME).table(each_original)
                    source_table = client.get_table(source_table_ref)
                    source_rows = client.list_rows(source_table_ref, source_table.schema)
                    detla_in_seconds = get_time_delta_in_seconds(source_table.modified)

                    if detla_in_seconds > BUFFER_SECONDS:
                        for batchiter in batch(source_rows, BATCH_ROW_SIZE):
                            batch_insert = []
                            for each_item_in_batch in batchiter:
                                batch_insert.append(each_item_in_batch)
                                row_counter += 1
                            try:
                                errors = client.insert_rows(destination_table_ref, batch_insert, source_table.schema)
                                batch_counter += 1
                                logging.info("Added Batch " + str(batch_counter) + " (Size:" + str(BATCH_ROW_SIZE)
                                             + ") to BigQuery table [" + str(each_original) + "]")
                                if errors:
                                    logging.info(errors)
                            except ValueError as err:
                                logging.info("--> Error Detail: " + err.message)
                                break
                        if DELETE_TABLES:
                            client.delete_table(source_table_ref)
                            logging.info('***** DELETE: -> Table {}:{} deleted.'.format(BQ_DATASET_NAME, each_temp))
                    else:
                        logging.info("Table [" + each_temp + "] is still in streaming mode and cannot be processed!"
                                     + " *** Seconds elapsed: " + str(detla_in_seconds) + "/" + str(BUFFER_SECONDS)
                                     + " seconds!")
                except Exception as err:
                    logging.info("--> Error Detail: " + err.message)
                    break
        logging.info("Total number of rows processed: " + str(row_counter))

    def sample_function():
        import logging
        logging.info('Hello from the sample function!')

    start_dag = python_operator.PythonOperator(task_id='Pre_Tasks', python_callable=sample_function)
    consolidation = python_operator.PythonOperator(task_id='BQ_Table_Consolidation', python_callable=run_table_consolidation)
    end_dag = python_operator.PythonOperator(task_id='Cleanup_Tasks', python_callable=sample_function)

    start_dag >> consolidation >> end_dag
