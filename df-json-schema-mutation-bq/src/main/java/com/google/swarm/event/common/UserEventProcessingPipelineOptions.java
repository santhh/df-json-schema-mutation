/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.swarm.event.common;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.ValueProvider;

public interface UserEventProcessingPipelineOptions extends DataflowPipelineOptions {
  @Description("Table spec to write the output to")
  ValueProvider<String> getDataSetId();

  void setDataSetId(ValueProvider<String> value);

  @Description("Subscribtion to read from")
  ValueProvider<String> getSubTopic();

  void setSubTopic(ValueProvider<String> value);

  @Description(
      "The dead-letter table to output to within BigQuery in <project-id>:<dataset>.<table> "
          + "format. If it doesn't exist, it will be created during pipeline execution.")
  ValueProvider<String> getOutputDeadletterTable();

  void setOutputDeadletterTable(ValueProvider<String> value);

  @Description("The Google Cloud Bigtable instance ID .")
  ValueProvider<String> getBigtableInstanceId();

  void setBigtableInstanceId(ValueProvider<String> value);

  @Description("The Cloud Bigtable table ID in the instance.")
  ValueProvider<String> getBigtableTableId();

  void setBigtableTableId(ValueProvider<String> value);
}
