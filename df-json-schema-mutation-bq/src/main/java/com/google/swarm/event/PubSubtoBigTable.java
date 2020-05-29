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
package com.google.swarm.event;

import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableScanConfiguration;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubtoBigTable {
  private static final Logger LOG = LoggerFactory.getLogger(PubSubtoBigTable.class);
  private static final String TEMP_TABLE_PREFIX = "_temp";
  private static final byte[] FAMILY = Bytes.toBytes("CustomerCf");
  private static final byte[] COLUMN_NAME = Bytes.toBytes("MobileData");

  public static void main(String[] args) {
    PubSubtoBigTableOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubtoBigTableOptions.class);
    run(options);
  }

  @SuppressWarnings("serial")
  public static PipelineResult run(PubSubtoBigTableOptions options) {

    Pipeline p = Pipeline.create(options);

    // Create BigTable Configuration
    CloudBigtableScanConfiguration bigTableConfig =
        new CloudBigtableScanConfiguration.Builder()
            .withProjectId(options.getProject().toString())
            .withInstanceId(options.getBigtableInstanceId().toString())
            .withTableId(options.getBigtableTableId().toString())
            .build();

    // Read PubSub Message
    PCollection<PubsubMessage> jsonEvent =
        p.apply(
            "ReadPubsubMessages",
            PubsubIO.readMessagesWithAttributes().fromSubscription(options.getSubTopic()));
    // PubSub to BigTable
    jsonEvent
        .apply(
            "ConvertMessageToMutation",
            ParDo.of(
                new DoFn<PubsubMessage, Mutation>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) throws Exception {
                    try {
                      PubsubMessage pubSubEvent = c.element();
                      String userequipmentImeisv = pubSubEvent.getAttribute("userequipment_imeisv");
                      String subscriberMsisdn = pubSubEvent.getAttribute("subscriber_msisdn");
                      String rowKey =
                          userequipmentImeisv
                              + '#'
                              + subscriberMsisdn
                              + '#'
                              + String.valueOf(System.currentTimeMillis());
                      Put row = new Put(Bytes.toBytes(rowKey));
                      row.addColumn(FAMILY, COLUMN_NAME, pubSubEvent.getPayload());
                      c.output(row);
                    } catch (Exception e) {
                      LOG.error("Failed to process input {}", c.element(), e);
                      throw e;
                    }
                  }
                }))
        .apply("WriteToBigTable", CloudBigtableIO.writeToTable(bigTableConfig));

    return p.run();
  }
}
