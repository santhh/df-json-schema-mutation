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
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableScanConfiguration;
import com.google.swarm.event.common.BQDestination;
import com.google.swarm.event.common.FailsafeElement;
import com.google.swarm.event.common.FailsafeElementCoder;
import com.google.swarm.event.common.PubSubMessageToTableRowTransform;
import com.google.swarm.event.common.TableRowSchemaMutator;
import com.google.swarm.event.common.UserEventProcessingPipelineOptions;
import com.google.swarm.event.common.Util;
import com.google.swarm.event.common.WritePubSubErrorMessageTransform;
import com.google.swarm.event.common.WriteStringErrorMessageTransform;
public class UserEventProcessingPipeline {
	private static final Logger LOG = LoggerFactory.getLogger(UserEventProcessingPipeline.class);
	private static final Duration WINDOW_INTERVAL = Duration.standardSeconds(10);
	private static final String TEMP_TABLE_PREFIX = "_temp";
	private static final byte[] FAMILY = Bytes.toBytes("GameEventData");
	private static final byte[] COLUMN_NAME = Bytes.toBytes("PlayerEvents");
	private static final FailsafeElementCoder<String, String> FAILSAFE_ELEMENT_CODER = FailsafeElementCoder
			.of(StringUtf8Coder.of(), StringUtf8Coder.of());
	public static void main(String[] args) {
		UserEventProcessingPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(UserEventProcessingPipelineOptions.class);
		run(options);
	}
	@SuppressWarnings("serial")
	public static PipelineResult run(UserEventProcessingPipelineOptions options) {
		
		
		Pipeline p = Pipeline.create(options);
		
		CloudBigtableScanConfiguration bigTableConfig =
				new CloudBigtableScanConfiguration.Builder()
						.withProjectId(options.getProject().toString())
						.withInstanceId(options.getBigtableInstanceId().toString())
						.withTableId(options.getBigtableTableId().toString())
						.build();
		FailsafeElementCoder<PubsubMessage, KV<String, String>> pubsubMessageErrorCoder = FailsafeElementCoder
				.of(PubsubMessageWithAttributesCoder.of(), KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
		FailsafeElementCoder<String, String> bqFailedInsertRowErrorCoder = FailsafeElementCoder.of(StringUtf8Coder.of(),
				StringUtf8Coder.of());
		CoderRegistry coderRegistry = p.getCoderRegistry();
		coderRegistry.registerCoderForType(pubsubMessageErrorCoder.getEncodedTypeDescriptor(), pubsubMessageErrorCoder);
		coderRegistry.registerCoderForType(bqFailedInsertRowErrorCoder.getEncodedTypeDescriptor(),
				bqFailedInsertRowErrorCoder);
		
		PCollection<PubsubMessage> jsonEvent = p
				.apply("ReadPubsubMessages",
						PubsubIO.readMessagesWithAttributes().fromSubscription(options.getSubTopic()));
		//PubSub to BigTable
		jsonEvent.apply("ConvertMessageToMutation", ParDo.of(new DoFn<PubsubMessage, Mutation>() {
			@ProcessElement
			public void processElement(ProcessContext c) throws Exception {
				try {
					PubsubMessage pubSubEvent = c.element();
					String eventType = pubSubEvent.getAttribute("event_type");
					String rowKey = eventType + '#' + String.valueOf(System.currentTimeMillis());
					Put row = new Put(Bytes.toBytes(rowKey));
					row.addColumn(FAMILY, COLUMN_NAME, pubSubEvent.getPayload());
					c.output(row);
				} catch (Exception e) {
					LOG.error("Failed to process input {}", c.element(), e);
					throw e;
				}
			}
		})).apply("WriteToBigTable", CloudBigtableIO.writeToTable(bigTableConfig));
		
		PCollectionTuple transformOut = jsonEvent
				.apply("ConvertMessageToTableRow", new PubSubMessageToTableRowTransform());
		
		WriteResult writeResult = transformOut.get(PubSubMessageToTableRowTransform.TRANSFORM_OUT).apply("Events Write",
				BigQueryIO.<KV<String, TableRow>>write()
						.to(new BQDestination(options.getDataSetId(), options.getProject()))
						.withFormatFunction(element -> {
							LOG.debug("BQ Row {}", element.getValue().getF());
							return element.getValue();
						}).withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
						.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED).withoutValidation()
						.withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors()));
		// failed row processing
		PCollection<KV<String, TableRow>> failedRows = writeResult.getFailedInserts()
				.apply("Add Temp Table Id As Key",
						WithKeys.of(row -> String.format("%s%s", row.get("EventName").toString(), TEMP_TABLE_PREFIX)))
				.setCoder(KvCoder.of(StringUtf8Coder.of(), TableRowJsonCoder.of()));
		WriteResult bqSchemaMutationWriteErrors = failedRows.apply("Mutation Event Write to Temp Table",
				BigQueryIO.<KV<String, TableRow>>write()
						.to(new BQDestination(options.getDataSetId(), options.getProject()))
						.withFormatFunction(element -> {
							return element.getValue();
						}).withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND).withoutValidation()
						.withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
						.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
		PCollection<FailsafeElement<String, String>> faileSafeWrappedMutationErrors = bqSchemaMutationWriteErrors
				.getFailedInserts().apply("WrapInsertionErrors", MapElements
						.into(FAILSAFE_ELEMENT_CODER.getEncodedTypeDescriptor()).via(Util::wrapBigQueryInsertError));
		writeResult.getFailedInserts()
				.apply("Add Event Type As Key", WithKeys.of(row -> row.get("EventName").toString()))
				.setCoder(KvCoder.of(StringUtf8Coder.of(), TableRowJsonCoder.of()))
				.apply("Fixed Window",
						Window.<KV<String, TableRow>>into(FixedWindows.of(WINDOW_INTERVAL))
								.triggering(AfterWatermark.pastEndOfWindow()
										.withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
												.plusDelayOf(Duration.standardSeconds(1)))
										.withLateFirings(AfterPane.elementCountAtLeast(1)))
								.discardingFiredPanes().withAllowedLateness(Duration.ZERO))
				.apply("Group By Event Type", GroupByKey.create()).apply("Update Table Schema",
						ParDo.of(new TableRowSchemaMutator(options.getProject(), options.getDataSetId())));
		transformOut.get(PubSubMessageToTableRowTransform.TRANSFORM_DEADLETTER_OUT)
				.apply("Write JSON Error Message",WritePubSubErrorMessageTransform.newBuilder()
						.setErrorRecordsTable(String.format("%s:%s.%s", options.getProject(), options.getDataSetId(),
								options.getOutputDeadletterTable()))
						.setErrorRecordsTableSchema(Util.getDeadletterTableSchemaJson()).build());
		faileSafeWrappedMutationErrors.apply("Write Failed Inserts",
				WriteStringErrorMessageTransform.newBuilder()
						.setErrorRecordsTable(String.format("%s:%s.%s", options.getProject(), options.getDataSetId(),
								options.getOutputDeadletterTable()))
						.setErrorRecordsTableSchema(Util.getDeadletterTableSchemaJson()).build());
		return p.run();

	}
}