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

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class WritePubSubErrorMessageTransform
    extends PTransform<
        PCollection<FailsafeElement<PubsubMessage, KV<String, String>>>, WriteResult> {

  private static final long serialVersionUID = -578821064047236156L;
  private static final Logger LOG = LoggerFactory.getLogger(WritePubSubErrorMessageTransform.class);

  public abstract String getErrorRecordsTable();

  public abstract String getErrorRecordsTableSchema();

  public static Builder newBuilder() {

    return new AutoValue_WritePubSubErrorMessageTransform.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setErrorRecordsTable(String failRecordsTableSpec);

    public abstract Builder setErrorRecordsTableSchema(String errorRecordsTableSchema);

    public abstract WritePubSubErrorMessageTransform build();
  }

  @Override
  public WriteResult expand(
      PCollection<FailsafeElement<PubsubMessage, KV<String, String>>> failedRecords) {
    LOG.info("table name {}", getErrorRecordsTable());

    return failedRecords
        .apply("FailedRecordToTableRow", ParDo.of(new FailedPubsubMessageToTableRowFn()))
        .apply(
            "WriteFailedRecordsToBigQuery",
            BigQueryIO.writeTableRows()
                .to(getErrorRecordsTable())
                .withJsonSchema(getErrorRecordsTableSchema())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND));
  }

  public static class FailedPubsubMessageToTableRowFn
      extends DoFn<FailsafeElement<PubsubMessage, KV<String, String>>, TableRow> {

    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
        DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    @ProcessElement
    public void processElement(ProcessContext context) {
      FailsafeElement<PubsubMessage, KV<String, String>> failsafeElement = context.element();
      final PubsubMessage message = failsafeElement.getOriginalPayload();
      String timestamp =
          TIMESTAMP_FORMATTER.print(context.timestamp().toDateTime(DateTimeZone.UTC));

      // Build the table row
      final TableRow failedRow =
          new TableRow()
              .set("timestamp", timestamp)
              .set("attributes", attributeMapToTableRows(message.getAttributeMap()))
              .set("errorMessage", failsafeElement.getErrorMessage())
              .set("stacktrace", failsafeElement.getStacktrace());

      if (message.getPayload() != null) {
        failedRow
            .set("payloadString", new String(message.getPayload()))
            .set("payloadBytes", message.getPayload());
      }

      context.output(failedRow);
    }
  }

  private static List<TableRow> attributeMapToTableRows(Map<String, String> attributeMap) {
    final List<TableRow> attributeTableRows = Lists.newArrayList();
    if (attributeMap != null) {
      attributeMap.forEach(
          (key, value) ->
              attributeTableRows.add(new TableRow().set("key", key).set("value", value)));
    }

    return attributeTableRows;
  }
}
