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

import java.nio.charset.StandardCharsets;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;

@AutoValue
public abstract class WriteStringErrorMessageTransform
		extends PTransform<PCollection<FailsafeElement<String, String>>, WriteResult> {

	private static final Logger LOG = LoggerFactory.getLogger(WriteStringErrorMessageTransform.class);

	private static final long serialVersionUID = 1L;

	public static Builder newBuilder() {
		return new AutoValue_WriteStringErrorMessageTransform.Builder();
	}

	public abstract String getErrorRecordsTable();

	public abstract String getErrorRecordsTableSchema();

	@Override
	public WriteResult expand(PCollection<FailsafeElement<String, String>> failedRecords) {

		return failedRecords.apply("FailedRecordToTableRow", ParDo.of(new FailedStringToTableRowFn())).apply(
				"WriteFailedRecordsToBigQuery",
				BigQueryIO.writeTableRows().to(getErrorRecordsTable()).withJsonSchema(getErrorRecordsTableSchema())
						.withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
						.withWriteDisposition(WriteDisposition.WRITE_APPEND));
	}

	/** Builder for {@link WriteStringMessageErrors}. */
	@AutoValue.Builder
	public abstract static class Builder {
		public abstract Builder setErrorRecordsTable(String errorRecordsTable);

		public abstract Builder setErrorRecordsTableSchema(String errorRecordsTableSchema);

		public abstract WriteStringErrorMessageTransform build();
	}

	public static class FailedStringToTableRowFn extends DoFn<FailsafeElement<String, String>, TableRow> {

		private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormat
				.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

		@ProcessElement
		public void processElement(ProcessContext context) {
			FailsafeElement<String, String> failsafeElement = context.element();
			final String message = failsafeElement.getOriginalPayload();
			String timestamp = TIMESTAMP_FORMATTER.print(context.timestamp().toDateTime(DateTimeZone.UTC));

			final TableRow failedRow = new TableRow().set("timestamp", timestamp)
					.set("errorMessage", failsafeElement.getErrorMessage())
					.set("stacktrace", failsafeElement.getStacktrace());

			if (message != null) {
				failedRow.set("payloadString", message).set("payloadBytes", message.getBytes(StandardCharsets.UTF_8));
			}
			LOG.error("Failed Row {}", failedRow.toString());
			context.output(failedRow);
		}
	}

}
