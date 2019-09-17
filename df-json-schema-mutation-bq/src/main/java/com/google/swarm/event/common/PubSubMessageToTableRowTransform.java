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
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PubSubMessageToTableRowTransform
    extends PTransform<PCollection<PubsubMessage>, PCollectionTuple> {

  private static final long serialVersionUID = -5720471132168764669L;

  private static final Logger LOG = LoggerFactory.getLogger(PubSubMessageToTableRowTransform.class);

  public static final TupleTag<KV<String, TableRow>> TRANSFORM_OUT =
      new TupleTag<KV<String, TableRow>>() {};
  public static final TupleTag<FailsafeElement<PubsubMessage, KV<String, String>>>
      TRANSFORM_DEADLETTER_OUT =
          new TupleTag<FailsafeElement<PubsubMessage, KV<String, String>>>() {};

  @Override
  public PCollectionTuple expand(PCollection<PubsubMessage> input) {
    PCollectionTuple jsonToTableRowOut =
        input
            .apply("MapToRecord", ParDo.of(new PubsubMessageToFailsafeElementFn()))
            .apply(
                "JsonToTableRow",
                FailsafeJsonToTableRowTransform.<PubsubMessage>newBuilder()
                    .setSuccessTag(TRANSFORM_OUT)
                    .setFailureTag(TRANSFORM_DEADLETTER_OUT)
                    .build());

    return PCollectionTuple.of(TRANSFORM_OUT, jsonToTableRowOut.get(TRANSFORM_OUT))
        .and(TRANSFORM_DEADLETTER_OUT, jsonToTableRowOut.get(TRANSFORM_DEADLETTER_OUT));
  }

  static class PubsubMessageToFailsafeElementFn
      extends DoFn<PubsubMessage, FailsafeElement<PubsubMessage, KV<String, String>>> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      PubsubMessage message = context.element();
      String eventType = message.getAttribute("event_type");
      if (eventType != null) {

        context.output(
            FailsafeElement.of(
                message,
                KV.of(eventType, new String(message.getPayload(), StandardCharsets.UTF_8))));
      } else {
        LOG.error("Message must contain event_type attribute {}", message.toString());
        throw new RuntimeException("Failed to parse message attribute " + message.toString());
      }
    }
  }
}
