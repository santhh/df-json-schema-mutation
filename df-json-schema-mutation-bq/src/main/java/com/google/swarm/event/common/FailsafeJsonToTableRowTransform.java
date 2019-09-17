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

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class FailsafeJsonToTableRowTransform<T>
    extends PTransform<PCollection<FailsafeElement<T, KV<String, String>>>, PCollectionTuple> {

  private static final long serialVersionUID = 4211900847689851730L;
  private static final Logger LOG = LoggerFactory.getLogger(FailsafeJsonToTableRowTransform.class);

  public abstract TupleTag<KV<String, TableRow>> successTag();

  public abstract TupleTag<FailsafeElement<T, KV<String, String>>> failureTag();

  public static <T> Builder<T> newBuilder() {
    return new AutoValue_FailsafeJsonToTableRowTransform.Builder<>();
  }

  @AutoValue.Builder
  public abstract static class Builder<T> {
    public abstract Builder<T> setSuccessTag(TupleTag<KV<String, TableRow>> successTag);

    public abstract Builder<T> setFailureTag(
        TupleTag<FailsafeElement<T, KV<String, String>>> failureTag);

    public abstract FailsafeJsonToTableRowTransform<T> build();
  }

  @Override
  public PCollectionTuple expand(
      PCollection<FailsafeElement<T, KV<String, String>>> failsafeElements) {
    return failsafeElements.apply(
        "JsonToTableRow",
        ParDo.of(
                new DoFn<FailsafeElement<T, KV<String, String>>, KV<String, TableRow>>() {
                  Gson gson = null;
                  Map<String, Object> map = new HashMap<String, Object>();

                  @Setup
                  public void setup() {
                    gson = new Gson();
                  }

                  @SuppressWarnings("unchecked")
                  @ProcessElement
                  public void processElement(ProcessContext context) {
                    FailsafeElement<T, KV<String, String>> element = context.element();
                    KV<String, String> jsonWithAttribute = element.getPayload();
                    try {
                      String json = jsonWithAttribute.getValue();

                      TableRow row = convertJsonToTableRow(jsonWithAttribute.getValue());
                      List<TableCell> cells = new ArrayList<>();
                      map = new HashMap<String, Object>();
                      map = gson.fromJson(json, map.getClass());

                      map.forEach(
                          (key, value) -> {
                            cells.add(new TableCell().set(key, value));
                          });

                      row.setF(cells);
                      context.output(KV.of(jsonWithAttribute.getKey(), row));
                    } catch (Exception e) {
                      context.output(
                          failureTag(),
                          FailsafeElement.of(element.getOriginalPayload(), jsonWithAttribute)
                              .setErrorMessage(e.getMessage())
                              .setStacktrace(Throwables.getStackTraceAsString(e)));
                    }
                  }
                })
            .withOutputTags(successTag(), TupleTagList.of(failureTag())));
  }

  private static TableRow convertJsonToTableRow(String json) {
    TableRow row;

    try (InputStream inputStream =
        new ByteArrayInputStream(json.trim().getBytes(StandardCharsets.UTF_8))) {

      row = TableRowJsonCoder.of().decode(inputStream, Context.OUTER);

    } catch (IOException e) {
      LOG.error("Can't parse JSON message", e.toString());
      throw new RuntimeException("Failed to serialize json to table row: " + json, e);
    }

    return row;
  }
}
