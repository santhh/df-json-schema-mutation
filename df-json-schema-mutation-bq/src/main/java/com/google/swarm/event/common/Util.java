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
import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.commons.lang.StringUtils;
import org.joda.time.Instant;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Util {

  private static final Logger LOG = LoggerFactory.getLogger(Util.class);
  private static final String DEADLETTER_SCHEMA_FILE_PATH = "fail_records_table_schema.json";
  private static final String NESTED_SCHEMA_REGEX = ".*[^=]=(.*[^ ]), .*[^=]=(.*[^ ])";
  private static final String SCHEMA_MUTATION_FAILED_MESSAGE =
      "SCHEMA_MUTATION_ERROR.PLEASE CLEAN UP EXISTING TEMP TABLE IF THERE IS ANY";

  public static String getDeadletterTableSchemaJson() {
    String schemaJson = null;
    try {
      schemaJson =
          Resources.toString(
              Resources.getResource(DEADLETTER_SCHEMA_FILE_PATH), StandardCharsets.UTF_8);
    } catch (Exception e) {
      LOG.error(
          "Unable to read {} file from the resources folder!", DEADLETTER_SCHEMA_FILE_PATH, e);
    }

    return schemaJson;
  }

  private static boolean isTimestamp(String value) {
    try {
      Instant.parse(value);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  public static boolean isJSONValid(String test) {
    try {
      new JSONObject(test);
    } catch (JSONException ex) {
      // edited, to include @Arthur's comment
      // e.g. in case JSONArray is valid as well...
      try {
        new JSONArray(test);
      } catch (JSONException ex1) {
        return false;
      }
    }
    return true;
  }

  public static boolean isNumeric(String value) {

    if (StringUtils.isNumeric(value)) {
      return true;
    }
    try {
      Float.parseFloat(value);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  public static String typeCheck(String value) {

    if (value == null || value.isEmpty()) {
      return "String";
    }
    if (isNumeric(value)) {
      return "FLOAT";
    } else if (isTimestamp(value)) {
      return "TIMESTAMP";

    } else if (value.matches(NESTED_SCHEMA_REGEX)) {
      return "RECORD";
    } else {
      return "STRING";
    }
  }

  public static FailsafeElement<String, String> wrapBigQueryInsertError(TableRow failedRow) {

    FailsafeElement<String, String> failsafeElement;
    try {

      failsafeElement = FailsafeElement.of(failedRow.toPrettyString(), failedRow.toPrettyString());
      failsafeElement.setErrorMessage(SCHEMA_MUTATION_FAILED_MESSAGE);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return failsafeElement;
  }
}
