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
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class TableRowSchemaMutator extends DoFn<KV<String, Iterable<TableRow>>, String> {
  private static final Logger LOG = LoggerFactory.getLogger(TableRowSchemaMutator.class);
  private transient BigQuery bigQuery;
  private String projectId;
  private ValueProvider<String> datasetId;

  public TableRowSchemaMutator(String projectId, ValueProvider<String> datasetId) {
    this.projectId = projectId;
    this.datasetId = datasetId;
  }

  @Setup
  public void setup() throws IOException {
    if (bigQuery == null) {
      bigQuery =
          BigQueryOptions.newBuilder()
              .setCredentials(GoogleCredentials.getApplicationDefault())
              .build()
              .getService();
    }
  }

  @ProcessElement
  public void processElement(ProcessContext c) {

    if (c.element().getValue().iterator().hasNext()) {
      TableRow mutatedRow = c.element().getValue().iterator().next();
      String eventType = c.element().getKey();
      Set<Field> additionalFields = Sets.newHashSet();
      TableId tableId = TableId.of(projectId, datasetId.get(), eventType);
      Table table = bigQuery.getTable(tableId);
      if (table.getTableId() != null) {
        TableDefinition tableDef = table.getDefinition();
        Schema schema = tableDef.getSchema();
        additionalFields = getAdditionalFields(schema, mutatedRow);
        if (additionalFields.size() > 0) {
          schema = addFieldsToSchema(schema, additionalFields);
          table =
              table
                  .toBuilder()
                  .setDefinition(tableDef.toBuilder().setSchema(schema).build())
                  .build()
                  .update();
          LOG.info(
              "Mutated Schema Updated Successfully for event type {}, Table {}, Number of Fields {}",
              eventType,
              table.getTableId().toString(),
              table.getDefinition().getSchema().getFields().size());
          c.output(table.getTableId().toString());
        } else {
          LOG.info(
              "No changes detect in {} schema for table {}",
              eventType,
              table.getTableId().toString());
        }

      } else {
        LOG.error("table is null {}", table);
        throw new RuntimeException("Table id is null " + table);
      }
    }
  }

  private Set<Field> getAdditionalFields(Schema schema, TableRow mutationRow) {
    FieldList fieldList = schema.getFields();
    Set<Field> additionalFields = Sets.newHashSet();

    List<TableCell> cells = mutationRow.getF();
    for (int i = 0; i < cells.size(); i++) {
      Map<String, Object> object = cells.get(i);
      String header = object.keySet().iterator().next();
      Object value = object.get(header);
      String type = Util.typeCheck(value.toString());
      LOG.debug("header  {}, value {}, type {}", header, value, type);
      if (!type.equals("RECORD")) {
        if (!isExistingField(fieldList, header)) {
          additionalFields.add(
              Field.of(header, LegacySQLTypeName.valueOf(type))
                  .toBuilder()
                  .setMode(Mode.NULLABLE)
                  .build());
        }

      } else {
        // check if it's a new record field
        String[] records = value.toString().split(",");
        if (!isExistingField(fieldList, header)) {
          LOG.debug("Record Type Handler for New record {}", header);
          List<Field> nestedFields = new ArrayList<>();
          for (int j = 0; j < records.length; j++) {
            String[] element = records[j].substring(1).split("=");
            String elementValue = element[1].substring(0, element[1].length() - 1);
            String elementType = Util.typeCheck(elementValue.trim());
            LOG.debug(
                "element header {} , element type {}, element Value {}",
                element[0],
                elementType,
                elementValue);
            nestedFields.add(
                Field.of(element[0], LegacySQLTypeName.valueOf(elementType))
                    .toBuilder()
                    .setMode(Mode.NULLABLE)
                    .build());
          }
          additionalFields.add(
              Field.of(header, LegacySQLTypeName.valueOf(type), FieldList.of(nestedFields))
                  .toBuilder()
                  .setMode(Mode.NULLABLE)
                  .build());
        } else {

          // existing record field
          FieldList existingNestedFields = fieldList.get(header).getSubFields();
          Set<Field> additionalNestedFields = Sets.newHashSet();
          LOG.debug(
              "Record Type Handler for existing record type {} , {}",
              header,
              existingNestedFields.size());
          for (int j = 0; j < records.length; j++) {
            String[] element = records[j].substring(1).split("=");
            if (!isExistingField(existingNestedFields, element[0])) {
              LOG.debug("Record Type Handler new fields found {}", header);
              String elementValue = element[1].substring(0, element[1].length() - 1);
              String elementType = Util.typeCheck(elementValue.trim());
              additionalNestedFields.add(
                  Field.of(element[0], LegacySQLTypeName.valueOf(elementType))
                      .toBuilder()
                      .setMode(Mode.NULLABLE)
                      .build());
            }
          }

          if (!additionalNestedFields.isEmpty()) {

            LOG.debug("Adding additional field for existing record type {}", header);
            additionalNestedFields.addAll(existingNestedFields);
            additionalFields.add(
                Field.of(
                        header,
                        LegacySQLTypeName.valueOf(type),
                        FieldList.of(additionalNestedFields))
                    .toBuilder()
                    .setMode(Mode.NULLABLE)
                    .build());
          }
        }
      }
    }

    return additionalFields;
  }

  private boolean isExistingField(FieldList fieldList, String fieldName) {
    try {
      fieldList.get(fieldName);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  private Schema addFieldsToSchema(Schema schema, Set<Field> additionalFields) {
    List<Field> newFieldList = Lists.newArrayList();
    FieldList currentFieldList = schema.getFields();

    Set<String> fieldNames =
        additionalFields.stream().map(Field::getName).collect(Collectors.toSet());

    List<Field> excludeExistingRecordList =
        currentFieldList.stream()
            .filter(f -> !fieldNames.contains(f.getName()))
            .collect(Collectors.toList());

    if (excludeExistingRecordList != null) {
      newFieldList.addAll(excludeExistingRecordList);
      newFieldList.addAll(additionalFields);

    } else {

      newFieldList.addAll(currentFieldList);
      newFieldList.addAll(additionalFields);
    }

    Schema mutatedSchema = Schema.of(newFieldList);
    return mutatedSchema;
  }
}
