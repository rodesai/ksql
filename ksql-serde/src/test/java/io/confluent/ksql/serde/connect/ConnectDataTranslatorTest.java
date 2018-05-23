/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.serde.connect;

import io.confluent.ksql.GenericRow;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class ConnectDataTranslatorTest {
  ConnectDataTranslator connectToKsqlTranslator = new ConnectDataTranslator();

  @Test
  public void shouldTranslateStructCorrectly() {
    Schema structSchema = SchemaBuilder
        .struct()
        .field("INT", SchemaBuilder.INT32_SCHEMA)
        .field("LONG", SchemaBuilder.INT64_SCHEMA)
        .build();
    Schema rowSchema = SchemaBuilder
        .struct()
        .field("STRUCT", structSchema)
        .build();

    Struct connectStruct = new Struct(rowSchema);
    Struct structColumn = new Struct(structSchema);
    structColumn.put("INT", 123);
    structColumn.put("LONG", 456L);
    connectStruct.put("STRUCT", structColumn);

    GenericRow row = connectToKsqlTranslator.toKsqlRow(rowSchema, rowSchema, connectStruct);

    assertThat(row.getColumns().size(), equalTo(1));
    assertThat(row.getColumnValue(0), instanceOf(Struct.class));
    Struct connectStructColumn = row.getColumnValue(0);
    assertThat(connectStructColumn.schema(), equalTo(structSchema));
    assertThat(connectStructColumn.get("INT"), equalTo(123));
    assertThat(connectStructColumn.get("LONG"), equalTo(456L));
  }

  @Test
  public void shouldThrowOnTypeMismatch() {
    Schema schema = SchemaBuilder.struct()
        .field("FIELD", SchemaBuilder.INT32_SCHEMA)
        .build();

    Schema badSchema = SchemaBuilder.struct()
        .field("FIELD", SchemaBuilder.STRING_SCHEMA)
        .build();
    Struct badData = new Struct(badSchema);
    badData.put("FIELD", "fubar");

    try {
      connectToKsqlTranslator.toKsqlRow(schema, badSchema, badData);
      Assert.fail("Translation failed to detect bad connect type");
    } catch (DataException e) {
      assertThat(e.getMessage(), containsString(Schema.Type.STRING.getName()));
      assertThat(e.getMessage(), containsString(Schema.Type.INT32.getName()));
    }
  }

  @Test
  public void shouldTranslateStructFieldWithDifferentCase() {
    Schema structSchema = SchemaBuilder
        .struct()
        .field("INT", SchemaBuilder.INT32_SCHEMA)
        .build();
    Schema rowSchema = SchemaBuilder
        .struct()
        .field("STRUCT", structSchema)
        .build();

    Schema dataStructSchema = SchemaBuilder
        .struct()
        .field("iNt", SchemaBuilder.INT32_SCHEMA)
        .build();
    Schema dataRowSchema = SchemaBuilder
        .struct()
        .field("STRUCT", dataStructSchema)
        .build();
    Struct connectStruct = new Struct(dataRowSchema);
    Struct structColumn = new Struct(dataStructSchema);
    structColumn.put("iNt", 123);
    connectStruct.put("STRUCT", structColumn);

    GenericRow row = connectToKsqlTranslator.toKsqlRow(
        rowSchema, dataRowSchema, connectStruct);

    assertThat(row.getColumns().size(), equalTo(1));
    assertThat(row.getColumnValue(0), instanceOf(Struct.class));
    Struct connectStructColumn = row.getColumnValue(0);
    assertThat(connectStructColumn.schema(), equalTo(structSchema));
    assertThat(connectStructColumn.get("INT"), equalTo(123));
  }

  @Test
  public void shouldThrowIfNestedFieldTypeDoesntMatch() {
    Schema structSchema = SchemaBuilder
        .struct()
        .field("INT", SchemaBuilder.INT32_SCHEMA)
        .build();
    Schema rowSchema = SchemaBuilder
        .struct()
        .field("STRUCT", structSchema)
        .build();

    Schema dataStructSchema = SchemaBuilder
        .struct()
        .field("INT", SchemaBuilder.STRING_SCHEMA)
        .build();
    Schema dataRowSchema = SchemaBuilder
        .struct()
        .field("STRUCT", dataStructSchema)
        .build();
    Struct connectStruct = new Struct(dataRowSchema);
    Struct structColumn = new Struct(dataStructSchema);
    structColumn.put("INT", "123");
    connectStruct.put("STRUCT", structColumn);

    try {
      connectToKsqlTranslator.toKsqlRow(rowSchema, dataRowSchema, connectStruct);
      Assert.fail("Translation failed to check nested field");
    } catch (DataException e) {
      assertThat(e.getMessage(), containsString(Schema.Type.INT32.getName()));
      assertThat(e.getMessage(), containsString(Schema.Type.STRING.getName()));
    }
  }

  @Test
  public void shouldTranslateNullValueCorrectly() {
    Schema rowSchema = SchemaBuilder.struct()
        .field("INT", SchemaBuilder.OPTIONAL_INT32_SCHEMA)
        .build();

    Struct connectStruct = new Struct(rowSchema);

    GenericRow row = connectToKsqlTranslator.toKsqlRow(
        rowSchema, rowSchema, connectStruct);
    assertThat(row.getColumns().size(), equalTo(1));
    assertThat(row.getColumnValue(0), is(nullValue()));
  }

  @Test
  public void shouldTranslateMissingStructFieldToNull() {
    Schema structSchema = SchemaBuilder
        .struct()
        .field("INT", SchemaBuilder.INT32_SCHEMA)
        .build();
    Schema rowSchema = SchemaBuilder
        .struct()
        .field("STRUCT", structSchema)
        .build();

    Schema dataRowSchema = SchemaBuilder
        .struct()
        .field("OTHER", SchemaBuilder.INT32_SCHEMA)
        .build();
    Struct connectStruct = new Struct(dataRowSchema);
    connectStruct.put("OTHER", 123);

    GenericRow row = connectToKsqlTranslator.toKsqlRow(
        rowSchema, rowSchema, connectStruct);
    assertThat(row.getColumns().size(), equalTo(1));
    assertThat(row.getColumnValue(0), is(nullValue()));
  }
}
