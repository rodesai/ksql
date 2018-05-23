/**
 * Copyright 2017 Confluent Inc.
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

package io.confluent.ksql.serde.avro;

import io.confluent.ksql.util.KsqlConfig;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.ksql.GenericRow;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;


public class KsqlGenericRowAvroDeserializerTest {

  String schemaStr = "{"
                     + "\"namespace\": \"kql\","
                     + " \"name\": \"orders\","
                     + " \"type\": \"record\","
                     + " \"fields\": ["
                     + "     {\"name\": \"orderTime\", \"type\": \"long\"},"
                     + "     {\"name\": \"orderId\",  \"type\": \"long\"},"
                     + "     {\"name\": \"itemId\", \"type\": \"string\"},"
                     + "     {\"name\": \"orderUnits\", \"type\": \"double\"},"
                     + "     {\"name\": \"arrayCol\", \"type\": {\"type\": \"array\", \"items\": "
                     + "\"double\"}},"
                     + "     {\"name\": \"mapCol\", \"type\": {\"type\": \"map\", \"values\": "
                     + "\"double\"}}"
                     + " ]"
                     + "}";

  Schema schema;
  org.apache.avro.Schema avroSchema;
  KsqlConfig ksqlConfig;

  @Before
  public void before() {
    org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
    avroSchema = parser.parse(schemaStr);

    schema = SchemaBuilder.struct()
        .field("ORDERTIME".toUpperCase(), Schema.INT64_SCHEMA)
        .field("ORDERID".toUpperCase(), Schema.INT64_SCHEMA)
        .field("ITEMID".toUpperCase(), Schema.STRING_SCHEMA)
        .field("ORDERUNITS".toUpperCase(), Schema.FLOAT64_SCHEMA)
        .field("ARRAYCOL".toUpperCase(), SchemaBuilder.array(Schema.FLOAT64_SCHEMA))
        .field("MAPCOL".toUpperCase(), SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.FLOAT64_SCHEMA))
        .build();

    ksqlConfig = new KsqlConfig(
        Collections.singletonMap(
            KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY,
            "fake-schema-registry-url"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldDeserializeCorrectly() {
    SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    List columns = Arrays.asList(1511897796092L, 1L, "item_1", 10.0, new Double[]{100.0},
                                 Collections.singletonMap("key1", 100.0));

    GenericRow genericRow = new GenericRow(columns);

    Deserializer<GenericRow> deserializer =
        new KsqlAvroTopicSerDe().getGenericRowSerde(
            schema, ksqlConfig, false, schemaRegistryClient).deserializer();

    byte[] serializedRow = getSerializedRow("t1", schemaRegistryClient, avroSchema, genericRow);

    GenericRow row = deserializer.deserialize("t1", serializedRow);
    Assert.assertNotNull(row);
    assertThat("Incorrect deserializarion", row.getColumns().size(), equalTo(6));
    assertThat("Incorrect deserializarion", row.getColumns().get(0), equalTo(1511897796092L));
    assertThat("Incorrect deserializarion", row.getColumns().get(1), equalTo
        (1L));
    assertThat("Incorrect deserializarion", row.getColumns().get(2), equalTo
        ( "item_1"));
    assertThat("Incorrect deserializarion", row.getColumns().get(3), equalTo
        ( 10.0));
    assertThat("Incorrect deserializarion", ((ArrayList)row.getColumns().get(4)).size(), equalTo
        (1));
    assertThat("Incorrect deserializarion", ((Map)row.getColumns().get(5)).size(), equalTo
        (1));
  }

  @Test
  public void shouldDeserializeIfThereAreRedundantFields() {
    Schema newSchema = SchemaBuilder.struct()
        .field("ordertime".toUpperCase(), Schema.INT64_SCHEMA)
        .field("orderid".toUpperCase(), Schema.INT64_SCHEMA)
        .field("itemid".toUpperCase(), Schema.STRING_SCHEMA)
        .field("orderunits".toUpperCase(), Schema.FLOAT64_SCHEMA)
        .build();
    SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    List columns = Arrays.asList(1511897796092L, 1L, "item_1", 10.0, new Double[]{100.0},
                                 Collections.singletonMap("key1", 100.0));

    GenericRow genericRow = new GenericRow(columns);

    Deserializer<GenericRow> deserializer =
        new KsqlAvroTopicSerDe().getGenericRowSerde(
            newSchema, ksqlConfig, false, schemaRegistryClient).deserializer();

    byte[] serializedRow = getSerializedRow("t1", schemaRegistryClient, avroSchema, genericRow);
    GenericRow row = deserializer.deserialize("t1", serializedRow);
    Assert.assertNotNull(row);
    assertThat("Incorrect deserializarion", row.getColumns().size(), equalTo(4));
    assertThat("Incorrect deserializarion", (Long)row.getColumns().get(0), equalTo(1511897796092L));
    assertThat("Incorrect deserializarion", (Long)row.getColumns().get(1), equalTo
        (1L));
    assertThat("Incorrect deserializarion", (String)row.getColumns().get(2), equalTo
        ( "item_1"));
  }


  @Test
  public void shouldDeserializeWithMissingFields() {
    String schemaStr1 = "{"
                        + "\"namespace\": \"kql\","
                        + " \"name\": \"orders\","
                        + " \"type\": \"record\","
                        + " \"fields\": ["
                        + "     {\"name\": \"orderTime\", \"type\": \"long\"},"
                        + "     {\"name\": \"orderId\",  \"type\": \"long\"},"
                        + "     {\"name\": \"itemId\", \"type\": \"string\"},"
                        + "     {\"name\": \"orderUnits\", \"type\": \"double\"}"
                        + " ]"
                        + "}";
    org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
    org.apache.avro.Schema avroSchema1 = parser.parse(schemaStr1);
    SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    List columns = Arrays.asList(1511897796092L, 1L, "item_1", 10.0);

    GenericRow genericRow = new GenericRow(columns);
    byte[] serializedRow = getSerializedRow("t1", schemaRegistryClient, avroSchema1, genericRow);

    Deserializer<GenericRow> deserializer =
        new KsqlAvroTopicSerDe().getGenericRowSerde(
            schema, ksqlConfig, false, schemaRegistryClient).deserializer();

    GenericRow row = deserializer.deserialize("t1", serializedRow);
    assertThat("Incorrect deserializarion", row.getColumns().size(), equalTo(6));
    assertThat("Incorrect deserializarion", (Long)row.getColumns().get(0), equalTo(1511897796092L));
    assertThat("Incorrect deserializarion", (Long)row.getColumns().get(1), equalTo
        (1L));
    assertThat("Incorrect deserializarion", (String)row.getColumns().get(2), equalTo
        ( "item_1"));
    Assert.assertNull(row.getColumns().get(4));
    Assert.assertNull(row.getColumns().get(5));
  }

  private byte[] getSerializedRow(String topicName, SchemaRegistryClient schemaRegistryClient,
                                  org.apache.avro.Schema rowAvroSchema,
                                  GenericRow genericRow) {
    Map map = new HashMap();
    // Automatically register the schema in the Schema Registry if it has not been registered.
    map.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
    map.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "");
    KafkaAvroSerializer kafkaAvroSerializer = new KafkaAvroSerializer(schemaRegistryClient, map);
    GenericRecord avroRecord = new GenericData.Record(rowAvroSchema);
    List<org.apache.avro.Schema.Field> fields = rowAvroSchema.getFields();
    for (int i = 0; i < genericRow.getColumns().size(); i++) {
      if (fields.get(i).schema().getType() == org.apache.avro.Schema.Type.ARRAY) {
        avroRecord.put(fields.get(i).name(), Arrays.asList((Object[]) genericRow.getColumns().get(i)));
      } else {
        avroRecord.put(fields.get(i).name(), genericRow.getColumns().get(i));
      }
    }

    return kafkaAvroSerializer.serialize(topicName, avroRecord);
  }

}
