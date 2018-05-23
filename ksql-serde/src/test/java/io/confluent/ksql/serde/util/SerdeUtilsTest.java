package io.confluent.ksql.serde.util;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SerdeUtilsTest {
  @Test
  public void shouldConvertFirstLevelToOptional() {
    Schema innerSchema = SchemaBuilder.struct()
        .field("foo", Schema.INT32_SCHEMA)
        .field("bar", Schema.INT64_SCHEMA)
        .build();
    Schema schema = SchemaBuilder.struct()
        .field("primitive", Schema.INT32_SCHEMA)
        .field("array", SchemaBuilder.array(Schema.INT32_SCHEMA))
        .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.INT32_SCHEMA))
        .field("struct", innerSchema)
        .build();

    Schema optional = SerdeUtils.toOptionalSchema(schema);

    for (int i = 0; i < schema.fields().size(); i++) {
      assertThat(
          optional.fields().get(i).name(),
          equalTo(schema.fields().get(i).name()));
      assertThat(
          optional.fields().get(i).schema().type(),
          equalTo(schema.fields().get(i).schema().type()));
      assertThat(optional.fields().get(i).schema().isOptional(), is(true));
    }
    assertThat(
        optional.field("array").schema().valueSchema(),
        equalTo(schema.field("array").schema().valueSchema()));
    assertThat(
        optional.field("map").schema().keySchema(),
        equalTo(schema.field("map").schema().keySchema()));
    assertThat(
        optional.field("map").schema().valueSchema(),
        equalTo(schema.field("map").schema().valueSchema()));
    assertThat(
        optional.field("struct").schema().fields(),
        equalTo(schema.field("struct").schema().fields()));
  }
}
