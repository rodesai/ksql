/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.structured;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.context.QueryContext.Stacker;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.plan.LogicalSchemaWithMetaAndKeyFields;
import io.confluent.ksql.execution.plan.StreamSource;
import io.confluent.ksql.execution.plan.TableSource;
import io.confluent.ksql.execution.plan.WindowedStreamSource;
import io.confluent.ksql.execution.plan.WindowedTableSource;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Optional;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SchemaKSourceFactoryTest {

  private static final Optional<AutoOffsetReset> OFFSET_RESET = Optional.of(
      AutoOffsetReset.EARLIEST);
  private static final KeyField KEY_FIELD = KeyField.none();
  private static final SourceName SOURCE = SourceName.of("alice");
  private static final SourceName ALIAS = SourceName.of("bob");
  private static final KsqlConfig CONFIG = new KsqlConfig(ImmutableMap.of());

  @Mock
  private KsqlQueryBuilder builder;
  @Mock
  private DataSource<?> dataSource;
  @Mock
  private LogicalSchemaWithMetaAndKeyFields schemaWithStuff;
  @Mock
  private LogicalSchema schema;
  @Mock
  private Stacker contextStacker;
  @Mock
  private QueryContext queryContext;
  @Mock
  private KsqlTopic topic;
  @Mock
  private KeyFormat keyFormat;
  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private WindowInfo windowInfo;

  @Before
  public void setUp() {
    when(dataSource.getKsqlTopic()).thenReturn(topic);
    when(dataSource.getName()).thenReturn(SOURCE);

    when(topic.getKeyFormat()).thenReturn(keyFormat);

    when(schemaWithStuff.getSchema()).thenReturn(schema);

    when(contextStacker.getQueryContext()).thenReturn(queryContext);

    when(builder.getKsqlConfig()).thenReturn(CONFIG);
    when(builder.getFunctionRegistry()).thenReturn(functionRegistry);

  }

  @Test
  public void shouldBuildWindowedStream() {
    // Given:
    when(dataSource.getDataSourceType()).thenReturn(DataSourceType.KSTREAM);
    when(keyFormat.isWindowed()).thenReturn(true);
    when(keyFormat.getWindowInfo()).thenReturn(Optional.of(windowInfo));

    // When:
    final SchemaKStream<?> result = SchemaKSourceFactory.buildSource(
        builder,
        dataSource,
        schemaWithStuff,
        contextStacker,
        OFFSET_RESET,
        KEY_FIELD,
        ALIAS
    );

    // Then:
    assertThat(result, not(instanceOf(SchemaKTable.class)));
    assertThat(result.getSourceStep(), instanceOf(WindowedStreamSource.class));

    assertThat(result.getSchema(), is(schema));
    assertThat(result.getSourceSchemaKStreams(), is(empty()));
    assertThat(result.getSourceStep().getSources(), is(empty()));
  }

  @Test
  public void shouldBuildNonWindowedStream() {
    // Given:
    when(dataSource.getDataSourceType()).thenReturn(DataSourceType.KSTREAM);
    when(keyFormat.isWindowed()).thenReturn(false);
    when(keyFormat.getWindowInfo()).thenReturn(Optional.empty());

    // When:
    final SchemaKStream<?> result = SchemaKSourceFactory.buildSource(
        builder,
        dataSource,
        schemaWithStuff,
        contextStacker,
        OFFSET_RESET,
        KEY_FIELD,
        ALIAS
    );

    // Then:
    assertThat(result, not(instanceOf(SchemaKTable.class)));
    assertThat(result.getSourceStep(), instanceOf(StreamSource.class));

    assertThat(result.getSchema(), is(schema));
    assertThat(result.getSourceSchemaKStreams(), is(empty()));
    assertThat(result.getSourceStep().getSources(), is(empty()));
  }

  @Test
  public void shouldBuildWindowedTable() {
    // Given:
    when(dataSource.getDataSourceType()).thenReturn(DataSourceType.KTABLE);
    when(keyFormat.isWindowed()).thenReturn(true);
    when(keyFormat.getWindowInfo()).thenReturn(Optional.of(windowInfo));

    // When:
    final SchemaKStream<?> result = SchemaKSourceFactory.buildSource(
        builder,
        dataSource,
        schemaWithStuff,
        contextStacker,
        OFFSET_RESET,
        KEY_FIELD,
        ALIAS
    );

    // Then:
    assertThat(result, instanceOf(SchemaKTable.class));
    assertThat(result.getSourceStep(), instanceOf(WindowedTableSource.class));

    assertThat(result.getSchema(), is(schema));
    assertThat(result.getSourceSchemaKStreams(), is(empty()));
    assertThat(result.getSourceStep().getSources(), is(empty()));
  }

  @Test
  public void shouldBuildNonWindowedTable() {
    // Given:
    when(dataSource.getDataSourceType()).thenReturn(DataSourceType.KTABLE);
    when(keyFormat.isWindowed()).thenReturn(false);
    when(keyFormat.getWindowInfo()).thenReturn(Optional.empty());

    // When:
    final SchemaKStream<?> result = SchemaKSourceFactory.buildSource(
        builder,
        dataSource,
        schemaWithStuff,
        contextStacker,
        OFFSET_RESET,
        KEY_FIELD,
        ALIAS
    );

    // Then:
    assertThat(result, instanceOf(SchemaKTable.class));
    assertThat(result.getSourceStep(), instanceOf(TableSource.class));

    assertThat(result.getSchema(), is(schema));
    assertThat(result.getSourceSchemaKStreams(), is(empty()));
    assertThat(result.getSourceStep().getSources(), is(empty()));
  }
}