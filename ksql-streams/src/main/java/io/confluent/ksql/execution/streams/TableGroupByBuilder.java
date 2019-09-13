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

package io.confluent.ksql.execution.streams;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.codegen.CodeGenRunner;
import io.confluent.ksql.execution.codegen.ExpressionMetadata;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.TableGroupBy;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.serde.KeySerde;
import java.util.List;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;

public final class TableGroupByBuilder {
  private TableGroupByBuilder() {
  }

  public static KGroupedTable<Object, GenericRow> build(
      final KTable<Object, GenericRow> ktable,
      final TableGroupBy<?, ?> step,
      final KsqlQueryBuilder queryBuilder,
      final GroupedFactory groupedFactory
  ) {
    final LogicalSchema sourceSchema = step.getSources().get(0).getProperties().getSchema();
    final QueryContext queryContext =  step.getProperties().getQueryContext();
    final Formats formats = step.getFormats();
    final PhysicalSchema physicalSchema = PhysicalSchema.from(
        sourceSchema,
        formats.getOptions()
    );
    final KeySerde<Object> keySerde = queryBuilder.buildKeySerde(
        formats.getKeyFormat(),
        physicalSchema,
        queryContext
    );
    final Serde<GenericRow> valSerde = queryBuilder.buildValueSerde(
        formats.getValueFormat().getFormatInfo(),
        physicalSchema,
        queryContext
    );
    final Grouped<Object, GenericRow> grouped = groupedFactory.create(
        StreamsUtil.buildOpName(queryContext),
        keySerde,
        valSerde
    );
    final List<ExpressionMetadata> groupBy = CodeGenRunner.compileExpressions(
        step.getGroupByExpressions().stream(),
        "Group By",
        sourceSchema,
        queryBuilder.getKsqlConfig(),
        queryBuilder.getFunctionRegistry()
    );
    final GroupByMapper mapper = new GroupByMapper(groupBy);
    return ktable
        .filter((key, value) -> value != null)
        .groupBy(new TableKeyValueMapper(mapper), grouped);
  }

  public static final class TableKeyValueMapper
      implements KeyValueMapper<Object, GenericRow, KeyValue<Object, GenericRow>> {
    private final GroupByMapper groupByMapper;

    private TableKeyValueMapper(final GroupByMapper groupByMapper) {
      this.groupByMapper = groupByMapper;
    }

    @Override
    public KeyValue<Object, GenericRow> apply(final Object key, final GenericRow value) {
      return new KeyValue<>(groupByMapper.apply(key, value), value);
    }

    GroupByMapper  getGroupByMapper() {
      return groupByMapper;
    }
  }
}
