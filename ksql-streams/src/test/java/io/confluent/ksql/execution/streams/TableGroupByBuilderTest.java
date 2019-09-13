package io.confluent.ksql.execution.streams;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.DereferenceExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.QualifiedName;
import io.confluent.ksql.execution.expression.tree.QualifiedNameReference;
import io.confluent.ksql.execution.plan.DefaultExecutionStepProperties;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.ExecutionStepProperties;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.TableGroupBy;
import io.confluent.ksql.execution.streams.TableGroupByBuilder.TableKeyValueMapper;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.KeySerde;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class TableGroupByBuilderTest {
  private static final String ALIAS = "SOURCE";
  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .valueColumn("PAC", SqlTypes.BIGINT)
      .valueColumn("MAN", SqlTypes.STRING)
      .build()
      .withAlias(ALIAS)
      .withMetaAndKeyColsInValue();
  private static final PhysicalSchema PHYSICAL_SCHEMA = PhysicalSchema.from(SCHEMA, SerdeOption.none());

  private final List<Expression> groupByExpressions = ImmutableList.of(
      dereference("PAC"),
      dereference("MAN")
  );
  private final QueryContext sourceContext =
      new QueryContext.Stacker(new QueryId("qid")).push("foo").push("source").getQueryContext();
  private final QueryContext stepContext =
      new QueryContext.Stacker(new QueryId("qid")).push("foo").push("groupby").getQueryContext();
  private final ExecutionStepProperties sourceProperties = new DefaultExecutionStepProperties(
      SCHEMA,
      sourceContext
  );
  private final ExecutionStepProperties properties = new DefaultExecutionStepProperties(
      SCHEMA,
      stepContext
  );
  private final Formats formats = Formats.of(
      KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA)),
      ValueFormat.of(FormatInfo.of(Format.JSON)),
      SerdeOption.none()
  );

  @Mock
  private KsqlQueryBuilder queryBuilder;
  @Mock
  private KsqlConfig ksqlConfig;
  @Mock
  private FunctionRegistry functionRegistry;
  @Mock
  private GroupedFactory groupedFactory;
  @Mock
  private ExecutionStep sourceStep;
  @Mock
  private KeySerde keySerde;
  @Mock
  private Serde<GenericRow> valueSerde;
  @Mock
  private Grouped grouped;
  @Mock
  private KTable sourceTable;
  @Mock
  private KTable filteredTable;
  @Mock
  private KGroupedTable groupedTable;
  @Captor
  private ArgumentCaptor<TableKeyValueMapper> mapperCaptor;
  @Captor
  private ArgumentCaptor<Predicate> predicateCaptor;

  private TableGroupBy<?, ?> groupBy;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Before
  @SuppressWarnings("unchecked")
  public void init() {
    when(queryBuilder.getKsqlConfig()).thenReturn(ksqlConfig);
    when(queryBuilder.getFunctionRegistry()).thenReturn(functionRegistry);
    when(queryBuilder.buildKeySerde(any(KeyFormat.class), any(), any())).thenReturn(keySerde);
    when(queryBuilder.buildValueSerde(any(), any(), any())).thenReturn(valueSerde);
    when(groupedFactory.create(any(), any(), any())).thenReturn(grouped);
    when(sourceTable.filter(any())).thenReturn(filteredTable);
    when(filteredTable.groupBy(any(KeyValueMapper.class), any(Grouped.class)))
        .thenReturn(groupedTable);
    when(sourceStep.getProperties()).thenReturn(sourceProperties);
    groupBy = new TableGroupBy<>(
        properties,
        sourceStep,
        formats,
        groupByExpressions
    );
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldPerformGroupByCorrectly() {
    // When:
    final KGroupedTable result =
        TableGroupByBuilder.build(sourceTable, groupBy, queryBuilder, groupedFactory);

    // Then:
    assertThat(result, is(groupedTable));
    verify(sourceTable).filter(any());
    verify(filteredTable).groupBy(mapperCaptor.capture(), same(grouped));
    verifyNoMoreInteractions(filteredTable, sourceTable);
    final GroupByMapper mapper = mapperCaptor.getValue().getGroupByMapper();
    assertThat(mapper.getExpressionMetadata(), hasSize(2));
    assertThat(
        mapper.getExpressionMetadata().get(0).getExpression(),
        equalTo(groupByExpressions.get(0))
    );
    assertThat(
        mapper.getExpressionMetadata().get(1).getExpression(),
        equalTo(groupByExpressions.get(1))
    );
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldFilterNullRowsBeforeGroupBy() {
    // When:
    TableGroupByBuilder.build(sourceTable, groupBy, queryBuilder, groupedFactory);

    // Then:
    verify(sourceTable).filter(predicateCaptor.capture());
    final Predicate predicate = predicateCaptor.getValue();
    assertThat(predicate.test(new Object(), new GenericRow()), is(true));
    assertThat(predicate.test(new Object(), null),  is(false));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldBuildGroupedCorrectlyForGroupBy() {
    // When:
    TableGroupByBuilder.build(sourceTable, groupBy, queryBuilder, groupedFactory);

    // Then:
    verify(groupedFactory).create("foo-groupby", keySerde, valueSerde);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldBuildKeySerdeCorrectlyForGroupBy() {
    // When:
    TableGroupByBuilder.build(sourceTable, groupBy, queryBuilder, groupedFactory);

    // Then:
    verify(queryBuilder).buildKeySerde(formats.getKeyFormat(), PHYSICAL_SCHEMA, stepContext);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldBuildValueSerdeCorrectlyForGroupBy() {
    // When:
    TableGroupByBuilder.build(sourceTable, groupBy, queryBuilder, groupedFactory);

    // Then:
    verify(queryBuilder).buildValueSerde(
        formats.getValueFormat().getFormatInfo(),
        PHYSICAL_SCHEMA,
        stepContext
    );
  }

  private static Expression dereference(final String column) {
    return new DereferenceExpression(new QualifiedNameReference(QualifiedName.of(ALIAS)), column);
  }
}