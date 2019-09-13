/*
 * Copyright 2018 Confluent Inc.
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

import static io.confluent.ksql.metastore.model.MetaStoreMatchers.KeyFieldMatchers.hasLegacyName;
import static io.confluent.ksql.metastore.model.MetaStoreMatchers.KeyFieldMatchers.hasLegacyType;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.same;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.execution.expression.tree.DereferenceExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.QualifiedName;
import io.confluent.ksql.execution.expression.tree.QualifiedNameReference;
import io.confluent.ksql.execution.plan.DefaultExecutionStepProperties;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.JoinType;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.streams.ExecutionStepFactory;
import io.confluent.ksql.execution.util.StructKeyUtil;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogContext;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KeyField.LegacyField;
import io.confluent.ksql.metastore.model.KsqlTable;
import io.confluent.ksql.metastore.model.MetaStoreMatchers.KeyFieldMatchers;
import io.confluent.ksql.planner.plan.FilterNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.ProjectNode;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.GenericRowSerDe;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.KeySerde;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.execution.streams.GroupedFactory;
import io.confluent.ksql.streams.JoinedFactory;
import io.confluent.ksql.execution.streams.MaterializedFactory;
import io.confluent.ksql.streams.StreamsFactories;
import io.confluent.ksql.execution.streams.StreamsUtil;
import io.confluent.ksql.structured.SchemaKStream.Type;
import io.confluent.ksql.testutils.AnalysisTestUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.SchemaUtil;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class SchemaKTableTest {

  private final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());
  private final MetaStore metaStore = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());
  private final GroupedFactory groupedFactory = mock(GroupedFactory.class);
  private final Grouped grouped = Grouped.with(
      "group", Serdes.String(), Serdes.String());
  private final KeyField validKeyField = KeyField.of(
      Optional.of("left.COL1"),
      metaStore.getSource("TEST2")
          .getKeyField()
          .legacy()
          .map(field -> field.withSource("left")));

  private SchemaKTable initialSchemaKTable;
  private KTable kTable;
  private KsqlTable<?> ksqlTable;
  private InternalFunctionRegistry functionRegistry;
  private KTable mockKTable;
  private SchemaKTable firstSchemaKTable;
  private SchemaKTable secondSchemaKTable;
  private LogicalSchema joinSchema;
  private final QueryContext.Stacker queryContext
      = new QueryContext.Stacker(new QueryId("query")).push("node");
  private final QueryContext.Stacker childContextStacker = queryContext.push("child");
  private final ProcessingLogContext processingLogContext = ProcessingLogContext.create();
  private Serde<GenericRow> rowSerde;
  private static final Expression TEST_2_COL_1 = new DereferenceExpression(
      new QualifiedNameReference(QualifiedName.of("TEST2")), "COL1");
  private static final Expression TEST_2_COL_2 = new DereferenceExpression(
      new QualifiedNameReference(QualifiedName.of("TEST2")), "COL2");
  private static final KeyFormat keyFormat = KeyFormat.nonWindowed(FormatInfo.of(Format.JSON));
  private static final ValueFormat valueFormat = ValueFormat.of(FormatInfo.of(Format.JSON));

  @Mock
  private KeySerde<Struct> keySerde;
  @Mock
  private KeySerde<Struct> reboundKeySerde;
  @Mock
  private KsqlQueryBuilder queryBuilder;

  @Before
  public void init() {
    functionRegistry = new InternalFunctionRegistry();
    ksqlTable = (KsqlTable) metaStore.getSource("TEST2");
    final StreamsBuilder builder = new StreamsBuilder();
    kTable = builder.table(
        ksqlTable.getKsqlTopic().getKafkaTopicName(),
        Consumed.with(
            Serdes.String(),
            getRowSerde(ksqlTable.getKsqlTopic(), ksqlTable.getSchema().valueConnectSchema())
        ));

    final KsqlTable secondKsqlTable = (KsqlTable) metaStore.getSource("TEST3");
    final KTable secondKTable = builder.table(
        secondKsqlTable.getKsqlTopic().getKafkaTopicName(),
        Consumed.with(
            Serdes.String(),
            getRowSerde(secondKsqlTable.getKsqlTopic(), secondKsqlTable.getSchema().valueConnectSchema())
        ));

    mockKTable = EasyMock.niceMock(KTable.class);
    firstSchemaKTable = buildSchemaKTableForJoin(ksqlTable, mockKTable);
    secondSchemaKTable = buildSchemaKTableForJoin(secondKsqlTable, secondKTable);
    joinSchema = getJoinSchema(ksqlTable.getSchema(), secondKsqlTable.getSchema());

    when(keySerde.rebind(any(PersistenceSchema.class))).thenReturn(reboundKeySerde);
    when(queryBuilder.getKsqlConfig()).thenReturn(ksqlConfig);
    when(queryBuilder.getProcessingLogContext()).thenReturn(processingLogContext);
    when(queryBuilder.getFunctionRegistry()).thenReturn(functionRegistry);
  }

  private ExecutionStep buildSourceStep(final LogicalSchema schema) {
    final ExecutionStep sourceStep = Mockito.mock(ExecutionStep.class);
    when(sourceStep.getProperties()).thenReturn(
        new DefaultExecutionStepProperties(schema, queryContext.getQueryContext()));
    return sourceStep;
  }

  private SchemaKTable buildSchemaKTable(
      final LogicalSchema schema,
      final KeyField keyField,
      final KTable kTable,
      final GroupedFactory groupedFactory) {
    return new SchemaKTable(
        kTable,
        buildSourceStep(schema),
        keyFormat,
        keySerde,
        keyField,
        new ArrayList<>(),
        Type.SOURCE,
        ksqlConfig,
        functionRegistry,
        new StreamsFactories(
            groupedFactory,
            JoinedFactory.create(ksqlConfig),
            MaterializedFactory.create(ksqlConfig))
    );
  }

  private SchemaKTable buildSchemaKTableFromPlan(final PlanNode logicalPlan) {
    return new SchemaKTable(
        kTable,
        buildSourceStep(logicalPlan.getTheSourceNode().getSchema()),
        keyFormat,
        keySerde,
        logicalPlan.getTheSourceNode().getKeyField(),
        new ArrayList<>(),
        SchemaKStream.Type.SOURCE,
        ksqlConfig,
        functionRegistry
    );
  }

  private SchemaKTable buildSchemaKTable(
      final KsqlTable ksqlTable,
      final KTable kTable,
      final GroupedFactory groupedFactory
  ) {
    final LogicalSchema schema = ksqlTable.getSchema().withAlias(ksqlTable.getName());

    final Optional<String> newKeyName = ksqlTable.getKeyField().name()
        .map(name -> SchemaUtil.buildAliasedFieldName(ksqlTable.getName(), name));

    final KeyField keyFieldWithAlias = KeyField.of(newKeyName, ksqlTable.getKeyField().legacy());

    return buildSchemaKTable(
        schema,
        keyFieldWithAlias,
        kTable,
        groupedFactory);
  }

  private SchemaKTable buildSchemaKTableForJoin(final KsqlTable ksqlTable, final KTable kTable) {
    return buildSchemaKTable(
        ksqlTable.getSchema(), ksqlTable.getKeyField(), kTable, GroupedFactory.create(ksqlConfig));
  }

  private Serde<GenericRow> getRowSerde(final KsqlTopic topic, final ConnectSchema schema) {
    return GenericRowSerDe.from(
        topic.getValueFormat().getFormatInfo(),
        PersistenceSchema.from(schema, false),
        new KsqlConfig(Collections.emptyMap()),
        MockSchemaRegistryClient::new,
        "test",
        processingLogContext);
  }

  @Test
  public void testSelectSchemaKStream() {
    // Given:
    final String selectQuery = "SELECT col0, col2, col3 FROM test2 WHERE col0 > 100 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    initialSchemaKTable = buildSchemaKTableFromPlan(logicalPlan);

    // When:
    final SchemaKTable projectedSchemaKStream = initialSchemaKTable.select(
        projectNode.getProjectSelectExpressions(),
        childContextStacker,
        queryBuilder
    );

    // Then:
    assertThat(projectedSchemaKStream.getSchema().value(), contains(
        Column.of("COL0", SqlTypes.BIGINT),
        Column.of("COL2", SqlTypes.STRING),
        Column.of("COL3", SqlTypes.DOUBLE)
    ));

    assertThat(projectedSchemaKStream.getSourceSchemaKStreams().get(0), is(initialSchemaKTable));
  }

  @Test
  public void shouldBuildStepForSelect() {
    // Given:
    final String selectQuery = "SELECT col0, col2, col3 FROM test2 WHERE col0 > 100 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    initialSchemaKTable = buildSchemaKTableFromPlan(logicalPlan);

    // When:
    final SchemaKTable projectedSchemaKStream = initialSchemaKTable.select(
        projectNode.getProjectSelectExpressions(),
        childContextStacker,
        queryBuilder
    );

    // Then:
    assertThat(
        projectedSchemaKStream.getSourceTableStep(),
        equalTo(
            ExecutionStepFactory.tableMapValues(
                childContextStacker,
                initialSchemaKTable.getSourceTableStep(),
                projectNode.getProjectSelectExpressions(),
                queryBuilder
            )
        )
    );
  }

  @Test
  public void testSelectWithExpression() {
    // Given:
    final String selectQuery = "SELECT col0, LEN(UCASE(col2)), col3*3+5 FROM test2 WHERE col0 > 100 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    initialSchemaKTable = buildSchemaKTableFromPlan(logicalPlan);

    // When:
    final SchemaKTable projectedSchemaKStream = initialSchemaKTable.select(
        projectNode.getProjectSelectExpressions(),
        childContextStacker,
        queryBuilder
    );

    // Then:
    assertThat(projectedSchemaKStream.getSchema().value(), contains(
        Column.of("COL0", SqlTypes.BIGINT),
        Column.of("KSQL_COL_1", SqlTypes.INTEGER),
        Column.of("KSQL_COL_2", SqlTypes.DOUBLE)
    ));

    assertThat(projectedSchemaKStream.getSourceSchemaKStreams().get(0), is(initialSchemaKTable));
  }

  @Test
  public void testFilter() {
    // Given:
    final String selectQuery = "SELECT col0, col2, col3 FROM test2 WHERE col0 > 100 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    final FilterNode filterNode = (FilterNode) logicalPlan.getSources().get(0).getSources().get(0);
    initialSchemaKTable = buildSchemaKTableFromPlan(logicalPlan);

    // When:
    final SchemaKTable filteredSchemaKStream = initialSchemaKTable.filter(
        filterNode.getPredicate(),
        childContextStacker,
        processingLogContext
    );

    // Then:
    assertThat(filteredSchemaKStream.getSchema().value(), contains(
        Column.of("TEST2", "ROWTIME", SqlTypes.BIGINT),
        Column.of("TEST2", "ROWKEY", SqlTypes.STRING),
        Column.of("TEST2", "COL0", SqlTypes.BIGINT),
        Column.of("TEST2", "COL1", SqlTypes.STRING),
        Column.of("TEST2", "COL2", SqlTypes.STRING),
        Column.of("TEST2", "COL3", SqlTypes.DOUBLE),
        Column.of("TEST2", "COL4", SqlTypes.BOOLEAN)
    ));

    assertThat(filteredSchemaKStream.getSourceSchemaKStreams().get(0), is(initialSchemaKTable));
  }

  @Test
  public void shouldBuildStepForFilter() {
    // Given:
    final String selectQuery = "SELECT col0, col2, col3 FROM test2 WHERE col0 > 100 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    final FilterNode filterNode = (FilterNode) logicalPlan.getSources().get(0).getSources().get(0);
    initialSchemaKTable = buildSchemaKTableFromPlan(logicalPlan);

    // When:
    final SchemaKTable filteredSchemaKStream = initialSchemaKTable.filter(
        filterNode.getPredicate(),
        childContextStacker,
        processingLogContext
    );

    // Then:
    assertThat(
        filteredSchemaKStream.getSourceTableStep(),
        equalTo(
            ExecutionStepFactory.tableFilter(
                childContextStacker,
                initialSchemaKTable.getSourceTableStep(),
                filterNode.getPredicate()
            )
        )
    );
  }

  @Test
  public void testGroupBy() {
    // Given:
    final String selectQuery = "SELECT col0, col1, col2 FROM test2 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    initialSchemaKTable = buildSchemaKTableFromPlan(logicalPlan);
    final List<Expression> groupByExpressions = Arrays.asList(TEST_2_COL_2, TEST_2_COL_1);

    // When:
    final SchemaKGroupedStream groupedSchemaKTable = initialSchemaKTable.groupBy(
        valueFormat,
        groupByExpressions,
        childContextStacker,
        queryBuilder);

    // Then:
    assertThat(groupedSchemaKTable, instanceOf(SchemaKGroupedTable.class));
    assertThat(groupedSchemaKTable.getKeyField().name(), is(Optional.empty()));
    assertThat(groupedSchemaKTable.getKeyField().legacy().map(LegacyField::name),
        is(Optional.of("TEST2.COL2|+|TEST2.COL1")));
  }

  @Test
  public void shouldBuildStepForGroupBy() {
    // Given:
    final String selectQuery = "SELECT col0, col1, col2 FROM test2 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    initialSchemaKTable = buildSchemaKTableFromPlan(logicalPlan);
    final List<Expression> groupByExpressions = Arrays.asList(TEST_2_COL_2, TEST_2_COL_1);

    // When:
    final SchemaKGroupedStream groupedSchemaKTable = initialSchemaKTable.groupBy(
        valueFormat,
        groupByExpressions,
        childContextStacker,
        queryBuilder);

    // Then:
    assertThat(
        ((SchemaKGroupedTable) groupedSchemaKTable).getSourceTableStep(),
        equalTo(
            ExecutionStepFactory.tableGroupBy(
                childContextStacker,
                initialSchemaKTable.getSourceTableStep(),
                Formats.of(initialSchemaKTable.keyFormat, valueFormat, SerdeOption.none()),
                groupByExpressions
            )
        )
    );
  }

  @Test
  public void shouldUseOpNameForGrouped() {
    // Given:
    final Serde<GenericRow> valSerde =
        getRowSerde(ksqlTable.getKsqlTopic(), ksqlTable.getSchema().valueConnectSchema());
    when(queryBuilder.buildValueSerde(any(), any(), any())).thenReturn(valSerde);
    expect(
        groupedFactory.create(
            eq(StreamsUtil.buildOpName(childContextStacker.getQueryContext())),
            anyObject(Serdes.String().getClass()),
            same(valSerde))
    ).andReturn(grouped);
    expect(mockKTable.filter(anyObject(Predicate.class))).andReturn(mockKTable);
    final KGroupedTable groupedTable = mock(KGroupedTable.class);
    expect(mockKTable.groupBy(anyObject(), same(grouped))).andReturn(groupedTable);
    replay(groupedFactory, mockKTable);

    final List<Expression> groupByExpressions = Collections.singletonList(TEST_2_COL_1);
    final SchemaKTable schemaKTable = buildSchemaKTable(ksqlTable, mockKTable, groupedFactory);

    // When:
    schemaKTable.groupBy(valueFormat, groupByExpressions, childContextStacker, queryBuilder);

    // Then:
    verify(mockKTable, groupedFactory);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldGroupKeysCorrectly() {
    // set up a mock KTable and KGroupedTable for the test. Set up the KTable to
    // capture the mapper that is passed in to produce new keys
    final KTable mockKTable = mock(KTable.class);
    final KGroupedTable mockKGroupedTable = mock(KGroupedTable.class);
    final Capture<KeyValueMapper> capturedKeySelector = Capture.newInstance();
    expect(mockKTable.filter(anyObject(Predicate.class))).andReturn(mockKTable);
    expect(mockKTable.groupBy(capture(capturedKeySelector), anyObject(Grouped.class)))
        .andReturn(mockKGroupedTable);
    replay(mockKTable, mockKGroupedTable);

    // Build our test object from the mocks
    final String selectQuery = "SELECT col0, col1, col2 FROM test2 EMIT CHANGES;";
    final PlanNode logicalPlan = buildLogicalPlan(selectQuery);
    initialSchemaKTable = new SchemaKTable(
        mockKTable,
        buildSourceStep(logicalPlan.getTheSourceNode().getSchema()),
        keyFormat,
        keySerde,
        logicalPlan.getTheSourceNode().getKeyField(),
        new ArrayList<>(),
        SchemaKStream.Type.SOURCE,
        ksqlConfig,
        functionRegistry
    );

    final List<Expression> groupByExpressions = Arrays.asList(TEST_2_COL_2, TEST_2_COL_1);

    // Call groupBy and extract the captured mapper
    initialSchemaKTable.groupBy(valueFormat, groupByExpressions, childContextStacker, queryBuilder);
    verify(mockKTable, mockKGroupedTable);
    final KeyValueMapper keySelector = capturedKeySelector.getValue();
    final GenericRow value = new GenericRow(Arrays.asList("key", 0, 100, "foo", "bar"));
    final KeyValue<String, GenericRow> keyValue =
        (KeyValue<String, GenericRow>) keySelector.apply("key", value);

    // Validate that the captured mapper produces the correct key
    assertThat(keyValue.key, equalTo(StructKeyUtil.asStructKey("bar|+|foo")));
    assertThat(keyValue.value, equalTo(value));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformTableToTableLeftJoin() {
    expect(mockKTable.leftJoin(eq(secondSchemaKTable.getKtable()),
                               anyObject(SchemaKStream.KsqlValueJoiner.class)))
        .andReturn(EasyMock.niceMock(KTable.class));

    replay(mockKTable);

    final SchemaKStream joinedKStream = firstSchemaKTable
        .leftJoin(
            secondSchemaKTable,
            joinSchema,
            validKeyField,
            childContextStacker);

    verify(mockKTable);

    assertThat(joinedKStream, instanceOf(SchemaKTable.class));
    assertEquals(SchemaKStream.Type.JOIN, joinedKStream.type);
    assertEquals(joinSchema, joinedKStream.getSchema());
    assertThat(joinedKStream.getKeyField(), is(validKeyField));
    assertEquals(Arrays.asList(firstSchemaKTable, secondSchemaKTable),
                 joinedKStream.sourceSchemaKStreams);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformTableToTableInnerJoin() {
    expect(mockKTable.join(eq(secondSchemaKTable.getKtable()),
                           anyObject(SchemaKStream.KsqlValueJoiner.class)))
        .andReturn(EasyMock.niceMock(KTable.class));

    replay(mockKTable);

    final SchemaKStream joinedKStream = firstSchemaKTable
        .join(secondSchemaKTable, joinSchema,
            validKeyField,
            childContextStacker);

    verify(mockKTable);

    assertThat(joinedKStream, instanceOf(SchemaKTable.class));
    assertEquals(SchemaKStream.Type.JOIN, joinedKStream.type);
    assertEquals(joinSchema, joinedKStream.getSchema());
    assertThat(joinedKStream.getKeyField(), is(validKeyField));
    assertEquals(Arrays.asList(firstSchemaKTable, secondSchemaKTable),
                 joinedKStream.sourceSchemaKStreams);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldPerformTableToTableOuterJoin() {
    expect(mockKTable.outerJoin(eq(secondSchemaKTable.getKtable()),
                                anyObject(SchemaKStream.KsqlValueJoiner.class)))
        .andReturn(EasyMock.niceMock(KTable.class));

    replay(mockKTable);

    final SchemaKStream joinedKStream = firstSchemaKTable
        .outerJoin(secondSchemaKTable, joinSchema,
            validKeyField,
            childContextStacker);

    verify(mockKTable);

    assertThat(joinedKStream, instanceOf(SchemaKTable.class));
    assertEquals(SchemaKStream.Type.JOIN, joinedKStream.type);
    assertEquals(joinSchema, joinedKStream.getSchema());
    assertThat(joinedKStream.getKeyField(), is(validKeyField));
    assertEquals(Arrays.asList(firstSchemaKTable, secondSchemaKTable),
                 joinedKStream.sourceSchemaKStreams);
  }

  interface Join {
    SchemaKTable join(
        SchemaKTable schemaKTable,
        LogicalSchema joinSchema,
        KeyField keyField,
        QueryContext.Stacker contextStacker
    );
  }

  @Test
  public void shouldBuildStepForTableTableJoin() {
    final KTable resultTable = EasyMock.niceMock(KTable.class);
    expect(mockKTable.outerJoin(
        eq(secondSchemaKTable.getKtable()),
        anyObject(SchemaKStream.KsqlValueJoiner.class))
    ).andReturn(resultTable);
    expect(mockKTable.join(
        eq(secondSchemaKTable.getKtable()),
        anyObject(SchemaKStream.KsqlValueJoiner.class))
    ).andReturn(resultTable);
    expect(mockKTable.leftJoin(
        eq(secondSchemaKTable.getKtable()),
        anyObject(SchemaKStream.KsqlValueJoiner.class))
    ).andReturn(resultTable);
    replay(mockKTable);

    final List<Pair<JoinType, Join>> cases = ImmutableList.of(
        Pair.of(JoinType.LEFT, firstSchemaKTable::leftJoin),
        Pair.of(JoinType.INNER, firstSchemaKTable::join),
        Pair.of(JoinType.OUTER, firstSchemaKTable::outerJoin)
    );
    for (final Pair<JoinType, Join> testCase : cases) {
      final SchemaKTable result =
          testCase.right.join(secondSchemaKTable, joinSchema, validKeyField, childContextStacker);
      assertThat(
          result.getSourceTableStep(),
          equalTo(
              ExecutionStepFactory.tableTableJoin(
                  childContextStacker,
                  testCase.left,
                  firstSchemaKTable.getSourceTableStep(),
                  secondSchemaKTable.getSourceTableStep(),
                  joinSchema
              )
          )
      );
    }
  }

  @Test
  public void shouldUpdateKeyIfRenamed() {
    // Given:
    final List<SelectExpression> selectExpressions = givenInitialKTableOf(
        "SELECT col0 as NEWKEY, col2, col3 FROM test1 EMIT CHANGES;");

    // When:
    final SchemaKStream result = initialSchemaKTable
        .select(selectExpressions, childContextStacker, queryBuilder);

    assertThat(result.getKeyField(),
        is(KeyField.of("NEWKEY", Column.of("NEWKEY", SqlTypes.BIGINT))));
  }

  @Test
  public void shouldUpdateKeyIfRenamedViaFullyQualifiedName() {
    // Given:
    final List<SelectExpression> selectExpressions = givenInitialKTableOf(
        "SELECT test1.col0 as NEWKEY, col2, col3 FROM test1 EMIT CHANGES;");

    // When:
    final SchemaKStream result = initialSchemaKTable
        .select(selectExpressions, childContextStacker, queryBuilder);

    // Then:
    assertThat(result.getKeyField(),
        is(KeyField.of("NEWKEY", Column.of("NEWKEY", SqlTypes.BIGINT))));
  }

  @Test
  public void shouldUpdateKeyIfRenamedAndSourceIsAliased() {
    // Given:
    final List<SelectExpression> selectExpressions = givenInitialKTableOf(
        "SELECT t.col0 as NEWKEY, col2, col3 FROM test1 t EMIT CHANGES;");

    // When:
    final SchemaKStream result = initialSchemaKTable
        .select(selectExpressions, childContextStacker, queryBuilder);

    // Then:
    assertThat(result.getKeyField(),
        is(KeyField.of("NEWKEY", Column.of("NEWKEY", SqlTypes.BIGINT))));
  }

  @Test
  public void shouldPreserveKeyOnSelectStar() {
    // Given:
    final List<SelectExpression> selectExpressions = givenInitialKTableOf(
        "SELECT * FROM test1 EMIT CHANGES;");

    // When:
    final SchemaKStream result = initialSchemaKTable
        .select(selectExpressions, childContextStacker, queryBuilder);

    // Then:
    assertThat(result.getKeyField(), KeyFieldMatchers.hasName("COL0"));
    assertThat(result.getKeyField(), hasLegacyName(initialSchemaKTable.keyField.legacy().map(LegacyField::name)));
    assertThat(result.getKeyField(), hasLegacyType(initialSchemaKTable.keyField.legacy().map(LegacyField::type)));
  }

  @Test
  public void shouldUpdateKeyIfMovedToDifferentIndex() {
    // Given:
    final List<SelectExpression> selectExpressions = givenInitialKTableOf(
        "SELECT col2, col0, col3 FROM test1 EMIT CHANGES;");

    // When:
    final SchemaKStream result = initialSchemaKTable
        .select(selectExpressions, childContextStacker, queryBuilder);

    // Then:
    assertThat(result.getKeyField(),
        Matchers.equalTo(KeyField.of("COL0", Column.of("COL0", SqlTypes.BIGINT))));
  }

  @Test
  public void shouldDropKeyIfNotSelected() {
    // Given:
    final List<SelectExpression> selectExpressions = givenInitialKTableOf(
        "SELECT col2, col3 FROM test1 EMIT CHANGES;");

    // When:
    final SchemaKStream result = initialSchemaKTable
        .select(selectExpressions, childContextStacker, queryBuilder);

    // Then:
    assertThat(result.getKeyField(), is(KeyField.none()));
  }

  @Test
  public void shouldHandleSourceWithoutKey() {
    // Given:
    final List<SelectExpression> selectExpressions = givenInitialKTableOf(
        "SELECT * FROM test4 EMIT CHANGES;");

    // When:
    final SchemaKStream result = initialSchemaKTable
        .select(selectExpressions, childContextStacker, queryBuilder);

    // Then:
    assertThat(result.getKeyField(), is(KeyField.none()));
  }

  @Test
  public void shouldSetKeyOnGroupBySingleExpressionThatIsInProjection() {
    // Given:
    givenInitialKTableOf("SELECT * FROM test2 EMIT CHANGES;");
    final List<Expression> groupByExprs =  ImmutableList.of(TEST_2_COL_1);

    // When:
    final SchemaKGroupedStream result = initialSchemaKTable
        .groupBy(valueFormat, groupByExprs, childContextStacker, queryBuilder);

    // Then:
    assertThat(result.getKeyField(),
        is(KeyField.of("TEST2.COL1", LegacyField.notInSchema("TEST2.COL1", SqlTypes.STRING))));
  }

  private static LogicalSchema getJoinSchema(
      final LogicalSchema leftSchema,
      final LogicalSchema rightSchema
  ) {
    final LogicalSchema.Builder schemaBuilder = LogicalSchema.builder();
    final String leftAlias = "left";
    final String rightAlias = "right";
    for (final Column field : leftSchema.value()) {
      schemaBuilder.valueColumn(Column.of(leftAlias, field.name(), field.type()));
    }

    for (final Column field : rightSchema.value()) {
      schemaBuilder.valueColumn(Column.of(rightAlias, field.name(), field.type()));
    }
    return schemaBuilder.build();
  }

  private List<SelectExpression> givenInitialKTableOf(final String selectQuery) {
    final PlanNode logicalPlan = AnalysisTestUtil.buildLogicalPlan(
        ksqlConfig,
        selectQuery,
        metaStore
    );

    initialSchemaKTable = new SchemaKTable(
        kTable,
        buildSourceStep(logicalPlan.getTheSourceNode().getSchema()),
        keyFormat,
        keySerde,
        logicalPlan.getTheSourceNode().getKeyField(),
        new ArrayList<>(),
        SchemaKStream.Type.SOURCE,
        ksqlConfig,
        functionRegistry
    );

    rowSerde = GenericRowSerDe.from(
        FormatInfo.of(Format.JSON, Optional.empty()),
        PersistenceSchema.from(initialSchemaKTable.getSchema().valueConnectSchema(), false),
        null,
        () -> null,
        "test",
        processingLogContext);

    final ProjectNode projectNode = (ProjectNode) logicalPlan.getSources().get(0);
    return projectNode.getProjectSelectExpressions();
  }

  private PlanNode buildLogicalPlan(final String query) {
    return AnalysisTestUtil.buildLogicalPlan(ksqlConfig, query, metaStore);
  }
}
