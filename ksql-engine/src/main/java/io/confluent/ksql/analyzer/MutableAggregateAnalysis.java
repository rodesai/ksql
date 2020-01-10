/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.analyzer;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.execution.expression.tree.AbstractColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class MutableAggregateAnalysis implements AggregateAnalysis {

  private final List<AbstractColumnReferenceExp> requiredColumns = new ArrayList<>();
  private final Map<Expression, Set<AbstractColumnReferenceExp>> nonAggSelectExpressions
      = new HashMap<>();
  private final Set<AbstractColumnReferenceExp> nonAggHavingFields = new HashSet<>();
  private final Set<AbstractColumnReferenceExp> aggSelectFields = new HashSet<>();
  private final List<Expression> finalSelectExpressions = new ArrayList<>();
  private final List<Expression> aggregateFunctionArguments = new ArrayList<>();
  private final List<FunctionCall> aggFunctions = new ArrayList<>();
  private Optional<Expression> havingExpression = Optional.empty();


  @Override
  public List<Expression> getAggregateFunctionArguments() {
    return Collections.unmodifiableList(aggregateFunctionArguments);
  }

  @Override
  public List<AbstractColumnReferenceExp> getRequiredColumns() {
    return Collections.unmodifiableList(requiredColumns);
  }

  @Override
  public Map<Expression, Set<AbstractColumnReferenceExp>> getNonAggregateSelectExpressions() {
    return Collections.unmodifiableMap(nonAggSelectExpressions);
  }

  @Override
  public Set<AbstractColumnReferenceExp> getAggregateSelectFields() {
    return Collections.unmodifiableSet(aggSelectFields);
  }

  @Override
  public Set<AbstractColumnReferenceExp> getNonAggregateHavingFields() {
    return Collections.unmodifiableSet(nonAggHavingFields);
  }

  @Override
  public List<FunctionCall> getAggregateFunctions() {
    return Collections.unmodifiableList(aggFunctions);
  }

  @Override
  public List<Expression> getFinalSelectExpressions() {
    return Collections.unmodifiableList(finalSelectExpressions);
  }

  @Override
  public Optional<Expression> getHavingExpression() {
    return havingExpression;
  }

  void setHavingExpression(final Expression havingExpression) {
    this.havingExpression = Optional.of(havingExpression);
  }

  void addAggregateFunctionArgument(final Expression argument) {
    aggregateFunctionArguments.add(argument);
  }

  void addAggFunction(final FunctionCall functionCall) {
    aggFunctions.add(functionCall);
  }

  void addAggregateSelectField(
      final Set<AbstractColumnReferenceExp> fields
  ) {
    aggSelectFields.addAll(fields);
  }

  void addNonAggregateSelectExpression(
      final Expression selectExpression,
      final Set<AbstractColumnReferenceExp> referencedFields
  ) {
    nonAggSelectExpressions.put(selectExpression, ImmutableSet.copyOf(referencedFields));
  }

  void addNonAggregateHavingField(final AbstractColumnReferenceExp node) {
    nonAggHavingFields.add(node);
  }

  void addRequiredColumn(final AbstractColumnReferenceExp node) {
    if (!requiredColumns.contains(node)) {
      requiredColumns.add(node);
    }
  }

  void addFinalSelectExpression(final Expression expression) {
    finalSelectExpressions.add(expression);
  }
}
