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

package io.confluent.ksql.function.udaf;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.KsqlAggregateFunction;
import org.apache.kafka.streams.kstream.Aggregator;

import java.util.Map;

public class KudafSubtractor implements Aggregator<String, GenericRow, GenericRow> {
  private Map<Integer, KsqlAggregateFunction> aggValToAggFunctionMap;
  private Map<Integer, Integer> aggValToValColumnMap;

  public KudafSubtractor(Map<Integer, KsqlAggregateFunction> aggValToAggFunctionMap, Map<Integer,
      Integer> aggValToValColumnMap) {
    this.aggValToAggFunctionMap = aggValToAggFunctionMap;
    this.aggValToValColumnMap = aggValToValColumnMap;
  }

  @SuppressWarnings("unchecked")
  @Override
  public GenericRow apply(String s, GenericRow rowValue, GenericRow aggRowValue) {
    aggValToValColumnMap.forEach((key, value) ->
        aggRowValue.getColumns().set(key, rowValue.getColumns().get(value))
    );

    aggValToAggFunctionMap.forEach((key, value) ->
        aggRowValue.getColumns().set(key,
            value.subtract(rowValue.getColumns().get(value.getArgIndexInValue()),
                aggRowValue.getColumns().get(key)))
    );
    return aggRowValue;
  }
}
