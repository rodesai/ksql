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

package io.confluent.ksql.function.udaf.topkdistinct;

import org.apache.kafka.connect.data.Schema;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class StringTopkDistinctKudafTest {

  ArrayList<String> valueArray;
  private final TopkDistinctKudaf<String> stringTopkDistinctKudaf
      = TopKDistinctTestUtils.getTopKDistinctKudaf(3, Schema.STRING_SCHEMA);

  @Before
  public void setup() {
    valueArray = new ArrayList(Arrays.asList("10", "30", "45", "10", "50", "60", "20", "60", "80", "35",
                                             "25","60", "80"));

  }

  @Test
  public void shouldAggregateTopK() {
    ArrayList<String> currentVal = new ArrayList();
    for (String d: valueArray) {
      currentVal = stringTopkDistinctKudaf.aggregate(d, currentVal);
    }

    assertThat("Invalid results.", currentVal, equalTo(new ArrayList(Arrays.asList("80", "60",
                                                                                   "50"))));
  }

  @Test
  public void shouldAggregateTopKWithLessThanKValues() {
    ArrayList<String> currentVal = new ArrayList();
    currentVal = stringTopkDistinctKudaf.aggregate("80", currentVal);

    assertThat("Invalid results.", currentVal, equalTo(new ArrayList(Arrays.asList("80"))));
  }

  @Test
  public void shouldMergeTopK() {
    ArrayList<String> array1 = new ArrayList(Arrays.asList("50", "45", "25"));
    ArrayList<String> array2 = new ArrayList(Arrays.asList("60", "50", "48"));

    assertThat("Invalid results.", stringTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        new ArrayList(Arrays.asList("60", "50", "48"))));
  }

  @Test
  public void shouldMergeTopKWithNulls() {
    ArrayList<String> array1 = new ArrayList(Arrays.asList("50", "45"));
    ArrayList<String> array2 = new ArrayList(Arrays.asList("60"));

    assertThat("Invalid results.", stringTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        new ArrayList(Arrays.asList("60", "50", "45"))));
  }

  @Test
  public void shouldMergeTopKWithNullsDuplicates() {
    ArrayList<String> array1 = new ArrayList(Arrays.asList("50", "45"));
    ArrayList<String> array2 = new ArrayList(Arrays.asList("60", "50"));

    assertThat("Invalid results.", stringTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        new ArrayList(Arrays.asList("60", "50", "45"))));
  }

  @Test
  public void shouldMergeTopKWithMoreNulls() {
    ArrayList<String> array1 = new ArrayList(Arrays.asList("60"));
    ArrayList<String> array2 = new ArrayList(Arrays.asList("60"));

    assertThat("Invalid results.", stringTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        new ArrayList(Arrays.asList("60"))));
  }
}