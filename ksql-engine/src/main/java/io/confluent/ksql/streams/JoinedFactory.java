/*
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

package io.confluent.ksql.streams;

import io.confluent.ksql.util.KsqlConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Joined;

public interface JoinedFactory {
  <K, V, V0> Joined<K, V, V0> create(
      Serde<K> keySerde,
      Serde<V> leftSerde,
      Serde<V0> rightSerde,
      String name);

  static JoinedFactory create(final KsqlConfig ksqlConfig) {
    return create(ksqlConfig, new RealStreamsStatics());
  }

  static JoinedFactory create(final KsqlConfig ksqlConfig, final StreamsStatics streamsStatics) {
    if (StreamsUtil.useProvidedName(ksqlConfig)) {
      return streamsStatics::joinedWith;
    }
    return new JoinedFactory() {
      @Override
      public <K, V, V0> Joined<K, V, V0> create(
          final Serde<K> keySerde,
          final Serde<V> leftSerde,
          final Serde<V0> rightSerde,
          final String name) {
        return streamsStatics.joinedWith(keySerde, leftSerde, rightSerde, null);
      }
    };
  }
}
