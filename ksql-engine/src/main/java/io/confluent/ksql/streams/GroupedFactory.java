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
import org.apache.kafka.streams.kstream.Grouped;

public interface GroupedFactory {
  <K, V> Grouped<K, V> create(String name, Serde<K> keySerde, Serde<V> valSerde);

  static GroupedFactory create(final KsqlConfig ksqlConfig) {
    return create(ksqlConfig, new RealStreamsStatics());
  }

  static GroupedFactory create(final KsqlConfig ksqlConfig, final StreamsStatics streamsStatics) {
    if (StreamsUtil.useProvidedName(ksqlConfig)) {
      return streamsStatics::groupedWith;
    }
    return new GroupedFactory() {
      @Override
      public <K, V> Grouped<K, V> create(
          final String name,
          final Serde<K> keySerde,
          final Serde<V> valSerde) {
        return streamsStatics.groupedWith(null, keySerde, valSerde);
      }
    };
  }
}
