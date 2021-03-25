/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.query;

import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.physical.PhysicalPlan;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import io.confluent.ksql.util.TransientQueryMetadata;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyDescription.Processor;

public class KafkaStreamsQueryValidator implements QueryValidator {
  private final ServiceContext serviceContext;
  private final MetaStore metaStore;

  public KafkaStreamsQueryValidator(
      final ServiceContext serviceContext,
      final MetaStore metaStore
  ) {
    this.serviceContext = Objects.requireNonNull(serviceContext, "serviceContext");
    this.metaStore = Objects.requireNonNull(metaStore, "metaStore");
  }

  @Override
  public void validateQuery(
      final SessionConfig config,
      final PhysicalPlan physicalPlan,
      final Collection<QueryMetadata> runningQueries,
  ) {
    validateCacheBytesUsage(
        runningQueries.stream()
            .filter(q -> q instanceof PersistentQueryMetadata)
            .collect(Collectors.toList()),
        config,
        config.getConfig(false)
            .getLong(KsqlConfig.KSQL_TOTAL_CACHE_MAX_BYTES_BUFFERING)
    );
  }

  @Override
  public void validateTransientQuery(
      final SessionConfig config,
      final PhysicalPlan physicalPlan,
      final Collection<QueryMetadata> runningQueries
  ) {
    validateCacheBytesUsage(
        runningQueries.stream()
            .filter(q -> q instanceof TransientQueryMetadata)
            .collect(Collectors.toList()),
        config,
        config.getConfig(false)
            .getLong(KsqlConfig.KSQL_TOTAL_CACHE_MAX_BYTES_BUFFERING_TRANSIENT)
    );
  }

  private void validateMaxStateStores(
      final Collection<QueryMetadata> running,
      final SessionConfig config,
      final int limit
  ) {

  }

  private void validateCacheBytesUsage(
      final Collection<QueryMetadata> running,
      final SessionConfig config,
      final long limit
  ) {
    if (limit < 0) {
      return;
    }
    final long configured = getCacheMaxBytesBuffering(config);
    final long usedByRunning = running.stream()
        .mapToLong(r -> new StreamsConfig(r.getStreamsProperties())
                .getLong(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG))
        .sum();
    if (configured + usedByRunning > limit) {
      throw new KsqlException(String.format(
          "Configured cache usage (cache.max.bytes.buffering=%d) would put usage over the "
              + "configured limit (%d). Current usage is %d",
          configured, usedByRunning, limit
      ));
    }
  }

  private long getCacheMaxBytesBuffering(final SessionConfig config) {
    return getDummyStreamsConfig(config).getLong(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG);
  }

  private StreamsConfig getDummyStreamsConfig(final SessionConfig config) {
    // hack to get at default config value
    final Map<String, Object> properties = new HashMap<>();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "dummy.app.id");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy.bootstrap");
    properties.putAll(config.getConfig(true).getKsqlStreamConfigProps());
    return new StreamsConfig(properties);
  }

  private int estimateNumStoreInstancesForQuery(final QueryMetadata query) {
    final TopologyDescription topologyDescription = query.getTopology().describe();
    final Set<String> stores = topologyDescription.subtopologies().stream()
        .flatMap(s -> s.nodes().stream())
        .filter(n -> n instanceof Processor)
        .flatMap(n -> ((Processor) n).stores().stream())
        .collect(Collectors.toSet());
    final int partitions = query.getSourceNames().stream()
        .mapToInt(this::partitionsForSource)
        .max()
        .orElseThrow(() -> new IllegalStateException("query must have at least one source"));
    return partitions * stores.size();
  }

  private int partitionsForSource(final SourceName name) {
    final KafkaTopicClient c = serviceContext.getTopicClient();
    TopicDescription desc = c.describeTopic(metaStore.getSource(name).getKafkaTopicName());
    return desc.partitions().size();
  }
}
