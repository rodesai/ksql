package io.confluent.ksql.query;

import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;

public interface RunningQueries {
  void validate(final QueryMetadata query);

  void register(final QueryMetadata query);

  void unregister(final QueryId queryId);

  Optional<PersistentQueryMetadata> getPersistentQuery(final QueryId queryId);

  Map<QueryId, PersistentQueryMetadata> getPersistentQueries();

  Set<QueryId> getQueriesWithSink(final SourceName sourceName);

  Set<QueryId> getInsertQueries(
      final SourceName sourceName,
      final BiPredicate<SourceName, PersistentQueryMetadata> filterQueries
  );

  void createSandbox();
}
