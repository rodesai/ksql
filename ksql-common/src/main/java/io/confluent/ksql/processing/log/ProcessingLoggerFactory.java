package io.confluent.ksql.processing.log;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.Collection;
import java.util.List;
import org.slf4j.LoggerFactory;

public class ProcessingLoggerFactory {
  private static final String PROCESSING_LOGGER_NAME_PREFIX = "processing.";
  private static final List<String> LOGGERS = Lists.newCopyOnWriteArrayList();

  public static ProcessingLog getLogger(final Class<?> clazz) {
    final String name = PROCESSING_LOGGER_NAME_PREFIX + clazz.getName();
    LOGGERS.add(name);
    return new ProcessingLog(LoggerFactory.getLogger(name));
  }

  public static Collection<String> getLoggers() {
    return ImmutableList.copyOf(LOGGERS);
  }
}
