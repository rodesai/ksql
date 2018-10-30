package io.confluent.ksql.processing.log;

import io.confluent.ksql.GenericRow;
import java.util.Base64;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;

public class ProcessingLog {
  private Logger inner;

  ProcessingLog(final Logger inner) {
    this.inner = inner;
  }

  public void error(final Supplier<GenericRow> msg) {
    inner.error("{}", msg);
  }

  public void info(final Supplier<GenericRow> msg) {
    inner.info("{}", msg);
  }

  public void debug(final Supplier<GenericRow> msg) {
    inner.debug("{}", msg);
  }

  // TODO: separate this!
  private static final Schema DESERIALIZATION_ERROR_SCHEMA = SchemaBuilder.struct()
      .field("message", Schema.OPTIONAL_STRING_SCHEMA)
      .field("record", Schema.OPTIONAL_STRING_SCHEMA)
      .optional()
      .build();

  private static final Schema  RECORD_PROCESSING_TRACE = SchemaBuilder.struct()
      .field("nodeId", Schema.OPTIONAL_STRING_SCHEMA)
      .field("message", Schema.OPTIONAL_STRING_SCHEMA)
      .field("record", Schema.OPTIONAL_STRING_SCHEMA)
      .optional()
      .build();

  public static final Schema PROCESSING_LOG_SCHEMA = SchemaBuilder.struct()
      .field("type", Schema.OPTIONAL_INT32_SCHEMA)
      .field("deserializationError", DESERIALIZATION_ERROR_SCHEMA)
      .field("recordProcessingTrace", RECORD_PROCESSING_TRACE)
      .build();

  public static class ProcessingLogRowSupplier implements Supplier<GenericRow> {
    final Supplier<GenericRow> rowSupplier;
    GenericRow row = null;

    public ProcessingLogRowSupplier(Supplier<GenericRow> rowSupplier) {
      this.rowSupplier = rowSupplier;
    }

    @Override
    public GenericRow get() {
      if (row == null) {
        row = rowSupplier.get();
      }
      return row;
    }

    @Override
    public String toString() {
      return get().toString();
    }
  }

  private static GenericRow structToGenericRow(final Struct struct) {
    return new GenericRow(
        struct.schema().fields().stream().map(f -> struct.get(f)).collect(Collectors.toList()));
  }

  public static Supplier<GenericRow> deserializationErrorMsg(
      final Exception exception,
      final byte[] record) {
    return new ProcessingLogRowSupplier(
        () -> {
          final Struct struct = new Struct(PROCESSING_LOG_SCHEMA);
          final Struct deserializationError = new Struct(DESERIALIZATION_ERROR_SCHEMA);
          deserializationError.put("message", exception.getMessage());
          deserializationError.put("record", Base64.getEncoder().encodeToString(record));
          struct.put("deserializationError", deserializationError);
          return structToGenericRow(struct);
        }
    );
  }

  public static Supplier<GenericRow> recordProcessingTraceMsg(
      final String nodeId,
      final String msg,
      final Schema schema,
      final GenericRow row) {
    return null;
  }
}
