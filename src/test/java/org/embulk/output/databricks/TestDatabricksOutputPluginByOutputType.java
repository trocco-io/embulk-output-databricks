package org.embulk.output.databricks;

import static org.embulk.output.databricks.util.ConfigUtil.*;
import static org.embulk.output.databricks.util.FixedColumnNameTableUtil.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.ZoneId;
import org.junit.Test;

public class TestDatabricksOutputPluginByOutputType extends AbstractTestDatabricksOutputPlugin {
  @Test
  public void testString() throws IOException {
    testOutputValues(
        new TestTypeSet("100", "string", "string", "100"),
        new TestTypeSet("100", "string", "bigint", 100L));
  }

  @Test
  public void testLong() throws IOException {
    testOutputValues(
        new TestTypeSet("100", "long", "string", "100"),
        new TestTypeSet("100", "long", "bigint", 100L),
        new TestTypeSet("100", "long", "int", 100),
        new TestTypeSet("100", "long", "smallint", 100),
        new TestTypeSet("100", "long", "tinyint", 100),
        new TestTypeSet("100", "long", "double", 100.0),
        new TestTypeSet("100", "long", "float", 100.0F),
        new TestTypeSet("100", "long", "decimal(4,1)", new BigDecimal("100.0")),
        new TestTypeSet("100", "long", "boolean", true),
        new TestTypeSet("0", "long", "boolean", false));
  }

  @Test
  public void testDouble() throws IOException {
    testOutputValues(
        new TestTypeSet("12.5", "double", "string", "12.5"),
        new TestTypeSet("12.5", "double", "bigint", 13L),
        new TestTypeSet("12.5", "double", "int", 13),
        new TestTypeSet("12.5", "double", "smallint", 13),
        new TestTypeSet("12.5", "double", "tinyint", 13),
        new TestTypeSet("12.5", "double", "double", 12.5),
        new TestTypeSet("12.5", "double", "float", 12.5F),
        new TestTypeSet("12.5", "double", "decimal(4,1)", new BigDecimal("12.5")),
        new TestTypeSet("12.5", "double", "boolean", true),
        new TestTypeSet("0", "double", "boolean", false));
  }

  @Test
  public void testBoolean() throws IOException {
    testOutputValues(
        new TestTypeSet("true", "boolean", "string", "true"),
        new TestTypeSet("true", "boolean", "double", 1.0),
        new TestTypeSet("true", "boolean", "int", 1),
        new TestTypeSet("true", "boolean", "boolean", true));
  }

  @Test
  public void testTimestamp() throws IOException {
    testOutputValues(
        new TestTypeSet(
            "2000-01-02 03:04:05.00 UTC", "timestamp", "string", "2000-01-02 03:04:05.000000"),
        new TestTypeSet(
            "2000-01-02 03:04:05.00 UTC",
            "timestamp",
            "timestamp",
            Timestamp.valueOf("2000-01-02 03:04:05.0")),
        new TestTypeSet(
            "2000-01-02 03:04:05.00 UTC", "timestamp", "date", Date.valueOf("2000-01-02")),
        new TestTimestampSet(
            "2000-01-02 03:04:05.00 UTC",
            "%Y-%m-%d",
            null,
            Timestamp.valueOf("2000-01-02 00:00:00.0")),
        new TestTimestampSet(
            "2000-01-02 03:04:05.00 UTC", "%Y", null, Timestamp.valueOf("2000-01-01 00:00:00.0")),
        new TestTimestampSet(
            "2000-01-02 20:04:05.00 UTC",
            "%Y-%m-%d",
            ZoneId.of("Asia/Tokyo"),
            Timestamp.valueOf("2000-01-03 00:00:00.0")),
        new TestTimestampToStringSet(
            "2000-01-02 03:04:05.00 UTC", "%Y/%m/%d %H-%M-%S", null, "2000/01/02 03-04-05"),
        new TestTimestampToStringSet(
            "2000-01-02 03:04:05.00 UTC",
            null,
            ZoneId.of("Asia/Tokyo"),
            "2000-01-02 12:04:05.000000"),
        new TestTimestampToStringSet(
            "2000-01-02 03:04:05.00 UTC",
            "%Y/%m/%d %H-%M-%S",
            ZoneId.of("Asia/Tokyo"),
            "2000/01/02 12-04-05"));
  }

  @Test
  public void testJson() throws IOException {
    testOutputValues(
        new TestTypeSet("{\"key\":\"value\"}", "json", "string", "{\"key\":\"value\"}"));
  }

  protected class TestTypeSet extends TestSet {
    TestTypeSet(String value, String inputType, String outputType, Object expected) {
      super(value, inputType, outputType, null, null, null, expected);
    }
  }

  protected class TestTimestampSet extends TestSet {
    TestTimestampSet(
        String value, String outputTimestampFormat, ZoneId outputTimezone, Object expected) {
      super(
          value,
          "timestamp",
          "timestamp",
          "string",
          outputTimestampFormat,
          outputTimezone,
          expected);
    }
  }

  protected class TestTimestampToStringSet extends TestSet {
    TestTimestampToStringSet(
        String value, String outputTimestampFormat, ZoneId outputTimezone, Object expected) {
      super(value, "timestamp", "string", null, outputTimestampFormat, outputTimezone, expected);
    }
  }
}
