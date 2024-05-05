package org.embulk.output.databricks;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.ZoneId;
import org.junit.Test;

public class TestDatabricksOutputPluginByTimestamp extends AbstractTestDatabricksOutputPlugin {
  @Test
  public void testTimestamp() throws IOException {
    testOutputValues(
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
            Timestamp.valueOf("2000-01-03 00:00:00.0")));
  }

  @Test
  public void testTimestampToString() throws IOException {
    testOutputValues(
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
