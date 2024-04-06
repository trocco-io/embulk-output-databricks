package org.embulk.output.databricks;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import org.junit.Test;

public class TestDatabricksOutputPluginByOutputValueType
    extends AbstractTestDatabricksOutputPlugin {
  @Test
  public void testAllValueType() throws IOException {
    // `byte`, `short`, `int`, `long`, `double`, `float`, `boolean`, `string`, `nstring`, `date`,
    // `time`, `timestamp`, `decimal`, `json`, `null`, `pass`
    testOutputValues(
        create("100", "long", "byte", 100L),
        create("100", "long", "short", 100L),
        create("100", "long", "int", 100L),
        create("100", "long", "long", 100L),
        create("100", "double", "double", 100.0),
        create("100", "double", "float", 100.0),
        create("true", "boolean", "boolean", true),
        create("100", "string", "string", "100"),
        create("100", "string", "nstring", "100"),
        create("2000-01-02 03:04:05.00 UTC", "timestamp", "date", Date.valueOf("2000-01-02")),
        create(
            "2000-01-02 03:04:05.00 UTC",
            "timestamp",
            "timestamp",
            Timestamp.valueOf("2000-01-02 03:04:05.0")),
        create("12.5", "double", "decimal", 12.5),
        create("{\"key\":\"value\"}", "json", "json", "{\"key\":\"value\"}"),
        create(null, "string", "null", null),
        create("pass", "string", "pass", "pass"));
  }

  private TestSet create(String value, String inputType, String outputValueType, Object expected) {
    return new TestSet(value, inputType, null, outputValueType, null, null, expected);
  }
}
