package org.embulk.output.databricks;

import static org.embulk.output.databricks.util.ConfigUtil.*;
import static org.embulk.output.databricks.util.ConnectionUtil.quotedDstTableName;
import static org.embulk.output.databricks.util.ConnectionUtil.runQuery;
import static org.embulk.output.databricks.util.FixedColumnNameTableUtil.*;
import static org.embulk.output.databricks.util.IOUtil.createInputFile;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.embulk.config.ConfigSource;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.junit.Assert;
import org.junit.Test;

public class TestDatabricksOutputPluginByOutputType extends AbstractTestDatabricksOutputPlugin {
  @Test
  public void testString() throws IOException {
    testOutputTypes(
        new TestSet("100", "string", "string", "100"),
        new TestSet("100", "string", "bigint", 100L));
  }

  @Test
  public void testLong() throws IOException {
    testOutputTypes(
        new TestSet("100", "long", "string", "100"),
        new TestSet("100", "long", "bigint", 100L),
        new TestSet("100", "long", "int", 100),
        new TestSet("100", "long", "smallint", 100),
        new TestSet("100", "long", "tinyint", 100),
        new TestSet("100", "long", "double", 100.0),
        new TestSet("100", "long", "float", 100.0F),
        new TestSet("100", "long", "decimal(4,1)", new BigDecimal("100.0")),
        new TestSet("100", "long", "boolean", true),
        new TestSet("0", "long", "boolean", false));
  }

  @Test
  public void testDouble() throws IOException {
    testOutputTypes(
        new TestSet("12.5", "double", "string", "12.5"),
        new TestSet("12.5", "double", "bigint", 13L),
        new TestSet("12.5", "double", "int", 13),
        new TestSet("12.5", "double", "smallint", 13),
        new TestSet("12.5", "double", "tinyint", 13),
        new TestSet("12.5", "double", "double", 12.5),
        new TestSet("12.5", "double", "float", 12.5F),
        new TestSet("12.5", "double", "decimal(4,1)", new BigDecimal("12.5")),
        new TestSet("12.5", "double", "boolean", true),
        new TestSet("0", "double", "boolean", false));
  }

  @Test
  public void testBoolean() throws IOException {
    testOutputTypes(
        new TestSet("true", "boolean", "string", "true"),
        new TestSet("true", "boolean", "double", 1.0),
        new TestSet("true", "boolean", "int", 1),
        new TestSet("true", "boolean", "boolean", true));
  }

  @Test
  public void testTimestamp() throws IOException {
    testOutputTypes(
        new TestSet(
            "2000-01-02 03:04:05.00 UTC", "timestamp", "string", "2000-01-02 03:04:05.000000"),
        new TestSet(
            "2000-01-02 03:04:05.00 UTC",
            "timestamp",
            "timestamp",
            Timestamp.valueOf("2000-01-02 03:04:05.0")),
        new TestSet("2000-01-02 03:04:05.00 UTC", "timestamp", "date", Date.valueOf("2000-01-02")));
  }

  @Test
  public void testJson() throws IOException {
    testOutputTypes(new TestSet("{\"key\":\"value\"}", "json", "string", "{\"key\":\"value\"}"));
  }

  private static class TestSet {
    final String value;
    final String inputType;
    final String outputType;
    final Object expected;

    TestSet(String value, String inputType, String outputType, Object expected) {
      this.value = value;
      this.inputType = inputType;
      this.outputType = outputType;
      this.expected = expected;
    }
  }

  private void testOutputTypes(TestSet... testSets) throws IOException {
    ConfigSource configSource = createPluginConfigSource(AbstractJdbcOutputPlugin.Mode.INSERT);
    String quotedDstTableName = quotedDstTableName(configSource);
    for (int i = 0; i < testSets.length; i++) {
      setColumnOption(configSource, columnName(i), testSets[i].outputType);
    }

    String header = csvHeader(Arrays.stream(testSets).map(x -> x.inputType).toArray(String[]::new));
    String inputValues = Arrays.stream(testSets).map(x -> x.value).collect(Collectors.joining(","));
    File inputFile = createInputFile(testFolder, header, inputValues);
    embulk.runOutput(configSource, inputFile.toPath());

    String sql = String.format("SELECT * FROM %s", quotedDstTableName);
    List<Map<String, Object>> results = runQuery(sql);
    Assert.assertEquals(1, results.size());
    for (int i = 0; i < testSets.length; i++) {
      Assert.assertEquals(testSets[i].expected, results.get(0).get(columnName(i)));
    }
  }
}
