package org.embulk.output.databricks;

import static org.embulk.output.databricks.util.ConfigUtil.createPluginConfigSource;
import static org.embulk.output.databricks.util.ConnectionUtil.quotedDstTableName;
import static org.embulk.output.databricks.util.ConnectionUtil.runQuery;
import static org.embulk.output.databricks.util.EmbulkIOUtil.createInputFile;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.embulk.config.ConfigSource;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.junit.Assert;
import org.junit.Test;

public class TestDatabaricksOutputPluginByNullOrEmptyString
    extends AbstractTestDatabricksOutputPlugin {
  @Test
  public void testNull() throws Exception {
    // In default behavior of CSV parser plugin, no-quoted empty string is treated as null.
    // (https://www.embulk.org/docs/built-in.html)
    runOutput("", null);
  }

  @Test
  public void testEmptyString() throws Exception {
    // In default behavior of CSV parser plugin, quoted empty string is treated as empty string.
    // (https://www.embulk.org/docs/built-in.html)
    runOutput("\"\"", null);
  }

  @Test
  public void testTwoDoubleQuoteString() throws Exception {
    runOutput("\"\"\"\"\"\"", "");
  }

  private void runOutput(String inputValue, String expectedValue) throws IOException {
    ConfigSource configSource = createPluginConfigSource(AbstractJdbcOutputPlugin.Mode.INSERT);
    String quotedDstTableName = quotedDstTableName(configSource);

    File inputFile = createInputFile(testFolder, "_c0:string", inputValue);
    embulk.runOutput(configSource, inputFile.toPath());

    String sql = String.format("SELECT * FROM %s ORDER BY _c0", quotedDstTableName);
    List<Map<String, Object>> results = runQuery(sql);
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(expectedValue, results.get(0).get("_c0"));
  }
}
