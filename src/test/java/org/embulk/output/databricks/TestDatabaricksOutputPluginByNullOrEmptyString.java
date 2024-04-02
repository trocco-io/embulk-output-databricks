package org.embulk.output.databricks;

import static org.embulk.output.databricks.util.ConfigUtil.createPluginConfigSource;
import static org.embulk.output.databricks.util.ConnectionUtil.quotedDstTableName;
import static org.embulk.output.databricks.util.ConnectionUtil.runQuery;
import static org.embulk.output.databricks.util.EmbulkIOUtil.createInputFile;

import java.io.File;
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
    ConfigSource configSource = createPluginConfigSource(AbstractJdbcOutputPlugin.Mode.INSERT);
    String quotedDstTableName = quotedDstTableName(configSource);

    File inputFile = createInputFile(testFolder, "_c0:string", "");
    embulk.runOutput(configSource, inputFile.toPath());

    String sql = String.format("SELECT * FROM %s ORDER BY _c0", quotedDstTableName);
    List<Map<String, Object>> results = runQuery(sql);
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(null, results.get(0).get("_c0"));
  }

  @Test
  public void testEmptyString() throws Exception {
    ConfigSource configSource = createPluginConfigSource(AbstractJdbcOutputPlugin.Mode.INSERT);
    String quotedDstTableName = quotedDstTableName(configSource);

    File inputFile = createInputFile(testFolder, "_c0:string", "\"\"");
    embulk.runOutput(configSource, inputFile.toPath());

    String sql = String.format("SELECT * FROM %s ORDER BY _c0", quotedDstTableName);
    List<Map<String, Object>> results = runQuery(sql);
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(null, results.get(0).get("_c0"));
  }

  @Test
  public void testTwoDoubleQuoteString() throws Exception {
    ConfigSource configSource = createPluginConfigSource(AbstractJdbcOutputPlugin.Mode.INSERT);
    String quotedDstTableName = quotedDstTableName(configSource);

    File inputFile = createInputFile(testFolder, "_c0:string", "\"\"\"\"\"\"");
    embulk.runOutput(configSource, inputFile.toPath());

    String sql = String.format("SELECT * FROM %s ORDER BY _c0", quotedDstTableName);
    List<Map<String, Object>> results = runQuery(sql);
    Assert.assertEquals(1, results.size());
    Assert.assertEquals("", results.get(0).get("_c0"));
  }
}
