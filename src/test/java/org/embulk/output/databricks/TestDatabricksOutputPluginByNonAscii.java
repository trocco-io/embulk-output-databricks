package org.embulk.output.databricks;

import static org.embulk.output.databricks.util.ConfigUtil.createPluginConfigSource;
import static org.embulk.output.databricks.util.ConnectionUtil.quotedDstTableName;
import static org.embulk.output.databricks.util.ConnectionUtil.run;
import static org.embulk.output.databricks.util.ConnectionUtil.runQuery;
import static org.embulk.output.databricks.util.IOUtil.createInputFile;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.embulk.config.ConfigSource;
import org.embulk.output.databricks.util.ConfigUtil;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin.Mode;
import org.junit.Assert;
import org.junit.Test;

public class TestDatabricksOutputPluginByNonAscii extends AbstractTestDatabricksOutputPlugin {

  @Test
  public void testColumnNameInsert() throws Exception {
    ConfigSource configSource = createPluginConfigSource(Mode.INSERT);
    runOutputAndAssertResult(configSource);
  }

  @Test
  public void testColumnNameInsertDirect() throws Exception {
    ConfigSource configSource = createPluginConfigSource(Mode.INSERT_DIRECT);
    runOutputAndAssertResult(configSource);
  }

  @Test
  public void testColumnNameTruncateInsert() throws Exception {
    ConfigSource configSource = createPluginConfigSource(Mode.TRUNCATE_INSERT);
    runOutputAndAssertResult(configSource);
  }

  @Test
  public void testColumnNameReplace() throws Exception {
    ConfigSource configSource = createPluginConfigSource(Mode.REPLACE);
    runOutputAndAssertResult(configSource);
  }

  @Test
  public void testColumnNameMergeWithMergeKeys() throws Exception {
    ConfigSource configSource = createPluginConfigSource(Mode.MERGE);
    ConfigUtil.setMergeKeys(configSource, "あ");
    setupForMerge(configSource, false);
    runOutputAndAssertResult(configSource, "あ", "い");
  }

  @Test
  public void testColumnNameWithoutMergeKeys() throws Exception {
    ConfigSource configSource = createPluginConfigSource(Mode.MERGE);
    setupForMerge(configSource, true);
    runOutputAndAssertResult(configSource, "あ", "い");
  }

  @Test
  public void testColumnNameMergeWithMergeRule() throws Exception {
    ConfigSource configSource = createPluginConfigSource(Mode.MERGE);
    ConfigUtil.setMergeRule(configSource, "`い` = CONCAT(T.`い`, 'あ', S.`い`)");
    setupForMerge(configSource, true);
    runOutput(configSource, "あ", "い");
    List<Map<String, Object>> results =
        runQuery("SELECT * FROM " + quotedDstTableName(configSource));
    Assert.assertEquals(1, results.size());
    Assert.assertEquals("test0", results.get(0).get("あ"));
    Assert.assertEquals("hogeあtest1", results.get(0).get("い"));
  }

  private void setupForMerge(ConfigSource configSource, boolean hasPrimaryKey) {
    String quotedDstTableName = quotedDstTableName(configSource);
    String primaryKey = hasPrimaryKey ? "PRIMARY KEY" : "";
    run("CREATE TABLE " + quotedDstTableName + " (`あ` STRING " + primaryKey + ", `い` STRING)");
    run("INSERT INTO " + quotedDstTableName + "(`あ`, `い`) VALUES ('test0', 'hoge')");
  }

  private void runOutputAndAssertResult(ConfigSource configSource) throws IOException {
    runOutputAndAssertResult(configSource, "あ", "`", "\"", "'");
  }

  private void runOutputAndAssertResult(ConfigSource configSource, String... columnNames)
      throws IOException {
    runOutput(configSource, columnNames);
    assertResult(configSource, columnNames);
  }

  private void runOutput(ConfigSource configSource, String... columnNames) throws IOException {
    String header =
        Arrays.stream(columnNames).map(x -> x + ":string").collect(Collectors.joining(","));
    String data =
        IntStream.range(0, columnNames.length)
            .mapToObj(i -> "test" + i)
            .collect(Collectors.joining(","));
    File inputFile = createInputFile(testFolder, header, data);
    embulk.runOutput(configSource, inputFile.toPath());
  }

  private void assertResult(ConfigSource configSource, String... columnNames) {
    List<Map<String, Object>> results =
        runQuery("SELECT * FROM " + quotedDstTableName(configSource));
    Assert.assertEquals(1, results.size());
    for (int i = 0; i < columnNames.length; i++) {
      Assert.assertEquals("test" + i, results.get(0).get(columnNames[i]));
    }
  }
}
