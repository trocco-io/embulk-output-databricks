package org.embulk.output.databricks;

import static org.embulk.output.databricks.util.ConfigUtil.createPluginConfigSource;
import static org.embulk.output.databricks.util.ConnectionUtil.*;
import static org.embulk.output.databricks.util.FixedColumnNameTableUtil.*;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.embulk.config.ConfigSource;
import org.embulk.output.databricks.util.ConnectionUtil;
import org.embulk.output.databricks.util.FixedColumnNameTableUtil;
import org.embulk.output.databricks.util.IOUtil;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.junit.*;

public class TestDatabricksOutputPluginByMode extends AbstractTestDatabricksOutputPlugin {
  private ConfigSource configSource;
  private String quotedDstTableName;

  @Test
  public void testInsertToNewTable() throws Exception {
    testKindOfInsertToNewTable(AbstractJdbcOutputPlugin.Mode.INSERT);
  }

  @Test
  public void testInsertToExistTable() throws Exception {
    testKindOfInsertToExistTable(AbstractJdbcOutputPlugin.Mode.INSERT);
  }

  @Test
  public void testInsertDirectToNewTable() throws Exception {
    testKindOfInsertToNewTable(AbstractJdbcOutputPlugin.Mode.INSERT_DIRECT);
  }

  @Test
  public void testInsertDirectToExistTable() throws Exception {
    testKindOfInsertToExistTable(AbstractJdbcOutputPlugin.Mode.INSERT_DIRECT);
  }

  private void testKindOfInsertToNewTable(AbstractJdbcOutputPlugin.Mode mode) throws IOException {
    setPluginConfigSource(mode);
    embulk.runOutput(configSource, createInputFile("test1,11", "test2,12").toPath());
    assertQueryResults("test1,11", "test2,12");
  }

  public void testKindOfInsertToExistTable(AbstractJdbcOutputPlugin.Mode mode) throws Exception {
    setPluginConfigSource(mode);
    createTable("test0,0", "test1,1");
    embulk.runOutput(configSource, createInputFile("test1,11", "test2,12").toPath());
    assertQueryResults("test0,0", "test1,1", "test1,11", "test2,12");
  }

  @Test
  public void testTruncateInsertToNewTable() throws Exception {
    testKindOfReplaceToNewTable(AbstractJdbcOutputPlugin.Mode.TRUNCATE_INSERT);
  }

  @Test
  public void testTruncateInsertToExistTable() throws Exception {
    testKindOfReplaceToExistTable(AbstractJdbcOutputPlugin.Mode.TRUNCATE_INSERT);
  }

  @Test
  public void testReplaceToNewTable() throws Exception {
    testKindOfReplaceToNewTable(AbstractJdbcOutputPlugin.Mode.REPLACE);
  }

  @Test
  public void testReplaceToExistTable() throws Exception {
    testKindOfReplaceToExistTable(AbstractJdbcOutputPlugin.Mode.REPLACE);
  }

  public void testKindOfReplaceToExistTable(AbstractJdbcOutputPlugin.Mode mode) throws Exception {
    setPluginConfigSource(mode);
    createTable("test0,0", "test1, 1");
    embulk.runOutput(configSource, createInputFile("test1,11", "test2,12").toPath());
    assertQueryResults("test1,11", "test2,12");
  }

  public void testKindOfReplaceToNewTable(AbstractJdbcOutputPlugin.Mode mode) throws Exception {
    setPluginConfigSource(mode);
    embulk.runOutput(configSource, createInputFile("test1,11", "test2,12").toPath());
    assertQueryResults("test1,11", "test2,12");
  }

  @Test
  public void testMergeToNewTable() throws Exception {
    setPluginConfigSource(AbstractJdbcOutputPlugin.Mode.MERGE, "_c0");
    embulk.runOutput(configSource, createInputFile("test1,11", "test2,12").toPath());
    assertQueryResults("test1,11", "test2,12");
  }

  @Test
  public void testMergeToExistTable() throws Exception {
    setPluginConfigSource(AbstractJdbcOutputPlugin.Mode.MERGE, "_c0");
    createTable("test0,0", "test1, 1");
    embulk.runOutput(configSource, createInputFile("test1,11", "test2,12").toPath());
    assertQueryResults("test0,0", "test1,11", "test2,12");
  }

  private void setPluginConfigSource(AbstractJdbcOutputPlugin.Mode mode, String... mergeKeys) {
    configSource = createPluginConfigSource(mode, Arrays.asList(mergeKeys));
    quotedDstTableName = quotedDstTableName(configSource);
  }

  private void createTable(String... rows) {
    String[] sqlTypes = sqlTypes(rows[0]);
    ConnectionUtil.run(createTableSQL(quotedDstTableName, sqlTypes));
    ConnectionUtil.run(insertSQL(quotedDstTableName, sqlTypes, rows));
  }

  private File createInputFile(String... rows) throws IOException {
    return IOUtil.createInputFile(testFolder, csvHeader(sqlTypes(rows[0])), rows);
  }

  private void assertQueryResults(String... rows) {
    int numberOfColumns = rows[0].split(",").length;
    List<String> data = new ArrayList<>();
    for (String row : rows) {
      data.addAll(Arrays.asList(row.split(",")));
    }
    String order =
        IntStream.range(0, numberOfColumns)
            .mapToObj(FixedColumnNameTableUtil::columnName)
            .collect(Collectors.joining(", "));
    String sql = String.format("SELECT * FROM %s ORDER BY %s", quotedDstTableName, order);
    List<Map<String, Object>> results = runQuery(sql);
    assertTableResults(results, numberOfColumns, data.toArray());
  }

  private String[] sqlTypes(String sampleRow) {
    int numberOfColumns = sampleRow.split(",").length;
    return IntStream.range(0, numberOfColumns).mapToObj(x -> "string").toArray(String[]::new);
  }
}
