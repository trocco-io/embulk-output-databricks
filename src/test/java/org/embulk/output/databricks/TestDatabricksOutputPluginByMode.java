package org.embulk.output.databricks;

import static org.embulk.output.databricks.util.ConfigUtil.createPluginConfigSource;
import static org.embulk.output.databricks.util.ConnectionUtil.quotedDstTableName;
import static org.embulk.output.databricks.util.ConnectionUtil.runQuery;
import static org.embulk.output.databricks.util.EmbulkIOUtil.createInputFile;

import java.io.File;
import java.util.*;
import org.embulk.config.ConfigSource;
import org.embulk.output.databricks.util.ConnectionUtil;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.junit.*;

public class TestDatabricksOutputPluginByMode extends AbstractTestDatabricksOutputPlugin {
  @Test
  public void testInsertAsNewTable() throws Exception {
    ConfigSource configSource = createPluginConfigSource(AbstractJdbcOutputPlugin.Mode.INSERT);
    String quotedDstTableName = quotedDstTableName(configSource);

    File inputFile = createInputFile(testFolder, "_c0:string", "test1", "test2");
    embulk.runOutput(configSource, inputFile.toPath());

    String sql = String.format("SELECT * FROM %s ORDER BY _c0", quotedDstTableName);
    List<Map<String, Object>> results = runQuery(sql);
    Assert.assertEquals(2, results.size());
    Assert.assertEquals("test1", results.get(0).get("_c0"));
    Assert.assertEquals("test2", results.get(1).get("_c0"));
  }

  @Test
  public void testInsertToExistTable() throws Exception {
    ConfigSource configSource = createPluginConfigSource(AbstractJdbcOutputPlugin.Mode.INSERT);
    String quotedDstTableName = quotedDstTableName(configSource);
    String createTableSQL = String.format("CREATE TABLE %s (_c0 string)", quotedDstTableName);
    ConnectionUtil.run(createTableSQL);

    String insertTableSQL = String.format("INSERT INTO %s VALUES ('test0')", quotedDstTableName);
    ConnectionUtil.run(insertTableSQL);

    File inputFile = createInputFile(testFolder, "_c0:string", "test1", "test2");
    embulk.runOutput(configSource, inputFile.toPath());

    String sql = String.format("SELECT * FROM %s ORDER BY _c0", quotedDstTableName);
    List<Map<String, Object>> results = runQuery(sql);
    Assert.assertEquals(3, results.size());
    Assert.assertEquals("test0", results.get(0).get("_c0"));
    Assert.assertEquals("test1", results.get(1).get("_c0"));
    Assert.assertEquals("test2", results.get(2).get("_c0"));
  }

  @Test
  public void testInsertDirectAsNewTable() throws Exception {
    ConfigSource configSource =
        createPluginConfigSource(AbstractJdbcOutputPlugin.Mode.INSERT_DIRECT);
    String quotedDstTableName = quotedDstTableName(configSource);

    File inputFile = createInputFile(testFolder, "_c0:string", "test1", "test2");
    embulk.runOutput(configSource, inputFile.toPath());

    String sql = String.format("SELECT * FROM %s ORDER BY _c0", quotedDstTableName);
    List<Map<String, Object>> results = runQuery(sql);
    Assert.assertEquals(2, results.size());
    Assert.assertEquals("test1", results.get(0).get("_c0"));
    Assert.assertEquals("test2", results.get(1).get("_c0"));
  }

  @Test
  public void testInsertDirectToExistTable() throws Exception {
    ConfigSource configSource =
        createPluginConfigSource(AbstractJdbcOutputPlugin.Mode.INSERT_DIRECT);
    String quotedDstTableName = quotedDstTableName(configSource);
    String createTableSQL = String.format("CREATE TABLE %s (_c0 string)", quotedDstTableName);
    ConnectionUtil.run(createTableSQL);

    String insertTableSQL = String.format("INSERT INTO %s VALUES ('test0')", quotedDstTableName);
    ConnectionUtil.run(insertTableSQL);

    File inputFile = createInputFile(testFolder, "_c0:string", "test1", "test2");
    embulk.runOutput(configSource, inputFile.toPath());

    String sql = String.format("SELECT * FROM %s ORDER BY _c0", quotedDstTableName);
    List<Map<String, Object>> results = runQuery(sql);
    Assert.assertEquals(3, results.size());
    Assert.assertEquals("test0", results.get(0).get("_c0"));
    Assert.assertEquals("test1", results.get(1).get("_c0"));
    Assert.assertEquals("test2", results.get(2).get("_c0"));
  }

  @Test
  public void testTruncateInsertAsNewTable() throws Exception {
    ConfigSource configSource =
        createPluginConfigSource(AbstractJdbcOutputPlugin.Mode.TRUNCATE_INSERT);
    String quotedDstTableName = quotedDstTableName(configSource);

    File inputFile = createInputFile(testFolder, "_c0:string", "test1", "test2");
    embulk.runOutput(configSource, inputFile.toPath());

    String sql = String.format("SELECT * FROM %s ORDER BY _c0", quotedDstTableName);
    List<Map<String, Object>> results = runQuery(sql);
    Assert.assertEquals(2, results.size());
    Assert.assertEquals("test1", results.get(0).get("_c0"));
    Assert.assertEquals("test2", results.get(1).get("_c0"));
  }

  @Test
  public void testTruncateInsertToExistTable() throws Exception {
    ConfigSource configSource =
        createPluginConfigSource(AbstractJdbcOutputPlugin.Mode.TRUNCATE_INSERT);
    String quotedDstTableName = quotedDstTableName(configSource);
    String createTableSQL = String.format("CREATE TABLE %s (_c0 string)", quotedDstTableName);
    ConnectionUtil.run(createTableSQL);

    String insertTableSQL = String.format("INSERT INTO %s VALUES ('test0')", quotedDstTableName);
    ConnectionUtil.run(insertTableSQL);

    File inputFile = createInputFile(testFolder, "_c0:string", "test1", "test2");
    embulk.runOutput(configSource, inputFile.toPath());

    String sql = String.format("SELECT * FROM %s ORDER BY _c0", quotedDstTableName);
    List<Map<String, Object>> results = runQuery(sql);
    Assert.assertEquals(2, results.size());
    Assert.assertEquals("test1", results.get(0).get("_c0"));
    Assert.assertEquals("test2", results.get(1).get("_c0"));
  }

  @Test
  public void testReplaceAsNewTable() throws Exception {
    ConfigSource configSource = createPluginConfigSource(AbstractJdbcOutputPlugin.Mode.REPLACE);
    String quotedDstTableName = quotedDstTableName(configSource);

    File inputFile = createInputFile(testFolder, "_c0:string", "test1", "test2");
    embulk.runOutput(configSource, inputFile.toPath());

    String sql = String.format("SELECT * FROM %s ORDER BY _c0", quotedDstTableName);
    List<Map<String, Object>> results = runQuery(sql);
    Assert.assertEquals(2, results.size());
    Assert.assertEquals("test1", results.get(0).get("_c0"));
    Assert.assertEquals("test2", results.get(1).get("_c0"));
  }

  @Test
  public void testReplaceToExistTable() throws Exception {
    ConfigSource configSource = createPluginConfigSource(AbstractJdbcOutputPlugin.Mode.REPLACE);
    String quotedDstTableName = quotedDstTableName(configSource);
    String createTableSQL = String.format("CREATE TABLE %s (_c0 string)", quotedDstTableName);
    ConnectionUtil.run(createTableSQL);

    String insertTableSQL = String.format("INSERT INTO %s VALUES ('test0')", quotedDstTableName);
    ConnectionUtil.run(insertTableSQL);

    File inputFile = createInputFile(testFolder, "_c0:string", "test1", "test2");
    embulk.runOutput(configSource, inputFile.toPath());

    String sql = String.format("SELECT * FROM %s ORDER BY _c0", quotedDstTableName);
    List<Map<String, Object>> results = runQuery(sql);
    Assert.assertEquals(2, results.size());
    Assert.assertEquals("test1", results.get(0).get("_c0"));
    Assert.assertEquals("test2", results.get(1).get("_c0"));
  }

  @Test
  public void testMergeAsNewTable() throws Exception {
    List<String> mergeKeys = new ArrayList<>(Collections.singletonList("_c0"));
    ConfigSource configSource =
        createPluginConfigSource(AbstractJdbcOutputPlugin.Mode.MERGE, Optional.of(mergeKeys));
    String quotedDstTableName = quotedDstTableName(configSource);

    File inputFile = createInputFile(testFolder, "_c0:string,_c1:long", "test1,1", "test2,2");
    embulk.runOutput(configSource, inputFile.toPath());

    String sql = String.format("SELECT * FROM %s ORDER BY _c0", quotedDstTableName);
    List<Map<String, Object>> results = runQuery(sql);
    Assert.assertEquals(2, results.size());
    Assert.assertEquals("test1", results.get(0).get("_c0"));
    Assert.assertEquals(1L, results.get(0).get("_c1"));
    Assert.assertEquals("test2", results.get(1).get("_c0"));
    Assert.assertEquals(2L, results.get(1).get("_c1"));
  }

  @Test
  public void testMergeToExistTable() throws Exception {
    List<String> mergeKeys = new ArrayList<>(Collections.singletonList("_c0"));
    ConfigSource configSource =
        createPluginConfigSource(AbstractJdbcOutputPlugin.Mode.MERGE, Optional.of(mergeKeys));
    String quotedDstTableName = quotedDstTableName(configSource);
    String createTableSQL =
        String.format("CREATE TABLE %s (_c0 string, _c1 long)", quotedDstTableName);
    ConnectionUtil.run(createTableSQL);

    String insertTableSQL =
        String.format("INSERT INTO %s VALUES ('test0', 0), ('test1', 1)", quotedDstTableName);
    ConnectionUtil.run(insertTableSQL);

    File inputFile = createInputFile(testFolder, "_c0:string,_c1:long", "test1,11", "test2,12");
    embulk.runOutput(configSource, inputFile.toPath());

    String sql = String.format("SELECT * FROM %s ORDER BY _c0", quotedDstTableName);
    List<Map<String, Object>> results = runQuery(sql);
    Assert.assertEquals(3, results.size());
    Assert.assertEquals("test0", results.get(0).get("_c0"));
    Assert.assertEquals(0L, results.get(0).get("_c1"));
    Assert.assertEquals("test1", results.get(1).get("_c0"));
    Assert.assertEquals(11L, results.get(1).get("_c1"));
    Assert.assertEquals("test2", results.get(2).get("_c0"));
    Assert.assertEquals(12L, results.get(2).get("_c1"));
  }
}
