package org.embulk.output.databricks;

import static org.embulk.output.databricks.util.ConfigUtil.createPluginConfigSource;
import static org.embulk.output.databricks.util.ConnectionUtil.quotedDstTableName;
import static org.embulk.output.databricks.util.ConnectionUtil.runQuery;
import static org.embulk.output.databricks.util.EmbulkIOUtil.createInputFile;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.embulk.EmbulkSystemProperties;
import org.embulk.config.ConfigSource;
import org.embulk.input.file.LocalFileInputPlugin;
import org.embulk.output.DatabricksOutputPlugin;
import org.embulk.output.databricks.util.ConfigUtil;
import org.embulk.output.databricks.util.ConnectionUtil;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.embulk.parser.csv.CsvParserPlugin;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.ParserPlugin;
import org.embulk.test.TestingEmbulk;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

public class TestDatabricksOutputPlugin {
  private static final EmbulkSystemProperties EMBULK_SYSTEM_PROPERTIES =
      EmbulkSystemProperties.of(new Properties());

  @Rule public TemporaryFolder testFolder = new TemporaryFolder();

  @Before
  public void setup() {
    if (ConfigUtil.disableOnlineTest()) {
      return;
    }
    ConnectionUtil.dropAllTemporaryTables();
  }

  @After
  public void cleanup() {
    if (ConfigUtil.disableOnlineTest()) {
      return;
    }
    ConnectionUtil.dropAllTemporaryTables();
  }

  @Rule
  public TestingEmbulk embulk =
      TestingEmbulk.builder()
          .setEmbulkSystemProperties(EMBULK_SYSTEM_PROPERTIES)
          .registerPlugin(FileInputPlugin.class, "file", LocalFileInputPlugin.class)
          .registerPlugin(ParserPlugin.class, "csv", CsvParserPlugin.class)
          .registerPlugin(OutputPlugin.class, "databricks", DatabricksOutputPlugin.class)
          .build();

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
}
