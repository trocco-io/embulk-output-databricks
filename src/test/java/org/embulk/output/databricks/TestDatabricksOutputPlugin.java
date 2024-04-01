package org.embulk.output.databricks;

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
import org.embulk.output.databricks.util.EmbulkIOUtil;
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
//    ConnectionUtil.dropAllTemporaryTables();
  }

  @After
  public void cleanup() {
  //  ConnectionUtil.dropAllTemporaryTables();
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
  public void testConfigDefault() throws Exception {
    ConfigSource configSource =
        ConfigUtil.createPluginConfigSource(AbstractJdbcOutputPlugin.Mode.REPLACE);
    File inputFile = EmbulkIOUtil.createInputFile(testFolder, "_c0:string", "test", "test0");
    embulk.runOutput(configSource, inputFile.toPath());
    DatabricksOutputPlugin.DatabricksPluginTask t = ConfigUtil.createPluginTask(configSource);
    List<Map<String, Object>> results =  ConnectionUtil.runQuery(
        String.format("SELECT * FROM `%s`.`%s`.`%s`", t.getCatalogName(), t.getSchemaName(), t.getTable()));
    Assert.assertEquals(2, results.size());
    Assert.assertEquals("test", results.get(0).get("_c0"));
    Assert.assertEquals("test0", results.get(1).get("_c0"));
  }
}
