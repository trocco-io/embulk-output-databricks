package org.embulk.output.databricks;

import static org.embulk.output.databricks.util.ConfigUtil.createPluginConfigSource;
import static org.embulk.output.databricks.util.ConfigUtil.setColumnOption;
import static org.embulk.output.databricks.util.ConnectionUtil.quotedDstTableName;
import static org.embulk.output.databricks.util.ConnectionUtil.runQuery;
import static org.embulk.output.databricks.util.FixedColumnNameTableUtil.columnName;
import static org.embulk.output.databricks.util.FixedColumnNameTableUtil.csvHeader;
import static org.embulk.output.databricks.util.IOUtil.createInputFile;

import java.io.File;
import java.io.IOException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.embulk.EmbulkSystemProperties;
import org.embulk.config.ConfigSource;
import org.embulk.input.file.LocalFileInputPlugin;
import org.embulk.output.DatabricksOutputPlugin;
import org.embulk.output.databricks.util.ConfigUtil;
import org.embulk.output.databricks.util.ConnectionUtil;
import org.embulk.output.databricks.util.DatabricksApiClientUtil;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.embulk.parser.csv.CsvParserPlugin;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.ParserPlugin;
import org.embulk.test.TestingEmbulk;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public abstract class AbstractTestDatabricksOutputPlugin {
  private static final EmbulkSystemProperties EMBULK_SYSTEM_PROPERTIES =
      EmbulkSystemProperties.of(new Properties());

  @Rule public TemporaryFolder testFolder = new TemporaryFolder();

  protected Boolean needSkip() {
    return ConfigUtil.disableOnlineTest();
  }

  @Before
  public void setup() {
    if (needSkip()) {
      return;
    }
    ConnectionUtil.dropAllTemporaryTables();
    DatabricksApiClientUtil.deleteAllTemporaryStagingVolumes();
  }

  @After
  public void cleanup() {
    if (needSkip()) {
      return;
    }
    ConnectionUtil.dropAllTemporaryTables();
    DatabricksApiClientUtil.deleteAllTemporaryStagingVolumes();
  }

  @Rule
  public TestingEmbulk embulk =
      TestingEmbulk.builder()
          .setEmbulkSystemProperties(EMBULK_SYSTEM_PROPERTIES)
          .registerPlugin(FileInputPlugin.class, "file", LocalFileInputPlugin.class)
          .registerPlugin(ParserPlugin.class, "csv", CsvParserPlugin.class)
          .registerPlugin(OutputPlugin.class, "databricks", DatabricksOutputPlugin.class)
          .build();

  protected class TestSet {
    final String value;
    final String inputType;
    final String outputType;
    final String outputValueType;

    final String outputTimestampFormat;

    final ZoneId outputTimezone;
    final Object expected;

    TestSet(
        String value,
        String inputType,
        String outputType,
        String outputValueType,
        String outputTimestampFormat,
        ZoneId outputTimezone,
        Object expected) {
      this.value = value;
      this.inputType = inputType;
      this.outputType = outputType;
      this.outputValueType = outputValueType;
      this.outputTimestampFormat = outputTimestampFormat;
      this.outputTimezone = outputTimezone;
      this.expected = expected;
    }
  }

  protected void testOutputValues(TestSet... testSets) throws IOException {
    ConfigSource configSource = createPluginConfigSource(AbstractJdbcOutputPlugin.Mode.INSERT);
    String quotedDstTableName = quotedDstTableName(configSource);
    for (int i = 0; i < testSets.length; i++) {
      TestSet t = testSets[i];
      setColumnOption(
          configSource,
          columnName(i),
          t.outputType,
          t.outputValueType,
          t.outputTimestampFormat,
          t.outputTimezone);
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
