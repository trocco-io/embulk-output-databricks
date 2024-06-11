package org.embulk.output.databricks;

import static org.embulk.output.databricks.util.ConfigUtil.createPluginConfigSource;
import static org.embulk.output.databricks.util.ConfigUtil.setColumnOption;
import static org.embulk.output.databricks.util.IOUtil.createInputFile;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import org.embulk.config.ConfigSource;
import org.embulk.exec.PartialExecutionException;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.junit.Assert;
import org.junit.Test;

public class TestDatabricksOutputPluginByUnsupported extends AbstractTestDatabricksOutputPlugin {
  @Test
  public void testUnsupportedInvalidType() throws IOException {
    ConfigSource configSource = createPluginConfigSource(AbstractJdbcOutputPlugin.Mode.INSERT);
    setColumnOption(configSource, "_c0", "Invalid");
    File inputFile = createInputFile(testFolder, "_c0:string", "invalid");
    PartialExecutionException partialExecutionException =
        Assert.assertThrows(
            PartialExecutionException.class,
            () -> embulk.runOutput(configSource, inputFile.toPath()));
    SQLException mainException =
        (SQLException) partialExecutionException.getCause().getCause().getCause().getCause();
    String errorMsg = mainException.getMessage();
    Assert.assertTrue(errorMsg, errorMsg.contains("Unsupported data type \"INVALID\""));
  }

  @Test
  public void testUnsupportedTimestampNTZType() throws IOException {
    ConfigSource configSource = createPluginConfigSource(AbstractJdbcOutputPlugin.Mode.INSERT);
    setColumnOption(configSource, "_c0", "timestamp_ntz");
    File inputFile = createInputFile(testFolder, "_c0:string", "2000-01-02 03:04:05.00 UTC");
    PartialExecutionException partialExecutionException =
        Assert.assertThrows(
            PartialExecutionException.class,
            () -> embulk.runOutput(configSource, inputFile.toPath()));
    SQLException mainException =
        (SQLException)
            partialExecutionException.getCause().getCause().getCause().getCause().getCause();
    String errorMsg = mainException.getMessage();
    Assert.assertTrue(errorMsg, errorMsg.contains("Syntax error at or near ','"));
    Assert.assertTrue(errorMsg, errorMsg.contains("COPY INTO"));
  }

  @Test
  public void testUnsupportedIntervalType() throws IOException {
    ConfigSource configSource = createPluginConfigSource(AbstractJdbcOutputPlugin.Mode.INSERT);
    setColumnOption(configSource, "_c0", "INTERVAL DAY TO SECOND");
    File inputFile = createInputFile(testFolder, "_c0:string", "11 23:4:0");
    PartialExecutionException partialExecutionException =
        Assert.assertThrows(
            PartialExecutionException.class,
            () -> embulk.runOutput(configSource, inputFile.toPath()));
    SQLException mainException =
        (SQLException)
            partialExecutionException.getCause().getCause().getCause().getCause().getCause();
    String errorMsg = mainException.getMessage();
    Assert.assertTrue(errorMsg, errorMsg.contains("Syntax error at or near ','"));
    Assert.assertTrue(errorMsg, errorMsg.contains("COPY INTO"));
  }

  @Test
  public void testUnsupportedArrayType() throws IOException {
    ConfigSource configSource = createPluginConfigSource(AbstractJdbcOutputPlugin.Mode.INSERT);
    setColumnOption(configSource, "_c0", "ARRAY<INT>");
    File inputFile = createInputFile(testFolder, "_c0:string", "[1]");

    PartialExecutionException partialExecutionException =
        Assert.assertThrows(
            PartialExecutionException.class,
            () -> embulk.runOutput(configSource, inputFile.toPath()));
    SQLException mainException =
        (SQLException)
            partialExecutionException.getCause().getCause().getCause().getCause().getCause();
    String errorMsg = mainException.getMessage();
    Assert.assertTrue(errorMsg, errorMsg.contains("cannot cast \"STRING\" to \"ARRAY<INT>\""));
    Assert.assertTrue(errorMsg, errorMsg.contains("COPY INTO"));
  }

  @Test
  public void testUnsupportedMapType() throws IOException {
    ConfigSource configSource = createPluginConfigSource(AbstractJdbcOutputPlugin.Mode.INSERT);
    setColumnOption(configSource, "_c0", "MAP<INT, INT>");
    File inputFile = createInputFile(testFolder, "_c0:string", "{ 1 : 3  }");

    PartialExecutionException partialExecutionException =
        Assert.assertThrows(
            PartialExecutionException.class,
            () -> embulk.runOutput(configSource, inputFile.toPath()));
    SQLException mainException =
        (SQLException)
            partialExecutionException.getCause().getCause().getCause().getCause().getCause();
    String errorMsg = mainException.getMessage();
    Assert.assertTrue(errorMsg, errorMsg.contains("cannot cast \"STRING\" to \"MAP<INT, INT>\""));
    Assert.assertTrue(errorMsg, errorMsg.contains("COPY INTO"));
  }

  @Test
  public void testUnsupportedStructType() throws IOException {
    ConfigSource configSource = createPluginConfigSource(AbstractJdbcOutputPlugin.Mode.INSERT);
    setColumnOption(configSource, "_c0", "STRUCT<price:INT>");
    File inputFile = createInputFile(testFolder, "_c0:string", "\"{ \"\"price\"\": 0}\"");

    PartialExecutionException partialExecutionException =
        Assert.assertThrows(
            PartialExecutionException.class,
            () -> embulk.runOutput(configSource, inputFile.toPath()));
    SQLException mainException =
        (SQLException)
            partialExecutionException.getCause().getCause().getCause().getCause().getCause();
    String errorMsg = mainException.getMessage();
    Assert.assertTrue(
        errorMsg, errorMsg.contains("cannot cast \"STRING\" to \"STRUCT<PRICE: INT>\""));
    Assert.assertTrue(errorMsg, errorMsg.contains("COPY INTO"));
  }
}
