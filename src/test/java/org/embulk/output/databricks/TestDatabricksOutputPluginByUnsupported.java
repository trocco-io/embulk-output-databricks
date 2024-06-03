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
  public void testUnsupportedTimestampNTZType() throws IOException {
    ConfigSource configSource = createPluginConfigSource(AbstractJdbcOutputPlugin.Mode.INSERT);
    setColumnOption(configSource, "_c0", "timestamp_ntz");
    File inputFile = createInputFile(testFolder, "_c0:string", "2000-01-02 03:04:05.00 UTC");
    PartialExecutionException e =
        Assert.assertThrows(
            PartialExecutionException.class,
            () -> embulk.runOutput(configSource, inputFile.toPath()));
    Assert.assertTrue(
        e.getCause().getCause().getCause().getCause().getCause() instanceof SQLException);
  }
}
