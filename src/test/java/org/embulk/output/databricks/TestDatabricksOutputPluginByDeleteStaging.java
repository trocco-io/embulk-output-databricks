package org.embulk.output.databricks;

import static org.embulk.output.databricks.util.ConfigUtil.createPluginConfigSource;
import static org.embulk.output.databricks.util.EmbulkIOUtil.createInputFile;

import java.io.File;
import org.embulk.config.ConfigSource;
import org.embulk.exec.PartialExecutionException;
import org.embulk.output.databricks.util.DatabricksApiClientUtil;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.junit.Assert;
import org.junit.Test;

public class TestDatabricksOutputPluginByDeleteStaging extends AbstractTestDatabricksOutputPlugin {
  @Test
  public void testDeleteStage() throws Exception {
    ConfigSource configSource = createPluginConfigSource(AbstractJdbcOutputPlugin.Mode.INSERT);
    configSource.set("delete_stage", true);
    File inputFile = createInputFile(testFolder, "_c0:string", "test1");

    Assert.assertTrue(DatabricksApiClientUtil.fetchAllTemporaryStagingVolumes().isEmpty());

    embulk.runOutput(configSource, inputFile.toPath());

    Assert.assertTrue(DatabricksApiClientUtil.fetchAllTemporaryStagingVolumes().isEmpty());
  }

  @Test
  public void testNotDeleteStage() throws Exception {
    ConfigSource configSource = createPluginConfigSource(AbstractJdbcOutputPlugin.Mode.INSERT);
    configSource.set("delete_stage", false);
    File inputFile = createInputFile(testFolder, "_c0:string", "test1");

    Assert.assertTrue(DatabricksApiClientUtil.fetchAllTemporaryStagingVolumes().isEmpty());

    embulk.runOutput(configSource, inputFile.toPath());

    Assert.assertFalse(DatabricksApiClientUtil.fetchAllTemporaryStagingVolumes().isEmpty());
  }

  @Test
  public void testDeleteStageOnError() throws Exception {
    ConfigSource configSource =
        createPluginConfigSource(AbstractJdbcOutputPlugin.Mode.MERGE_DIRECT);
    configSource.set("delete_stage", true);
    configSource.set("delete_stage_on_error", true);
    File inputFile = createInputFile(testFolder, "_c0:string", "test1");

    Assert.assertTrue(DatabricksApiClientUtil.fetchAllTemporaryStagingVolumes().isEmpty());

    Assert.assertThrows(
        PartialExecutionException.class, () -> embulk.runOutput(configSource, inputFile.toPath()));

    Assert.assertTrue(DatabricksApiClientUtil.fetchAllTemporaryStagingVolumes().isEmpty());
  }

  @Test
  public void testNotDeleteStageOnError() throws Exception {
    ConfigSource configSource =
        createPluginConfigSource(AbstractJdbcOutputPlugin.Mode.MERGE_DIRECT);
    configSource.set("delete_stage", true);
    configSource.set("delete_stage_on_error", false);
    File inputFile = createInputFile(testFolder, "_c0:string", "test1");

    Assert.assertTrue(DatabricksApiClientUtil.fetchAllTemporaryStagingVolumes().isEmpty());

    Assert.assertThrows(
        PartialExecutionException.class, () -> embulk.runOutput(configSource, inputFile.toPath()));

    Assert.assertFalse(DatabricksApiClientUtil.fetchAllTemporaryStagingVolumes().isEmpty());
  }
}
