package org.embulk.output.databricks;

import static org.embulk.output.databricks.util.ConfigUtil.createPluginConfigSource;
import static org.embulk.output.databricks.util.IOUtil.createInputFile;

import java.io.File;
import java.io.IOException;
import org.embulk.config.ConfigSource;
import org.embulk.exec.PartialExecutionException;
import org.embulk.output.databricks.util.DatabricksApiClientUtil;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class TestDatabricksOutputPluginByDeleteStaging extends AbstractTestDatabricksOutputPlugin {
  @Test
  public void testDeleteStage() throws Exception {
    runOutputSuccess(true, true);
    Assert.assertTrue(DatabricksApiClientUtil.fetchAllTemporaryStagingVolumes().isEmpty());
  }

  @Test
  public void testNotDeleteStage() throws Exception {
    runOutputSuccess(false, false);
    Assert.assertFalse(DatabricksApiClientUtil.fetchAllTemporaryStagingVolumes().isEmpty());
  }

  @Test
  public void testDeleteStageOnError() throws Exception {
    Assume.assumeFalse(needSkip());

    Assert.assertThrows(PartialExecutionException.class, () -> runOutputError(true, true));
    Assert.assertTrue(DatabricksApiClientUtil.fetchAllTemporaryStagingVolumes().isEmpty());
  }

  @Test
  public void testNotDeleteStageOnError() throws Exception {
    Assume.assumeFalse(needSkip());

    Assert.assertThrows(PartialExecutionException.class, () -> runOutputError(true, false));
    Assert.assertFalse(DatabricksApiClientUtil.fetchAllTemporaryStagingVolumes().isEmpty());
  }

  @Test
  public void testDeleteNotStageOnErrorButStageOnError() throws Exception {
    Assume.assumeFalse(needSkip());

    Assert.assertThrows(PartialExecutionException.class, () -> runOutputError(false, true));
    Assert.assertFalse(DatabricksApiClientUtil.fetchAllTemporaryStagingVolumes().isEmpty());
  }

  private void runOutput(
      AbstractJdbcOutputPlugin.Mode mode, Boolean deleteStage, Boolean deleteStageOnError)
      throws IOException {
    ConfigSource configSource = createPluginConfigSource(mode);
    configSource.set("delete_stage", deleteStage);
    configSource.set("delete_stage_on_error", deleteStageOnError);
    File inputFile = createInputFile(testFolder, "_c0:string", "test1");

    Assert.assertTrue(DatabricksApiClientUtil.fetchAllTemporaryStagingVolumes().isEmpty());
    embulk.runOutput(configSource, inputFile.toPath());
  }

  private void runOutputSuccess(Boolean deleteStage, Boolean deleteStageOnError)
      throws IOException {
    runOutput(AbstractJdbcOutputPlugin.Mode.INSERT, deleteStage, deleteStageOnError);
  }

  private void runOutputError(Boolean deleteStage, Boolean deleteStageOnError) throws IOException {
    runOutput(AbstractJdbcOutputPlugin.Mode.MERGE_DIRECT, deleteStage, deleteStageOnError);
  }
}
