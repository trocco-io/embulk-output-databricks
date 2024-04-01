package org.embulk.output.databricks;

import org.embulk.config.ConfigSource;
import org.embulk.output.databricks.util.ConfigUtil;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.junit.Assert;
import org.junit.Test;

public class TestConfigUtil {
  @Test
  public void TestCreatePluginTask() {
    final ConfigSource configSource =
        ConfigUtil.createPluginConfigSource(AbstractJdbcOutputPlugin.Mode.INSERT);
    Assert.assertNotNull(ConfigUtil.createPluginTask(configSource));
  }
}
