package org.embulk.output.databricks.util;

import static com.google.common.base.Strings.isNullOrEmpty;

import java.util.List;
import java.util.Optional;
import org.embulk.config.ConfigSource;
import org.embulk.output.DatabricksOutputPlugin;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.embulk.test.EmbulkTests;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigMapper;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.modules.ZoneIdModule;

public class ConfigUtil {
  private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder()
          .addDefaultModules()
          .addModule(ZoneIdModule.withLegacyNames())
          .build();
  private static final ConfigMapper CONFIG_MAPPER = CONFIG_MAPPER_FACTORY.createConfigMapper();

  private static final String configEnvName = "EMBULK_OUTPUT_DATABRICKS_TEST_CONFIG";

  public static Boolean disableOnlineTest() {
    return isNullOrEmpty(System.getenv(configEnvName));
  }

  public static ConfigSource baseConfigSource() {
    return EmbulkTests.config(configEnvName);
  }

  public interface TestTask extends Task {
    @Config("server_hostname")
    public String getServerHostname();

    @Config("http_path")
    public String getHTTPPath();

    @Config("personal_access_token")
    public String getPersonalAccessToken();

    @Config("catalog_name")
    public String getCatalogName();

    @Config("schema_name")
    public String getSchemaName();

    @Config("table_prefix")
    public String getTablePrefix();
  }

  public static TestTask createTestTask() {
    return CONFIG_MAPPER.map(baseConfigSource(), TestTask.class);
  }

  public static ConfigSource createPluginConfigSource(AbstractJdbcOutputPlugin.Mode mode) {
    return createPluginConfigSource(mode, Optional.empty());
  }

  public static ConfigSource createPluginConfigSource(
      AbstractJdbcOutputPlugin.Mode mode, Optional<List<String>> mergeKeys) {
    final TestTask t = createTestTask();
    return CONFIG_MAPPER_FACTORY
        .newConfigSource()
        .set("type", "databricks")
        .set("server_hostname", t.getServerHostname())
        .set("http_path", t.getHTTPPath())
        .set("personal_access_token", t.getPersonalAccessToken())
        .set("catalog_name", t.getCatalogName())
        .set("schema_name", t.getSchemaName())
        .set("mode", mode)
        .set("delete_stage_on_error", true)
        .set("merge_keys", mergeKeys)
        .set("delete_stage", true)
        .set("table", t.getTablePrefix() + "_dst");
  }

  public static DatabricksOutputPlugin.DatabricksPluginTask createPluginTask(
      ConfigSource configSource) {
    return CONFIG_MAPPER.map(configSource, DatabricksOutputPlugin.DatabricksPluginTask.class);
  }
}
