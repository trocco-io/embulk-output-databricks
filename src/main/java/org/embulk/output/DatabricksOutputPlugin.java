package org.embulk.output;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.output.databricks.DatabricksAPIClient;
import org.embulk.output.databricks.DatabricksCopyBatchInsert;
import org.embulk.output.databricks.DatabricksOutputConnection;
import org.embulk.output.databricks.DatabricksOutputConnector;
import org.embulk.output.jdbc.*;
import org.embulk.spi.Schema;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabricksOutputPlugin extends AbstractJdbcOutputPlugin {
  protected final Logger logger = LoggerFactory.getLogger(getClass());

  public interface DatabricksPluginTask extends PluginTask {
    @Config("driver_path")
    @ConfigDefault("null")
    public Optional<String> getDriverPath();

    @Config("server_hostname")
    public String getServerHostname();

    @Config("http_path")
    public String getHTTPPath();

    @Config("auth_type")
    @ConfigDefault("\"pat\"") // oauth-m2m or pat
    public String getAuthType();

    @Config("personal_access_token")
    @ConfigDefault("null")
    public Optional<String> getPersonalAccessToken();

    @Config("oauth2_client_id")
    @ConfigDefault("null")
    public Optional<String> getOauth2ClientId();

    @Config("oauth2_client_secret")
    @ConfigDefault("null")
    public Optional<String> getOauth2ClientSecret();

    @Config("catalog_name")
    public String getCatalogName();

    @Config("schema_name")
    public String getSchemaName();

    @Config("staging_volume_name_prefix")
    @ConfigDefault("\"embulk_output_databricks_\"")
    public String getStagingVolumeNamePrefix();

    @Config("delete_stage")
    @ConfigDefault("false")
    public boolean getDeleteStage();

    @Config("delete_stage_on_error")
    @ConfigDefault("false")
    public boolean getDeleteStageOnError();

    @Config("user_agent")
    @ConfigDefault("{}")
    public UserAgentEntry getUserAgentEntry();

    public interface UserAgentEntry extends Task {
      @Config("product_name")
      @ConfigDefault("\"unknown\"")
      public String getProductName();

      @Config("product_version")
      @ConfigDefault("\"0.0.0\"")
      public String getProductVersion();
    }

    static String fetchPersonalAccessToken(DatabricksPluginTask t) {
      return validatePresence(t.getPersonalAccessToken(), "personal_access_token");
    }

    static String fetchOauth2ClientId(DatabricksPluginTask t) {
      return validatePresence(t.getOauth2ClientId(), "oauth2_client_id");
    }

    static String fetchOauth2ClientSecret(DatabricksPluginTask t) {
      return validatePresence(t.getOauth2ClientSecret(), "oauth2_client_secret");
    }
  }

  static <T> T validatePresence(Optional<T> val, String varName) {
    if (val.isPresent()) {
      return val.get();
    }
    throw new ConfigException(String.format("%s must not be null.", varName));
  }

  @Override
  protected Class<? extends PluginTask> getTaskClass() {
    return DatabricksPluginTask.class;
  }

  @Override
  protected Features getFeatures(PluginTask task) {
    return new Features()
        .setMaxTableNameLength(127)
        .setSupportedModes(
            Collections.unmodifiableSet(
                new HashSet<>(
                    Arrays.asList(
                        Mode.INSERT,
                        Mode.INSERT_DIRECT,
                        Mode.TRUNCATE_INSERT,
                        Mode.REPLACE,
                        Mode.MERGE))))
        .setIgnoreMergeKeys(false);
  }

  @Override
  protected JdbcOutputConnector getConnector(PluginTask task, boolean retryableMetadataOperation) {
    // https://docs.databricks.com/en/integrations/jdbc/index.html
    // https://docs.databricks.com/en/integrations/jdbc/authentication.html
    // https://docs.databricks.com/en/integrations/jdbc/compute.html
    DatabricksPluginTask t = (DatabricksPluginTask) task;
    loadDriver("com.databricks.client.jdbc.Driver", t.getDriverPath());
    String url = String.format("jdbc:databricks://%s:443", t.getServerHostname());
    Properties props = new java.util.Properties();
    props.put("httpPath", t.getHTTPPath());
    String authType = t.getAuthType();
    switch (authType) {
      case "pat":
        props.put("AuthMech", "3");
        props.put("UID", "token");
        props.put("PWD", DatabricksPluginTask.fetchPersonalAccessToken(t));
        break;
      case "oauth-m2m":
        props.put("AuthMech", "11");
        props.put("Auth_Flow", "1");
        props.put("OAuth2ClientId", DatabricksPluginTask.fetchOauth2ClientId(t));
        props.put("OAuth2Secret", DatabricksPluginTask.fetchOauth2ClientSecret(t));
        break;
      default:
        throw new ConfigException(String.format("unknown auth_type '%s'", authType));
    }
    props.put("SSL", "1");
    props.put("ConnCatalog", t.getCatalogName());
    props.put("ConnSchema", t.getSchemaName());
    props.putAll(t.getOptions());

    // overwrite UserAgentEntry property if the same property is set in options
    String productName = t.getUserAgentEntry().getProductName();
    String productVersion = t.getUserAgentEntry().getProductVersion();
    props.put("UserAgentEntry", productName + "/" + productVersion);

    logConnectionProperties(url, props);
    return new DatabricksOutputConnector(
        url, props, t.getTransactionIsolation(), t.getCatalogName(), t.getSchemaName());
  }

  @Override
  public ConfigDiff transaction(
      ConfigSource config, Schema schema, int taskCount, Control control) {
    DatabricksPluginTask t = (DatabricksPluginTask) CONFIG_MAPPER.map(config, this.getTaskClass());
    DatabricksAPIClient apiClient = DatabricksAPIClient.create(t);
    String volumeName =
        DatabricksAPIClient.fetchCurrentTransactionVolumeName(t.getStagingVolumeNamePrefix());
    ConfigDiff configDiff;
    try {
      apiClient.createVolume(t.getCatalogName(), t.getSchemaName(), volumeName);
      configDiff = super.transaction(config, schema, taskCount, control);
      if (t.getDeleteStage()) {
        apiClient.deleteVolume(t.getCatalogName(), t.getSchemaName(), volumeName);
      }
    } catch (Exception e) {
      if (t.getDeleteStage() && t.getDeleteStageOnError()) {
        apiClient.deleteVolume(t.getCatalogName(), t.getSchemaName(), volumeName);
      }
      throw new RuntimeException(e);
    }
    return configDiff;
  }

  @Override
  protected BatchInsert newBatchInsert(PluginTask task, Optional<MergeConfig> mergeConfig)
      throws IOException, SQLException {
    if (mergeConfig.isPresent()) {
      throw new UnsupportedOperationException(
          "Databricks output plugin doesn't support 'merge_direct' mode. Use 'merge' mode instead.");
    }
    DatabricksPluginTask t = (DatabricksPluginTask) task;
    return new DatabricksCopyBatchInsert(
        getConnector(task, true),
        task.getTargetTableSchema(),
        DatabricksAPIClient.createDatabricksConfig(t),
        t.getCatalogName(),
        t.getSchemaName(),
        DatabricksAPIClient.fetchCurrentTransactionVolumeName(t.getStagingVolumeNamePrefix()),
        t.getDeleteStage(),
        t.getDeleteStageOnError());
  }

  @Override
  protected void logConnectionProperties(String url, Properties props) {
    List<String> maskedKeys = Arrays.asList("PWD", "OAuth2Secret");
    Properties maskedProps = new Properties();
    for (Object keyObj : props.keySet()) {
      String key = (String) keyObj;
      String maskedVal = maskedKeys.contains(key) ? "***" : props.getProperty(key);
      maskedProps.setProperty(key, maskedVal);
    }
    super.logConnectionProperties(url, maskedProps);
  }

  public Optional<JdbcSchema> newJdbcSchemaFromTableIfExists(
      JdbcOutputConnection connection, TableIdentifier table) throws SQLException {
    return super.newJdbcSchemaFromTableIfExists(
        connection,
        ((DatabricksOutputConnection) connection).currentConnectionTableIdentifier(table));
  }
}
