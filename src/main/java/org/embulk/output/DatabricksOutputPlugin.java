package org.embulk.output;

import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.output.databricks.DatabricksAPIClient;
import org.embulk.output.databricks.DatabricksCopyBatchInsert;
import org.embulk.output.databricks.DatabricksOutputConnection;
import org.embulk.output.databricks.DatabricksOutputConnector;
import org.embulk.output.jdbc.*;
import org.embulk.spi.Schema;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
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

    @Config("personal_access_token")
    public String getPersonalAccessToken();

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
    @ConfigDefault("null")
    public Optional<Map<String, String>> getUserAgent();
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
    props.put("AuthMech", "3");
    props.put("UID", "token");
    props.put("PWD", t.getPersonalAccessToken());
    props.put("SSL", "1");
    props.put("ConnCatalog", t.getCatalogName());
    props.put("ConnSchema", t.getSchemaName());
    props.putAll(t.getOptions());
    // overwrite UserAgentEntry property if the same property is set in options
    if (t.getUserAgent().isPresent()) {
      String product_name = t.getUserAgent().get().get("product_name");
      String product_version = t.getUserAgent().get().get("product_version");

      props.put("UserAgentEntry", product_name + "/" + product_version);
    }

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
    Properties maskedProps = new Properties();
    for (Object keyObj : props.keySet()) {
      String key = (String) keyObj;
      String maskedVal = key.equals("PWD") ? "***" : props.getProperty(key);
      maskedProps.setProperty(key, maskedVal);
    }
    super.logConnectionProperties(url, maskedProps);
  }

  // This is almost copy from AbstractJdbcOutputPlugin excepting validation of table exists in
  // current schema
  public Optional<JdbcSchema> newJdbcSchemaFromTableIfExists(
      JdbcOutputConnection connection, TableIdentifier table) throws SQLException {
    if (!connection.tableExists(table)) {
      // DatabaseMetaData.getPrimaryKeys fails if table does not exist
      return Optional.empty();
    }

    DatabaseMetaData dbm = connection.getMetaData();
    String escape = dbm.getSearchStringEscape();

    ResultSet rs =
        dbm.getPrimaryKeys(table.getDatabase(), table.getSchemaName(), table.getTableName());
    final HashSet<String> primaryKeysBuilder = new HashSet<>();
    try {
      while (rs.next()) {
        if (!((DatabricksOutputConnection) connection)
            .isAvailableTableMetadataInConnection(rs, table)) {
          continue;
        }
        primaryKeysBuilder.add(rs.getString("COLUMN_NAME"));
      }
    } finally {
      rs.close();
    }
    final Set<String> primaryKeys = Collections.unmodifiableSet(primaryKeysBuilder);

    final ArrayList<JdbcColumn> builder = new ArrayList<>();
    // NOTE: Columns of TIMESTAMP_NTZ, INTERVAL are not included in getColumns result.
    // This cause runtime sql exception when copy into.
    // (probably because of unsupported in databricks jdbc)
    // https://docs.databricks.com/en/sql/language-manual/data-types/interval-type.html
    // https://docs.databricks.com/en/sql/language-manual/data-types/timestamp-ntz-type.html#notes
    rs =
        dbm.getColumns(
            JdbcUtils.escapeSearchString(table.getDatabase(), escape),
            JdbcUtils.escapeSearchString(table.getSchemaName(), escape),
            JdbcUtils.escapeSearchString(table.getTableName(), escape),
            null);
    try {
      while (rs.next()) {
        if (!((DatabricksOutputConnection) connection)
            .isAvailableTableMetadataInConnection(rs, table)) {
          continue;
        }
        String columnName = rs.getString("COLUMN_NAME");
        String simpleTypeName = rs.getString("TYPE_NAME").toUpperCase(Locale.ENGLISH);
        boolean isUniqueKey = primaryKeys.contains(columnName);
        int sqlType = rs.getInt("DATA_TYPE");
        int colSize = rs.getInt("COLUMN_SIZE");
        int decDigit = rs.getInt("DECIMAL_DIGITS");
        if (rs.wasNull()) {
          decDigit = -1;
        }
        int charOctetLength = rs.getInt("CHAR_OCTET_LENGTH");
        boolean isNotNull = "NO".equals(rs.getString("IS_NULLABLE"));
        // rs.getString("COLUMN_DEF") // or null  // TODO
        builder.add(
            JdbcColumn.newGenericTypeColumn(
                columnName,
                sqlType,
                simpleTypeName,
                colSize,
                decDigit,
                charOctetLength,
                isNotNull,
                isUniqueKey));
        // We can't get declared column name using JDBC API.
        // Subclasses need to overwrite it.
      }
    } finally {
      rs.close();
    }
    final List<JdbcColumn> columns = Collections.unmodifiableList(builder);
    if (columns.isEmpty()) {
      return Optional.empty();
    } else {
      return Optional.of(new JdbcSchema(columns));
    }
  }
}
