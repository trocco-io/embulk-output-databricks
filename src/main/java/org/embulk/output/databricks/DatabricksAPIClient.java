package org.embulk.output.databricks;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.catalog.VolumeType;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import org.embulk.config.ConfigException;
import org.embulk.output.DatabricksOutputPlugin.DatabricksPluginTask;

public class DatabricksAPIClient {
  public static DatabricksAPIClient create(DatabricksPluginTask task) {
    return new DatabricksAPIClient(createDatabricksConfig(task));
  }

  private final WorkspaceClient workspaceClient;

  public DatabricksAPIClient(DatabricksConfig config) {
    workspaceClient = new WorkspaceClient(config);
  }

  public void createVolume(String catalogName, String schemaName, String volumeName) {
    // https://docs.databricks.com/api/workspace/volumes/create
    workspaceClient.volumes().create(catalogName, schemaName, volumeName, VolumeType.MANAGED);
  }

  public void deleteVolume(String catalogName, String schemaName, String volumeName) {
    // https://docs.databricks.com/api/workspace/volumes/delete
    String name = String.format("%s.%s.%s", catalogName, schemaName, volumeName);
    workspaceClient.volumes().delete(name);
  }

  public void uploadFile(String filePath, InputStream inputStream) {
    // https://docs.databricks.com/api/workspace/files/upload
    workspaceClient.files().upload(filePath, inputStream);
  }

  public void deleteFile(String filePath) {
    // https://docs.databricks.com/api/workspace/files/upload
    workspaceClient.files().delete(filePath);
  }

  public static DatabricksConfig createDatabricksConfig(DatabricksPluginTask task) {
    DatabricksConfig config = new DatabricksConfig().setHost(task.getServerHostname());
    String authType = task.getAuthType();
    config.setAuthType(authType);
    switch (authType) {
      case "pat":
        config.setToken(DatabricksPluginTask.fetchPersonalAccessToken(task));
        break;
      case "oauth-m2m":
        config.setClientId(DatabricksPluginTask.fetchOauth2ClientId(task));
        config.setClientSecret(DatabricksPluginTask.fetchOauth2ClientSecret(task));
        break;
      default:
        throw new ConfigException(String.format("unknown auth_type '%s'", authType));
    }
    return config;
  }

  public static String createFilePath(
      String catalogName, String schemaName, String volumeName, String fileName) {
    return String.format("/Volumes/%s/%s/%s/%s", catalogName, schemaName, volumeName, fileName);
  }

  private static String currentTransactionVolumeName = null;

  public static String fetchCurrentTransactionVolumeName(String prefix) {
    if (currentTransactionVolumeName == null) {
      currentTransactionVolumeName = prefix + createRandomUnityCatalogObjectName();
    }
    return currentTransactionVolumeName;
  }

  public static void resetFetchCurrentTransactionVolumeName() {
    currentTransactionVolumeName = null;
  }

  public static String createRandomUnityCatalogObjectName() {
    // https://docs.databricks.com/en/sql/language-manual/sql-ref-names.html
    // https://docs.databricks.com/en/sql/language-manual/sql-ref-identifiers.html
    Date now = new Date();
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
    String randomSuffix = UUID.randomUUID().toString().replace("-", "");
    return dateFormat.format(now) + "TEST_" + randomSuffix;
  }
}
