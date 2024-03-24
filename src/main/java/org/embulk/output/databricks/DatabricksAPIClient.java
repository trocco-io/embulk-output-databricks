package org.embulk.output.databricks;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.catalog.VolumeType;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import org.embulk.output.DatabricksOutputPlugin;

public class DatabricksAPIClient {
  public static DatabricksAPIClient create(DatabricksOutputPlugin.DatabricksPluginTask task) {
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

  public static DatabricksConfig createDatabricksConfig(
      DatabricksOutputPlugin.DatabricksPluginTask task) {
    return new DatabricksConfig()
        .setHost(task.getServerHostname())
        .setToken(task.getPersonalAccessToken());
  }

  public static String createFilePath(
      String catalogName, String schemaName, String volumeName, String fileName) {
    return String.format("/Volumes/%s/%s/%s/%s", catalogName, schemaName, volumeName, fileName);
  }

  private static String currentTransactionVolumeName = null;

  public static String fetchCurrentTransactionVolumeName() {
    if (currentTransactionVolumeName == null) {
      currentTransactionVolumeName = createRandomUnityCatalogObjectName();
    }
    return currentTransactionVolumeName;
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
