package org.embulk.output.databricks.util;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.catalog.VolumeInfo;
import java.util.ArrayList;
import java.util.List;

public class DatabricksApiClientUtil {
  public static void deleteAllTemporaryStagingVolumes() {
    ConfigUtil.TestTask t = ConfigUtil.createTestTask();
    deleteAllTemporaryStagingVolumes(t.getCatalogName(), t.getSchemaName());
    deleteAllTemporaryStagingVolumes(t.getCatalogName(), t.getNonAsciiSchemaName());
    deleteAllTemporaryStagingVolumes(t.getNonAsciiCatalogName(), t.getSchemaName());
    deleteAllTemporaryStagingVolumes(t.getNonAsciiCatalogName(), t.getNonAsciiSchemaName());
  }

  public static void deleteAllTemporaryStagingVolumes(String catalogName, String schemaName) {
    WorkspaceClient client = createWorkspaceClient();
    fetchAllTemporaryStagingVolumes(catalogName, schemaName)
        .forEach(
            x -> {
              String name =
                  String.format("%s.%s.%s", x.getCatalogName(), x.getSchemaName(), x.getName());
              client.volumes().delete(name);
            });
  }

  public static List<VolumeInfo> fetchAllTemporaryStagingVolumes() {
    ConfigUtil.TestTask t = ConfigUtil.createTestTask();
    return fetchAllTemporaryStagingVolumes(t.getCatalogName(), t.getSchemaName());
  }

  private static List<VolumeInfo> fetchAllTemporaryStagingVolumes(
      String catalogName, String schemaName) {
    ConfigUtil.TestTask t = ConfigUtil.createTestTask();
    WorkspaceClient client = createWorkspaceClient();
    List<VolumeInfo> results = new ArrayList<>();
    client
        .volumes()
        .list(catalogName, schemaName)
        .forEach(
            x -> {
              if (x.getName().startsWith(t.getStagingVolumeNamePrefix())) {
                results.add(x);
              }
            });
    return results;
  }

  private static WorkspaceClient createWorkspaceClient() {
    ConfigUtil.TestTask t = ConfigUtil.createTestTask();
    DatabricksConfig config =
        new DatabricksConfig().setHost(t.getServerHostname()).setToken(t.getPersonalAccessToken());
    return new WorkspaceClient(config);
  }
}
