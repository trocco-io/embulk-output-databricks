package org.embulk.output.databricks.util;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.catalog.VolumeInfo;
import java.util.ArrayList;
import java.util.List;

public class DatabricksApiClientUtil {
  public static void deleteAllTemporaryStagingVolumes() {
    WorkspaceClient client = createWorkspaceClient();
    fetchAllTemporaryStagingVolumes()
        .forEach(
            x -> {
              String name =
                  String.format("%s.%s.%s", x.getCatalogName(), x.getSchemaName(), x.getName());
              client.volumes().delete(name);
            });
  }

  public static List<VolumeInfo> fetchAllTemporaryStagingVolumes() {
    ConfigUtil.TestTask t = ConfigUtil.createTestTask();
    WorkspaceClient client = createWorkspaceClient();
    List<VolumeInfo> results = new ArrayList<>();
    client
        .volumes()
        .list(t.getCatalogName(), t.getSchemaName())
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
