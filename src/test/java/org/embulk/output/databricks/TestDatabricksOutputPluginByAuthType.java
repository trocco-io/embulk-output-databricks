package org.embulk.output.databricks;

import static org.embulk.output.databricks.util.ConfigUtil.createEmptyConfigSource;
import static org.embulk.output.databricks.util.ConnectionUtil.quotedDstTableName;
import static org.embulk.output.databricks.util.ConnectionUtil.runQuery;
import static org.embulk.output.databricks.util.IOUtil.createInputFile;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.exec.PartialExecutionException;
import org.embulk.output.databricks.util.ConfigUtil;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.junit.Assert;
import org.junit.Test;

public class TestDatabricksOutputPluginByAuthType extends AbstractTestDatabricksOutputPlugin {
  @Test
  public void testAuthTypeDefault() throws IOException {
    final ConfigUtil.TestTask t = ConfigUtil.createTestTask();
    testRun((x) -> x.set("personal_access_token", t.getPersonalAccessToken()));
  }

  @Test
  public void testAuthTypePat() throws IOException {
    final ConfigUtil.TestTask t = ConfigUtil.createTestTask();
    testRun(
        (x) -> x.set("auth_type", "pat").set("personal_access_token", t.getPersonalAccessToken()));
  }

  @Test
  public void testAtuTypePatWithoutPersonalAccessToken() {
    testConfigException((x) -> x.set("auth_type", "pat"), "personal_access_token");
  }

  @Test
  public void testAuthTypeOauthM2M() throws IOException {
    final ConfigUtil.TestTask t = ConfigUtil.createTestTask();
    testRun(
        (x) ->
            x.set("auth_type", "oauth-m2m")
                .set("oauth2_client_id", t.getOauth2ClientId())
                .set("oauth2_client_secret", t.getOauth2ClientSecret()));
  }

  @Test
  public void testAuthTypeM2MOauthWithoutOauth2ClientId() {
    final ConfigUtil.TestTask t = ConfigUtil.createTestTask();
    testConfigException(
        (x) ->
            x.set("auth_type", "oauth-m2m").set("oauth2_client_secret", t.getOauth2ClientSecret()),
        "oauth2_client_id");
  }

  @Test
  public void testAuthTypeM2MOauthWithoutOauth2ClientSecret() {
    final ConfigUtil.TestTask t = ConfigUtil.createTestTask();
    testConfigException(
        (x) -> x.set("auth_type", "oauth-m2m").set("oauth2_client_id", t.getOauth2ClientId()),
        "oauth2_client_secret");
  }

  @Test
  public void testInvalidAuthType() {
    testConfigException((x) -> x.set("auth_type", "invalid"), "auth_type");
  }

  private void testRun(Function<ConfigSource, ConfigSource> setConfigSource) throws IOException {
    ConfigSource configSource = setConfigSource.apply(createBaseConfigSource());
    String quotedDstTableName = quotedDstTableName(configSource);

    File inputFile = createInputFile(testFolder, "_c0:string", "test");
    embulk.runOutput(configSource, inputFile.toPath());

    String sql = String.format("SELECT * FROM %s ORDER BY _c0", quotedDstTableName);
    List<Map<String, Object>> results = runQuery(sql);
    Assert.assertEquals(1, results.size());
    Assert.assertEquals("test", results.get(0).get("_c0"));
  }

  private void testConfigException(
      Function<ConfigSource, ConfigSource> setConfigSource, String containedMessage) {
    ConfigSource configSource = setConfigSource.apply(createBaseConfigSource());
    Exception e =
        Assert.assertThrows(
            PartialExecutionException.class,
            () ->
                embulk.runOutput(
                    setConfigSource.apply(configSource),
                    createInputFile(testFolder, "_c0:string", "test").toPath()));
    Assert.assertTrue(e.getCause() instanceof ConfigException);
    Assert.assertTrue(
        String.format("「%s」 does not contains '%s'", e.getMessage(), containedMessage),
        e.getMessage().contains(containedMessage));
  }

  private ConfigSource createBaseConfigSource() {
    final ConfigUtil.TestTask t = ConfigUtil.createTestTask();
    return createEmptyConfigSource()
        .set("type", "databricks")
        .set("server_hostname", t.getServerHostname())
        .set("http_path", t.getHTTPPath())
        .set("catalog_name", t.getCatalogName())
        .set("schema_name", t.getSchemaName())
        .set("mode", AbstractJdbcOutputPlugin.Mode.INSERT)
        .set("delete_stage_on_error", true)
        .set("delete_stage", true)
        .set("staging_volume_name_prefix", t.getStagingVolumeNamePrefix())
        .set("table", t.getTablePrefix() + "_dst");
  }
}
