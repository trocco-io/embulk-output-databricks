package org.embulk.output.databricks.util;

import java.sql.*;
import java.util.*;
import org.embulk.config.ConfigSource;
import org.embulk.output.DatabricksOutputPlugin;

public class ConnectionUtil {
  public static Connection connect(
      String serverHostname, String httpPath, String personalAccessToken)
      throws SQLException, ClassNotFoundException {
    Class.forName("com.databricks.client.jdbc.Driver");
    String url = String.format("jdbc:databricks://%s:443", serverHostname);
    Properties props = new java.util.Properties();
    props.put("httpPath", httpPath);
    props.put("AuthMech", "3");
    props.put("UID", "token");
    props.put("PWD", personalAccessToken);
    props.put("SSL", "1");
    return DriverManager.getConnection(url, props);
  }

  public static Connection connectByTestTask() throws SQLException, ClassNotFoundException {
    ConfigUtil.TestTask testTask = ConfigUtil.createTestTask();
    return connect(
        testTask.getServerHostname(), testTask.getHTTPPath(), testTask.getPersonalAccessToken());
  }

  public static String quotedDstTableName(ConfigSource configSource) {
    DatabricksOutputPlugin.DatabricksPluginTask t = ConfigUtil.createPluginTask(configSource);
    return String.format("`%s`.`%s`.`%s`", t.getCatalogName(), t.getSchemaName(), t.getTable());
  }

  public static void dropAllTemporaryTables() {
    ConfigUtil.TestTask t = ConfigUtil.createTestTask();
    try (Connection conn = connectByTestTask()) {
      dropAllTemporaryTables(conn, t.getCatalogName(), t.getSchemaName());
      dropAllTemporaryTables(conn, t.getCatalogName(), t.getNonAsciiSchemaName());
      dropAllTemporaryTables(conn, t.getNonAsciiCatalogName(), t.getSchemaName());
      dropAllTemporaryTables(conn, t.getNonAsciiCatalogName(), t.getNonAsciiSchemaName());
    } catch (SQLException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private static void dropAllTemporaryTables(
      Connection conn, String catalogName, String schemaName) {
    ConfigUtil.TestTask t = ConfigUtil.createTestTask();
    String tableNamesSQL =
        String.format(
            "select table_name from system.information_schema.tables where table_catalog = '%s' AND table_schema = '%s' AND table_name LIKE '%s%%'",
            catalogName, schemaName, t.getTablePrefix());
    runQuery(conn, tableNamesSQL)
        .forEach(
            x -> {
              String tableName = (String) x.get("table_name");
              String dropSql =
                  String.format(
                      "drop table if exists `%s`.`%s`.`%s`", catalogName, schemaName, tableName);
              run(conn, dropSql);
            });
  }

  public static List<Map<String, Object>> runQuery(Connection conn, String query) {
    try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(query)) {
      List<Map<String, Object>> result = new ArrayList<>();
      while (rs.next()) {
        Map<String, Object> resMap = new HashMap<>();
        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
          resMap.put(rs.getMetaData().getColumnName(i), rs.getObject(i));
        }
        result.add(resMap);
      }
      return result;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public static List<Map<String, Object>> runQuery(String query) {
    try (Connection conn = connectByTestTask()) {
      return runQuery(conn, query);
    } catch (SQLException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public static Boolean run(Connection conn, String query) {
    try (Statement stmt = conn.createStatement()) {
      return stmt.execute(query);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public static Boolean run(String query) {
    try (Connection conn = connectByTestTask()) {
      return run(conn, query);
    } catch (SQLException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
