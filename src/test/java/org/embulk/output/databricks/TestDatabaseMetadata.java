package org.embulk.output.databricks;

import static java.lang.String.format;
import static org.embulk.output.databricks.util.ConnectionUtil.*;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.embulk.output.databricks.util.ConfigUtil;
import org.embulk.output.jdbc.JdbcUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

// The purpose of this class is to understand  the behavior of DatabaseMetadata,
// so if this test fails due to a library update, please change the test result.
public class TestDatabaseMetadata {
  private DatabaseMetaData dbm;
  private Connection conn;

  ConfigUtil.TestTask t = ConfigUtil.createTestTask();
  String catalog = t.getCatalogName();
  String schema = t.getSchemaName();
  String table = t.getTablePrefix() + "_test";
  String nonAsciiCatalog = t.getNonAsciiCatalogName();
  String nonAsciiSchema = t.getNonAsciiSchemaName();
  String nonAsciiTable = t.getTablePrefix() + "_テスト";

  @Before
  public void setup() throws SQLException, ClassNotFoundException {
    conn = connectByTestTask();
    dbm = conn.getMetaData();
    run(conn, "USE CATALOG " + catalog);
    run(conn, "USE SCHEMA " + schema);
    createTables();
  }

  @After
  public void cleanup() {
    try {
      conn.close();
    } catch (SQLException ignored) {

    }
    dropAllTemporaryTables();
  }

  @Test
  public void testGetPrimaryKeys() throws SQLException {
    assertEquals(1, countPrimaryKeys(catalog, schema, table, "a0"));
    assertEquals(1, countPrimaryKeys(null, schema, table, "a0"));
    assertEquals(1, countPrimaryKeys(nonAsciiCatalog, nonAsciiSchema, nonAsciiTable, "h0"));
    assertEquals(1, countPrimaryKeys(null, nonAsciiSchema, nonAsciiTable, "d0"));
  }

  @Test
  public void testGetTables() throws SQLException {
    assertEquals(1, countTablesResult(catalog, schema, table));
    assertEquals(2, countTablesResult(null, schema, table));
    assertEquals(1, countTablesResult(nonAsciiCatalog, nonAsciiSchema, nonAsciiTable));
    assertEquals(0, countTablesResult(null, nonAsciiSchema, nonAsciiTable)); // expected 2
  }

  @Test
  public void testGetColumns() throws SQLException {
    assertEquals(2, countColumnsResult(catalog, schema, table));
    assertEquals(4, countColumnsResult(null, schema, table));
    assertEquals(2, countColumnsResult(nonAsciiCatalog, nonAsciiSchema, nonAsciiTable));
    assertEquals(0, countColumnsResult(null, nonAsciiSchema, nonAsciiTable)); // expected 2
  }

  private void createTables() {
    String queryFormat =
        "CREATE TABLE IF NOT EXISTS `%s`.`%s`.`%s` (%s String PRIMARY KEY, %s INTEGER)";
    run(conn, format(queryFormat, catalog, schema, table, "a0", "a1"));
    run(conn, format(queryFormat, catalog, schema, nonAsciiTable, "b0", "b1"));
    run(conn, format(queryFormat, catalog, nonAsciiSchema, table, "c0", "c1"));
    run(conn, format(queryFormat, catalog, nonAsciiSchema, nonAsciiTable, "d0", "d1"));
    run(conn, format(queryFormat, nonAsciiCatalog, schema, table, "e0", "e1"));
    run(conn, format(queryFormat, nonAsciiCatalog, schema, nonAsciiTable, "f0", "f1"));
    run(conn, format(queryFormat, nonAsciiCatalog, nonAsciiSchema, table, "g0", "g1"));
    run(conn, format(queryFormat, nonAsciiCatalog, nonAsciiSchema, nonAsciiTable, "h0", "h1"));
  }

  private int countPrimaryKeys(
      String catalogName, String schemaName, String tableName, String primaryKey)
      throws SQLException {
    try (ResultSet rs = dbm.getPrimaryKeys(catalogName, schemaName, tableName)) {
      int count = 0;
      while (rs.next()) {
        String columnName = rs.getString("COLUMN_NAME");
        assertEquals(primaryKey, columnName);
        count += 1;
      }
      return count;
    }
  }

  private int countTablesResult(String catalogName, String schemaName, String tableName)
      throws SQLException {
    String e = dbm.getSearchStringEscape();
    String c = JdbcUtils.escapeSearchString(catalogName, e);
    String s = JdbcUtils.escapeSearchString(schemaName, e);
    String t = JdbcUtils.escapeSearchString(tableName, e);
    try (ResultSet rs = dbm.getTables(c, s, t, null)) {
      return countResultSet(rs);
    }
  }

  private int countColumnsResult(String catalogName, String schemaName, String tableName)
      throws SQLException {
    String e = dbm.getSearchStringEscape();
    String c = JdbcUtils.escapeSearchString(catalogName, e);
    String s = JdbcUtils.escapeSearchString(schemaName, e);
    String t = JdbcUtils.escapeSearchString(tableName, e);
    try (ResultSet rs = dbm.getColumns(c, s, t, null)) {
      return countResultSet(rs);
    }
  }

  private int countResultSet(ResultSet rs) throws SQLException {
    int count = 0;
    while (rs.next()) {
      count += 1;
    }
    return count;
  }
}
