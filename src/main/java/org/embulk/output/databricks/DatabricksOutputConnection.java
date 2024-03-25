package org.embulk.output.databricks;

import java.sql.*;
import java.util.*;
import org.embulk.output.jdbc.*;

public class DatabricksOutputConnection extends JdbcOutputConnection {
  final String catalogName;

  public DatabricksOutputConnection(Connection connection, String catalogName, String schemaName)
      throws SQLException {
    super(connection, schemaName);
    this.catalogName = catalogName;
    useCatalog(catalogName);
    useSchema(schemaName);
  }

  @Override
  protected void setSearchPath(String schema) throws SQLException {
    // There is nothing to do here as the schema needs to be configured after the catalogue has been
    // set up.
    // Also, the command to set the schema is unique to Databricks.
  }

  protected void useCatalog(String catalog) throws SQLException {
    // https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-use-catalog.html
    executeUseStatement("USE CATALOG " + quoteIdentifierString(catalog));
  }

  protected void useSchema(String schema) throws SQLException {
    // https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-use-schema.html
    executeUseStatement("USE SCHEMA " + quoteIdentifierString(schema));
  }

  protected void executeUseStatement(String sql) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      executeUpdate(stmt, sql);
    }
  }

  // TODO This is almost copy from JdbcOutputConnection excepting validation of table exists in current schema

  public boolean tableExists(TableIdentifier table) throws SQLException {
    try (ResultSet rs =
        connection
            .getMetaData()
            .getTables(table.getDatabase(), table.getSchemaName(), table.getTableName(), null)) {
      while (rs.next()) {
        if (isAvailableTableMetadataInConnection(rs, table)) {
          return true;
        }
      }
    }
    return false;
  }

  public boolean isAvailableTableMetadataInConnection(ResultSet rs, TableIdentifier tableIdentifier)
      throws SQLException {
    // If unchecked, tables in other catalogs may appear to exist.
    // This is because the base embulk jdbc plugin's tableIdentifier.getDatabase() is often returns
    // null
    // and one Databricks connection has multiple available catalogs　(databases).

    if (tableIdentifier.getDatabase() == null) {
      logger.trace("tableIdentifier.getDatabase() == null, check by instance variable");
      if (!rs.getString("TABLE_CAT").equalsIgnoreCase(catalogName)) {
        return false;
      }
    }
    if (tableIdentifier.getSchemaName() == null) {
      logger.trace("tableIdentifier.getSchemaName() == null, check by instance variable");
      if (!rs.getString("TABLE_SCHEM").equalsIgnoreCase(schemaName)) {
        return false;
      }
    }

    if (tableIdentifier.getDatabase() != null
        && !tableIdentifier.getDatabase().equalsIgnoreCase(catalogName)) {
      logger.error(
          String.format(
              "tableIdentifier.getSchemaName() != instance variable. (%s, %s)",
              tableIdentifier.getDatabase(), catalogName));
    }
    if (tableIdentifier.getSchemaName() != null
        && !tableIdentifier.getSchemaName().equalsIgnoreCase(schemaName)) {
      logger.error(
          String.format(
              "tableIdentifier.getSchemaName() != instance variable. (%s, %s)",
              tableIdentifier.getSchemaName(), schemaName));
    }
    return true;
  }

  @Override
  public void initialize(boolean autoCommit, Optional<TransactionIsolation> transactionIsolation)
      throws SQLException {
    if (connection.getAutoCommit() != autoCommit) {
      logger.trace(
          "Databricks cannot change autocommit state. (newValue {} current {})", autoCommit, connection.getAutoCommit());
    }

    if (transactionIsolation.isPresent()) {
      connection.setTransactionIsolation(transactionIsolation.get().toInt());
    }

    try {
      TransactionIsolation currentTransactionIsolation =
          TransactionIsolation.fromInt(connection.getTransactionIsolation());
      logger.info("TransactionIsolation={}", currentTransactionIsolation.toString());
    } catch (IllegalArgumentException e) {
      logger.info("TransactionIsolation=unknown");
    }
  }

  public void runCopy(
      TableIdentifier table, String filePath, JdbcSchema jdbcSchema, String delimiterString)
      throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      String sql = buildCopySQL(table, filePath, jdbcSchema, delimiterString);
      executeUpdate(stmt, sql);
      commitIfNecessary(connection);
    }
  }

  // https://docs.databricks.com/en/ingestion/copy-into/examples.html#load-csv-files-with-copy-into
  // https://docs.databricks.com/en/sql/language-manual/delta-copy-into.html
  private String buildCopySQL(
      TableIdentifier table, String filePath, JdbcSchema jdbcSchema, String delimiterString) {
    StringBuilder sb = new StringBuilder();
    sb.append("COPY INTO ");
    quoteTableIdentifier(sb, table);
    sb.append(" FROM ( SELECT ");
    final List<JdbcColumn> columns = jdbcSchema.getColumns();
    for (int i = 0; i < columns.size(); i++) {
      final JdbcColumn column = columns.get(i);
      if (column.isSkipColumn()) {
        continue;
      }
      if (i != 0) {
        sb.append(" , ");
      }
      sb.append(String.format("_c%d::%s %s", i, getCreateTableTypeName(column), column.getName()));
    }
    sb.append(" FROM ");
    sb.append(quoteIdentifierString(filePath, "\""));
    sb.append(" )");
    sb.append(" FILEFORMAT = CSV ");
    sb.append(" FORMAT_OPTIONS (");
    sb.append(" 'delimiter' = '");
    sb.append(delimiterString);
    sb.append("'");
    sb.append(")");
    return sb.toString();
  }

  @Override
  protected String buildColumnTypeName(JdbcColumn c) {
    if (c.getSimpleTypeName().equals("CLOB")) {
      return "VARCHAR(65535)";
    }
    return super.buildColumnTypeName(c);
  }
}
