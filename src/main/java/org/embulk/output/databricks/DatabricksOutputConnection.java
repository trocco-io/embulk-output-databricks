package org.embulk.output.databricks;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
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

  // This is almost copy from JdbcOutputConnection excepting validation of table exists in current
  // schema
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
          "Databricks cannot change autocommit state. (newValue {} current {})",
          autoCommit,
          connection.getAutoCommit());
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

  public void runCopy(TableIdentifier table, String filePath, JdbcSchema jdbcSchema)
      throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      String sql = buildCopySQL(table, filePath, jdbcSchema);
      executeUpdate(stmt, sql);
      commitIfNecessary(connection);
    }
  }

  // https://docs.databricks.com/en/ingestion/copy-into/examples.html#load-csv-files-with-copy-into
  // https://docs.databricks.com/en/sql/language-manual/delta-copy-into.html
  protected String buildCopySQL(TableIdentifier table, String filePath, JdbcSchema jdbcSchema) {
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
      String quotedColumnName = quoteIdentifierString(column.getName());
      sb.append(String.format("_c%d::%s %s", i, getCreateTableTypeName(column), quotedColumnName));
    }
    sb.append(" FROM ");
    sb.append(quoteIdentifierString(filePath, "\""));
    sb.append(" )");
    sb.append(" FILEFORMAT = CSV ");
    sb.append(" FORMAT_OPTIONS (");
    sb.append(" 'nullValue' = '\\\\N' , ");
    sb.append(" 'delimiter' = '\\t' ");
    sb.append(")");
    return sb.toString();
  }

  @Override
  protected String quoteIdentifierString(String str, String quoteString) {
    // https://docs.databricks.com/en/sql/language-manual/sql-ref-identifiers.html
    if (quoteString.equals("`")) {
      return quoteString + str.replaceAll(quoteString, quoteString + quoteString) + quoteString;
    }
    return super.quoteIdentifierString(str, quoteString);
  }

  // This is almost a copy of JdbcOutputConnection except for aggregating fromTables to first from
  // table,
  // because Databricks MERGE INTO source can only specify a single table.
  @Override
  protected void collectMerge(
      List<TableIdentifier> fromTables,
      JdbcSchema schema,
      TableIdentifier toTable,
      MergeConfig mergeConfig,
      Optional<String> preSql,
      Optional<String> postSql)
      throws SQLException {
    if (fromTables.isEmpty()) {
      return;
    }

    Statement stmt = connection.createStatement();
    try {
      if (preSql.isPresent()) {
        execute(stmt, preSql.get());
      }

      TableIdentifier aggregateToTable = fromTables.get(0);
      List<TableIdentifier> aggregateFromTables =
          fromTables.stream().skip(1).collect(Collectors.toList());
      if (!aggregateFromTables.isEmpty()) {
        String aggregateSQL = buildAggregateSQL(aggregateFromTables, aggregateToTable);
        executeUpdate(stmt, aggregateSQL);
      }

      String sql = buildCollectMergeSql(aggregateToTable, schema, toTable, mergeConfig);
      executeUpdate(stmt, sql);

      if (postSql.isPresent()) {
        execute(stmt, postSql.get());
      }

      commitIfNecessary(connection);
    } catch (SQLException ex) {
      throw safeRollback(connection, ex);
    } finally {
      stmt.close();
    }
  }

  // https://github.com/embulk/embulk-output-jdbc/blob/242db4daf397fb8bfd286f5e61f8da67b51d7b31/embulk-output-redshift/src/main/java/org/embulk/output/redshift/RedshiftOutputConnection.java
  // https://docs.databricks.com/en/sql/language-manual/delta-merge-into.html
  protected String buildCollectMergeSql(
      TableIdentifier aggregateToTable,
      JdbcSchema schema,
      TableIdentifier toTable,
      MergeConfig mergeConfig)
      throws SQLException {
    StringBuilder sb = new StringBuilder();
    sb.append("MERGE INTO ");
    quoteTableIdentifier(sb, toTable);
    sb.append(" T ");
    sb.append(" USING ");
    quoteTableIdentifier(sb, aggregateToTable);
    sb.append(" S ");
    sb.append(" ON (");
    for (int i = 0; i < mergeConfig.getMergeKeys().size(); i++) {
      if (i != 0) {
        sb.append(" AND ");
      }
      String mergeKey = quoteIdentifierString(mergeConfig.getMergeKeys().get(i));
      sb.append("T.");
      sb.append(mergeKey);
      sb.append(" = S.");
      sb.append(mergeKey);
    }
    sb.append(")");
    sb.append(" WHEN MATCHED THEN ");
    sb.append(" UPDATE SET ");
    if (mergeConfig.getMergeRule().isPresent()) {
      for (int i = 0; i < mergeConfig.getMergeRule().get().size(); i++) {
        if (i != 0) {
          sb.append(", ");
        }
        sb.append(mergeConfig.getMergeRule().get().get(i));
      }
    } else {
      for (int i = 0; i < schema.getCount(); i++) {
        if (i != 0) {
          sb.append(", ");
        }
        String column = quoteIdentifierString(schema.getColumnName(i));
        sb.append(column);
        sb.append(" = S.");
        sb.append(column);
      }
    }
    sb.append(" WHEN NOT MATCHED THEN");
    sb.append(" INSERT (");
    sb.append(buildColumns(schema, ""));
    sb.append(") VALUES (");
    sb.append(buildColumns(schema, "S."));
    sb.append(");");
    return sb.toString();
  }

  protected String buildAggregateSQL(List<TableIdentifier> fromTables, TableIdentifier toTable) {
    StringBuilder sb = new StringBuilder();
    sb.append("INSERT INTO ");
    quoteTableIdentifier(sb, toTable);
    sb.append(" ( ");
    for (int i = 0; i < fromTables.size(); i++) {
      if (i != 0) {
        sb.append(" UNION ALL ");
      }
      sb.append("SELECT * FROM ");
      quoteTableIdentifier(sb, fromTables.get(i));
    }
    sb.append(" );");
    return sb.toString();
  }

  @Override
  protected String buildColumnTypeName(JdbcColumn c) {
    switch (c.getSimpleTypeName()) {
      case "CLOB":
        return "STRING";
      case "DOUBLE PRECISION":
        return "DOUBLE";
      case "FLOAT": // If not specified here, it will be recognized as FLOAT(X).
        return "FLOAT";
      default:
        return super.buildColumnTypeName(c);
    }
  }

  private String buildColumns(JdbcSchema schema, String prefix) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < schema.getCount(); i++) {
      if (i != 0) {
        sb.append(", ");
      }
      sb.append(prefix);
      sb.append(quoteIdentifierString(schema.getColumnName(i)));
    }
    return sb.toString();
  }
}
