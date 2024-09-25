package org.embulk.output.databricks;

import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;
import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.JdbcSchema;
import org.embulk.output.jdbc.MergeConfig;
import org.embulk.output.jdbc.TableIdentifier;
import org.junit.Assert;
import org.junit.Test;

public class TestDatabricksOutputConnection {

  @Test
  public void TestBuildCopySQL() throws SQLException {
    try (DatabricksOutputConnection conn = buildOutputConnection()) {
      TableIdentifier tableIdentifier = new TableIdentifier("database", "schemaName", "tableName");
      String actual = conn.buildCopySQL(tableIdentifier, "filePath", buildJdbcSchema());
      String expected =
          "COPY INTO `database`.`schemaName`.`tableName` FROM ( SELECT _c0::string `あ` , _c1::bigint ```` FROM \"filePath\" ) FILEFORMAT = CSV  FORMAT_OPTIONS ( 'nullValue' = '\\\\N' ,  'delimiter' = '\\t' )";
      Assert.assertEquals(expected, actual);
    }
  }

  @Test
  public void TestBuildAggregateSQL() throws SQLException {
    try (DatabricksOutputConnection conn = buildOutputConnection()) {
      List<TableIdentifier> fromTableIdentifiers = new ArrayList<>();
      fromTableIdentifiers.add(new TableIdentifier("database", "schemaName", "tableName0"));
      fromTableIdentifiers.add(new TableIdentifier("database", "schemaName", "tableName1"));
      TableIdentifier toTableIdentifier =
          new TableIdentifier("database", "schemaName", "tableName9");
      String actual = conn.buildAggregateSQL(fromTableIdentifiers, toTableIdentifier);
      String expected =
          "INSERT INTO `database`.`schemaName`.`tableName9` ( SELECT * FROM `database`.`schemaName`.`tableName0` UNION ALL SELECT * FROM `database`.`schemaName`.`tableName1` );";
      Assert.assertEquals(expected, actual);
    }
  }

  @Test
  public void TestMergeConfigSQLWithMergeRules() throws SQLException {
    List<String> mergeKeys = buildMergeKeys("col0", "col1");
    Optional<List<String>> mergeRules =
        buildMergeRules("col0 = CONCAT(T.col0, 'test')", "col1 = T.col1 + S.col1");
    String actual = mergeConfigSQL(new MergeConfig(mergeKeys, mergeRules));
    String expected =
        "MERGE INTO `database`.`schemaName`.`tableName100` T  USING `database`.`schemaName`.`tableName9` S  ON (T.`col0` = S.`col0` AND T.`col1` = S.`col1`) WHEN MATCHED THEN  UPDATE SET col0 = CONCAT(T.col0, 'test'), col1 = T.col1 + S.col1 WHEN NOT MATCHED THEN INSERT (`あ`, ````) VALUES (S.`あ`, S.````);";
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void TestMergeConfigSQLWithNoMergeRules() throws SQLException {
    List<String> mergeKeys = buildMergeKeys("col0", "col1");
    Optional<List<String>> mergeRules = Optional.empty();
    String actual = mergeConfigSQL(new MergeConfig(mergeKeys, mergeRules));
    String expected =
        "MERGE INTO `database`.`schemaName`.`tableName100` T  USING `database`.`schemaName`.`tableName9` S  ON (T.`col0` = S.`col0` AND T.`col1` = S.`col1`) WHEN MATCHED THEN  UPDATE SET `あ` = S.`あ`, ```` = S.```` WHEN NOT MATCHED THEN INSERT (`あ`, ````) VALUES (S.`あ`, S.````);";
    Assert.assertEquals(expected, actual);
  }

  private String mergeConfigSQL(MergeConfig mergeConfig) throws SQLException {
    try (DatabricksOutputConnection conn = buildOutputConnection()) {
      TableIdentifier aggregateToTable =
          new TableIdentifier("database", "schemaName", "tableName9");
      TableIdentifier toTable = new TableIdentifier("database", "schemaName", "tableName100");
      return conn.buildCollectMergeSql(aggregateToTable, buildJdbcSchema(), toTable, mergeConfig);
    }
  }

  private List<String> buildMergeKeys(String... keys) {
    return Arrays.asList(keys);
  }

  private Optional<List<String>> buildMergeRules(String... keys) {
    return keys.length > 0 ? Optional.of(Arrays.asList(keys)) : Optional.empty();
  }

  private DatabricksOutputConnection buildOutputConnection() throws SQLException {
    return new DatabricksOutputConnection(
        buildDummyConnection(), "defaultCatalogName", "defaultSchemaName");
  }

  private JdbcSchema buildJdbcSchema() {
    List<JdbcColumn> jdbcColumns = new ArrayList<>();
    jdbcColumns.add(JdbcColumn.newTypeDeclaredColumn("あ", Types.VARCHAR, "string", true, false));
    jdbcColumns.add(JdbcColumn.newTypeDeclaredColumn("`", Types.BIGINT, "bigint", true, false));
    return new JdbcSchema(jdbcColumns);
  }

  private Connection buildDummyConnection() {
    return new Connection() {
      @Override
      public Statement createStatement() throws SQLException {
        return new Statement() {
          @Override
          public ResultSet executeQuery(String sql) throws SQLException {
            return null;
          }

          @Override
          public int executeUpdate(String sql) throws SQLException {
            return 0;
          }

          @Override
          public void close() throws SQLException {}

          @Override
          public int getMaxFieldSize() throws SQLException {
            return 0;
          }

          @Override
          public void setMaxFieldSize(int max) throws SQLException {}

          @Override
          public int getMaxRows() throws SQLException {
            return 0;
          }

          @Override
          public void setMaxRows(int max) throws SQLException {}

          @Override
          public void setEscapeProcessing(boolean enable) throws SQLException {}

          @Override
          public int getQueryTimeout() throws SQLException {
            return 0;
          }

          @Override
          public void setQueryTimeout(int seconds) throws SQLException {}

          @Override
          public void cancel() throws SQLException {}

          @Override
          public SQLWarning getWarnings() throws SQLException {
            return null;
          }

          @Override
          public void clearWarnings() throws SQLException {}

          @Override
          public void setCursorName(String name) throws SQLException {}

          @Override
          public boolean execute(String sql) throws SQLException {
            return false;
          }

          @Override
          public ResultSet getResultSet() throws SQLException {
            return null;
          }

          @Override
          public int getUpdateCount() throws SQLException {
            return 0;
          }

          @Override
          public boolean getMoreResults() throws SQLException {
            return false;
          }

          @Override
          public void setFetchDirection(int direction) throws SQLException {}

          @Override
          public int getFetchDirection() throws SQLException {
            return 0;
          }

          @Override
          public void setFetchSize(int rows) throws SQLException {}

          @Override
          public int getFetchSize() throws SQLException {
            return 0;
          }

          @Override
          public int getResultSetConcurrency() throws SQLException {
            return 0;
          }

          @Override
          public int getResultSetType() throws SQLException {
            return 0;
          }

          @Override
          public void addBatch(String sql) throws SQLException {}

          @Override
          public void clearBatch() throws SQLException {}

          @Override
          public int[] executeBatch() throws SQLException {
            return new int[0];
          }

          @Override
          public Connection getConnection() throws SQLException {
            return null;
          }

          @Override
          public boolean getMoreResults(int current) throws SQLException {
            return false;
          }

          @Override
          public ResultSet getGeneratedKeys() throws SQLException {
            return null;
          }

          @Override
          public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
            return 0;
          }

          @Override
          public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
            return 0;
          }

          @Override
          public int executeUpdate(String sql, String[] columnNames) throws SQLException {
            return 0;
          }

          @Override
          public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
            return false;
          }

          @Override
          public boolean execute(String sql, int[] columnIndexes) throws SQLException {
            return false;
          }

          @Override
          public boolean execute(String sql, String[] columnNames) throws SQLException {
            return false;
          }

          @Override
          public int getResultSetHoldability() throws SQLException {
            return 0;
          }

          @Override
          public boolean isClosed() throws SQLException {
            return false;
          }

          @Override
          public void setPoolable(boolean poolable) throws SQLException {}

          @Override
          public boolean isPoolable() throws SQLException {
            return false;
          }

          @Override
          public void closeOnCompletion() throws SQLException {}

          @Override
          public boolean isCloseOnCompletion() throws SQLException {
            return false;
          }

          @Override
          public <T> T unwrap(Class<T> iface) throws SQLException {
            return null;
          }

          @Override
          public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return false;
          }
        };
      }

      @Override
      public PreparedStatement prepareStatement(String sql) throws SQLException {
        return null;
      }

      @Override
      public CallableStatement prepareCall(String sql) throws SQLException {
        return null;
      }

      @Override
      public String nativeSQL(String sql) throws SQLException {
        return null;
      }

      @Override
      public void setAutoCommit(boolean autoCommit) throws SQLException {}

      @Override
      public boolean getAutoCommit() throws SQLException {
        return false;
      }

      @Override
      public void commit() throws SQLException {}

      @Override
      public void rollback() throws SQLException {}

      @Override
      public void close() throws SQLException {}

      @Override
      public boolean isClosed() throws SQLException {
        return false;
      }

      @Override
      public DatabaseMetaData getMetaData() throws SQLException {
        return new DatabaseMetaData() {
          @Override
          public boolean allProceduresAreCallable() throws SQLException {
            return false;
          }

          @Override
          public boolean allTablesAreSelectable() throws SQLException {
            return false;
          }

          @Override
          public String getURL() throws SQLException {
            return null;
          }

          @Override
          public String getUserName() throws SQLException {
            return null;
          }

          @Override
          public boolean isReadOnly() throws SQLException {
            return false;
          }

          @Override
          public boolean nullsAreSortedHigh() throws SQLException {
            return false;
          }

          @Override
          public boolean nullsAreSortedLow() throws SQLException {
            return false;
          }

          @Override
          public boolean nullsAreSortedAtStart() throws SQLException {
            return false;
          }

          @Override
          public boolean nullsAreSortedAtEnd() throws SQLException {
            return false;
          }

          @Override
          public String getDatabaseProductName() throws SQLException {
            return null;
          }

          @Override
          public String getDatabaseProductVersion() throws SQLException {
            return null;
          }

          @Override
          public String getDriverName() throws SQLException {
            return null;
          }

          @Override
          public String getDriverVersion() throws SQLException {
            return null;
          }

          @Override
          public int getDriverMajorVersion() {
            return 0;
          }

          @Override
          public int getDriverMinorVersion() {
            return 0;
          }

          @Override
          public boolean usesLocalFiles() throws SQLException {
            return false;
          }

          @Override
          public boolean usesLocalFilePerTable() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsMixedCaseIdentifiers() throws SQLException {
            return false;
          }

          @Override
          public boolean storesUpperCaseIdentifiers() throws SQLException {
            return false;
          }

          @Override
          public boolean storesLowerCaseIdentifiers() throws SQLException {
            return false;
          }

          @Override
          public boolean storesMixedCaseIdentifiers() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
            return false;
          }

          @Override
          public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
            return false;
          }

          @Override
          public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
            return false;
          }

          @Override
          public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
            return false;
          }

          @Override
          public String getIdentifierQuoteString() throws SQLException {
            return "`";
          }

          @Override
          public String getSQLKeywords() throws SQLException {
            return null;
          }

          @Override
          public String getNumericFunctions() throws SQLException {
            return null;
          }

          @Override
          public String getStringFunctions() throws SQLException {
            return null;
          }

          @Override
          public String getSystemFunctions() throws SQLException {
            return null;
          }

          @Override
          public String getTimeDateFunctions() throws SQLException {
            return null;
          }

          @Override
          public String getSearchStringEscape() throws SQLException {
            return null;
          }

          @Override
          public String getExtraNameCharacters() throws SQLException {
            return null;
          }

          @Override
          public boolean supportsAlterTableWithAddColumn() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsAlterTableWithDropColumn() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsColumnAliasing() throws SQLException {
            return false;
          }

          @Override
          public boolean nullPlusNonNullIsNull() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsConvert() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsConvert(int fromType, int toType) throws SQLException {
            return false;
          }

          @Override
          public boolean supportsTableCorrelationNames() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsDifferentTableCorrelationNames() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsExpressionsInOrderBy() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsOrderByUnrelated() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsGroupBy() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsGroupByUnrelated() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsGroupByBeyondSelect() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsLikeEscapeClause() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsMultipleResultSets() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsMultipleTransactions() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsNonNullableColumns() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsMinimumSQLGrammar() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsCoreSQLGrammar() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsExtendedSQLGrammar() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsANSI92EntryLevelSQL() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsANSI92IntermediateSQL() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsANSI92FullSQL() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsIntegrityEnhancementFacility() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsOuterJoins() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsFullOuterJoins() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsLimitedOuterJoins() throws SQLException {
            return false;
          }

          @Override
          public String getSchemaTerm() throws SQLException {
            return null;
          }

          @Override
          public String getProcedureTerm() throws SQLException {
            return null;
          }

          @Override
          public String getCatalogTerm() throws SQLException {
            return null;
          }

          @Override
          public boolean isCatalogAtStart() throws SQLException {
            return false;
          }

          @Override
          public String getCatalogSeparator() throws SQLException {
            return null;
          }

          @Override
          public boolean supportsSchemasInDataManipulation() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsSchemasInProcedureCalls() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsSchemasInTableDefinitions() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsSchemasInIndexDefinitions() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsCatalogsInDataManipulation() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsCatalogsInProcedureCalls() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsCatalogsInTableDefinitions() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsPositionedDelete() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsPositionedUpdate() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsSelectForUpdate() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsStoredProcedures() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsSubqueriesInComparisons() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsSubqueriesInExists() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsSubqueriesInIns() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsSubqueriesInQuantifieds() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsCorrelatedSubqueries() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsUnion() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsUnionAll() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
            return false;
          }

          @Override
          public int getMaxBinaryLiteralLength() throws SQLException {
            return 0;
          }

          @Override
          public int getMaxCharLiteralLength() throws SQLException {
            return 0;
          }

          @Override
          public int getMaxColumnNameLength() throws SQLException {
            return 0;
          }

          @Override
          public int getMaxColumnsInGroupBy() throws SQLException {
            return 0;
          }

          @Override
          public int getMaxColumnsInIndex() throws SQLException {
            return 0;
          }

          @Override
          public int getMaxColumnsInOrderBy() throws SQLException {
            return 0;
          }

          @Override
          public int getMaxColumnsInSelect() throws SQLException {
            return 0;
          }

          @Override
          public int getMaxColumnsInTable() throws SQLException {
            return 0;
          }

          @Override
          public int getMaxConnections() throws SQLException {
            return 0;
          }

          @Override
          public int getMaxCursorNameLength() throws SQLException {
            return 0;
          }

          @Override
          public int getMaxIndexLength() throws SQLException {
            return 0;
          }

          @Override
          public int getMaxSchemaNameLength() throws SQLException {
            return 0;
          }

          @Override
          public int getMaxProcedureNameLength() throws SQLException {
            return 0;
          }

          @Override
          public int getMaxCatalogNameLength() throws SQLException {
            return 0;
          }

          @Override
          public int getMaxRowSize() throws SQLException {
            return 0;
          }

          @Override
          public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
            return false;
          }

          @Override
          public int getMaxStatementLength() throws SQLException {
            return 0;
          }

          @Override
          public int getMaxStatements() throws SQLException {
            return 0;
          }

          @Override
          public int getMaxTableNameLength() throws SQLException {
            return 0;
          }

          @Override
          public int getMaxTablesInSelect() throws SQLException {
            return 0;
          }

          @Override
          public int getMaxUserNameLength() throws SQLException {
            return 0;
          }

          @Override
          public int getDefaultTransactionIsolation() throws SQLException {
            return 0;
          }

          @Override
          public boolean supportsTransactions() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
            return false;
          }

          @Override
          public boolean supportsDataDefinitionAndDataManipulationTransactions()
              throws SQLException {
            return false;
          }

          @Override
          public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
            return false;
          }

          @Override
          public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
            return false;
          }

          @Override
          public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
            return false;
          }

          @Override
          public ResultSet getProcedures(
              String catalog, String schemaPattern, String procedureNamePattern)
              throws SQLException {
            return null;
          }

          @Override
          public ResultSet getProcedureColumns(
              String catalog,
              String schemaPattern,
              String procedureNamePattern,
              String columnNamePattern)
              throws SQLException {
            return null;
          }

          @Override
          public ResultSet getTables(
              String catalog, String schemaPattern, String tableNamePattern, String[] types)
              throws SQLException {
            return null;
          }

          @Override
          public ResultSet getSchemas() throws SQLException {
            return null;
          }

          @Override
          public ResultSet getCatalogs() throws SQLException {
            return null;
          }

          @Override
          public ResultSet getTableTypes() throws SQLException {
            return null;
          }

          @Override
          public ResultSet getColumns(
              String catalog,
              String schemaPattern,
              String tableNamePattern,
              String columnNamePattern)
              throws SQLException {
            return null;
          }

          @Override
          public ResultSet getColumnPrivileges(
              String catalog, String schema, String table, String columnNamePattern)
              throws SQLException {
            return null;
          }

          @Override
          public ResultSet getTablePrivileges(
              String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
            return null;
          }

          @Override
          public ResultSet getBestRowIdentifier(
              String catalog, String schema, String table, int scope, boolean nullable)
              throws SQLException {
            return null;
          }

          @Override
          public ResultSet getVersionColumns(String catalog, String schema, String table)
              throws SQLException {
            return null;
          }

          @Override
          public ResultSet getPrimaryKeys(String catalog, String schema, String table)
              throws SQLException {
            return null;
          }

          @Override
          public ResultSet getImportedKeys(String catalog, String schema, String table)
              throws SQLException {
            return null;
          }

          @Override
          public ResultSet getExportedKeys(String catalog, String schema, String table)
              throws SQLException {
            return null;
          }

          @Override
          public ResultSet getCrossReference(
              String parentCatalog,
              String parentSchema,
              String parentTable,
              String foreignCatalog,
              String foreignSchema,
              String foreignTable)
              throws SQLException {
            return null;
          }

          @Override
          public ResultSet getTypeInfo() throws SQLException {
            return null;
          }

          @Override
          public ResultSet getIndexInfo(
              String catalog, String schema, String table, boolean unique, boolean approximate)
              throws SQLException {
            return null;
          }

          @Override
          public boolean supportsResultSetType(int type) throws SQLException {
            return false;
          }

          @Override
          public boolean supportsResultSetConcurrency(int type, int concurrency)
              throws SQLException {
            return false;
          }

          @Override
          public boolean ownUpdatesAreVisible(int type) throws SQLException {
            return false;
          }

          @Override
          public boolean ownDeletesAreVisible(int type) throws SQLException {
            return false;
          }

          @Override
          public boolean ownInsertsAreVisible(int type) throws SQLException {
            return false;
          }

          @Override
          public boolean othersUpdatesAreVisible(int type) throws SQLException {
            return false;
          }

          @Override
          public boolean othersDeletesAreVisible(int type) throws SQLException {
            return false;
          }

          @Override
          public boolean othersInsertsAreVisible(int type) throws SQLException {
            return false;
          }

          @Override
          public boolean updatesAreDetected(int type) throws SQLException {
            return false;
          }

          @Override
          public boolean deletesAreDetected(int type) throws SQLException {
            return false;
          }

          @Override
          public boolean insertsAreDetected(int type) throws SQLException {
            return false;
          }

          @Override
          public boolean supportsBatchUpdates() throws SQLException {
            return false;
          }

          @Override
          public ResultSet getUDTs(
              String catalog, String schemaPattern, String typeNamePattern, int[] types)
              throws SQLException {
            return null;
          }

          @Override
          public Connection getConnection() throws SQLException {
            return null;
          }

          @Override
          public boolean supportsSavepoints() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsNamedParameters() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsMultipleOpenResults() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsGetGeneratedKeys() throws SQLException {
            return false;
          }

          @Override
          public ResultSet getSuperTypes(
              String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
            return null;
          }

          @Override
          public ResultSet getSuperTables(
              String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
            return null;
          }

          @Override
          public ResultSet getAttributes(
              String catalog,
              String schemaPattern,
              String typeNamePattern,
              String attributeNamePattern)
              throws SQLException {
            return null;
          }

          @Override
          public boolean supportsResultSetHoldability(int holdability) throws SQLException {
            return false;
          }

          @Override
          public int getResultSetHoldability() throws SQLException {
            return 0;
          }

          @Override
          public int getDatabaseMajorVersion() throws SQLException {
            return 0;
          }

          @Override
          public int getDatabaseMinorVersion() throws SQLException {
            return 0;
          }

          @Override
          public int getJDBCMajorVersion() throws SQLException {
            return 0;
          }

          @Override
          public int getJDBCMinorVersion() throws SQLException {
            return 0;
          }

          @Override
          public int getSQLStateType() throws SQLException {
            return 0;
          }

          @Override
          public boolean locatorsUpdateCopy() throws SQLException {
            return false;
          }

          @Override
          public boolean supportsStatementPooling() throws SQLException {
            return false;
          }

          @Override
          public RowIdLifetime getRowIdLifetime() throws SQLException {
            return null;
          }

          @Override
          public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
            return null;
          }

          @Override
          public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
            return false;
          }

          @Override
          public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
            return false;
          }

          @Override
          public ResultSet getClientInfoProperties() throws SQLException {
            return null;
          }

          @Override
          public ResultSet getFunctions(
              String catalog, String schemaPattern, String functionNamePattern)
              throws SQLException {
            return null;
          }

          @Override
          public ResultSet getFunctionColumns(
              String catalog,
              String schemaPattern,
              String functionNamePattern,
              String columnNamePattern)
              throws SQLException {
            return null;
          }

          @Override
          public ResultSet getPseudoColumns(
              String catalog,
              String schemaPattern,
              String tableNamePattern,
              String columnNamePattern)
              throws SQLException {
            return null;
          }

          @Override
          public boolean generatedKeyAlwaysReturned() throws SQLException {
            return false;
          }

          @Override
          public <T> T unwrap(Class<T> iface) throws SQLException {
            return null;
          }

          @Override
          public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return false;
          }
        };
      }

      @Override
      public void setReadOnly(boolean readOnly) throws SQLException {}

      @Override
      public boolean isReadOnly() throws SQLException {
        return false;
      }

      @Override
      public void setCatalog(String catalog) throws SQLException {}

      @Override
      public String getCatalog() throws SQLException {
        return null;
      }

      @Override
      public void setTransactionIsolation(int level) throws SQLException {}

      @Override
      public int getTransactionIsolation() throws SQLException {
        return 0;
      }

      @Override
      public SQLWarning getWarnings() throws SQLException {
        return null;
      }

      @Override
      public void clearWarnings() throws SQLException {}

      @Override
      public Statement createStatement(int resultSetType, int resultSetConcurrency)
          throws SQLException {
        return null;
      }

      @Override
      public PreparedStatement prepareStatement(
          String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return null;
      }

      @Override
      public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
          throws SQLException {
        return null;
      }

      @Override
      public Map<String, Class<?>> getTypeMap() throws SQLException {
        return null;
      }

      @Override
      public void setTypeMap(Map<String, Class<?>> map) throws SQLException {}

      @Override
      public void setHoldability(int holdability) throws SQLException {}

      @Override
      public int getHoldability() throws SQLException {
        return 0;
      }

      @Override
      public Savepoint setSavepoint() throws SQLException {
        return null;
      }

      @Override
      public Savepoint setSavepoint(String name) throws SQLException {
        return null;
      }

      @Override
      public void rollback(Savepoint savepoint) throws SQLException {}

      @Override
      public void releaseSavepoint(Savepoint savepoint) throws SQLException {}

      @Override
      public Statement createStatement(
          int resultSetType, int resultSetConcurrency, int resultSetHoldability)
          throws SQLException {
        return null;
      }

      @Override
      public PreparedStatement prepareStatement(
          String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
          throws SQLException {
        return null;
      }

      @Override
      public CallableStatement prepareCall(
          String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
          throws SQLException {
        return null;
      }

      @Override
      public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
          throws SQLException {
        return null;
      }

      @Override
      public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
          throws SQLException {
        return null;
      }

      @Override
      public PreparedStatement prepareStatement(String sql, String[] columnNames)
          throws SQLException {
        return null;
      }

      @Override
      public Clob createClob() throws SQLException {
        return null;
      }

      @Override
      public Blob createBlob() throws SQLException {
        return null;
      }

      @Override
      public NClob createNClob() throws SQLException {
        return null;
      }

      @Override
      public SQLXML createSQLXML() throws SQLException {
        return null;
      }

      @Override
      public boolean isValid(int timeout) throws SQLException {
        return false;
      }

      @Override
      public void setClientInfo(String name, String value) throws SQLClientInfoException {}

      @Override
      public void setClientInfo(Properties properties) throws SQLClientInfoException {}

      @Override
      public String getClientInfo(String name) throws SQLException {
        return null;
      }

      @Override
      public Properties getClientInfo() throws SQLException {
        return null;
      }

      @Override
      public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return null;
      }

      @Override
      public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return null;
      }

      @Override
      public void setSchema(String schema) throws SQLException {}

      @Override
      public String getSchema() throws SQLException {
        return null;
      }

      @Override
      public void abort(Executor executor) throws SQLException {}

      @Override
      public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {}

      @Override
      public int getNetworkTimeout() throws SQLException {
        return 0;
      }

      @Override
      public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
      }

      @Override
      public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
      }
    };
  }
}
