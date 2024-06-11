package org.embulk.output.databricks;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;
import org.embulk.output.jdbc.AbstractJdbcOutputConnector;
import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.TransactionIsolation;

public class DatabricksOutputConnector extends AbstractJdbcOutputConnector {
  private final String url;
  private final Properties properties;

  private final String catalogName;

  private final String schemaName;

  public DatabricksOutputConnector(
      String url,
      Properties properties,
      Optional<TransactionIsolation> transactionIsolation,
      String catalogName,
      String schemaName) {
    super(transactionIsolation);
    this.url = url;
    this.properties = properties;
    this.catalogName = catalogName;
    this.schemaName = schemaName;
  }

  @Override
  protected JdbcOutputConnection connect() throws SQLException {
    Connection c = DriverManager.getConnection(url, properties);
    try {
      DatabricksOutputConnection con = new DatabricksOutputConnection(c, catalogName, schemaName);
      c = null;
      return con;
    } finally {
      if (c != null) {
        c.close();
      }
    }
  }
}
