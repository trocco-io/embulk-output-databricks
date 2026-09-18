package org.embulk.output.databricks;

import com.databricks.sdk.core.DatabricksConfig;
import java.io.*;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import org.embulk.output.jdbc.JdbcOutputConnector;
import org.embulk.output.jdbc.JdbcSchema;
import org.embulk.output.jdbc.TableIdentifier;
import org.embulk.output.postgresql.AbstractPostgreSQLCopyBatchInsert;
import org.slf4j.Logger;

public class DatabricksCopyBatchInsert extends AbstractPostgreSQLCopyBatchInsert {
  private final Logger logger = org.slf4j.LoggerFactory.getLogger(getClass());
  private TableIdentifier tableIdentifier = null;
  private final ExecutorService executorService;
  private final JdbcOutputConnector connector;
  private final JdbcSchema targetTableSchema;
  private final DatabricksConfig databricksConfig;
  private final String catalogName;
  private final String schemaName;
  private final String volumeName;
  private final boolean deleteStage;
  private final boolean deleteStageOnError;
  private final boolean escapeWithEnclosing;
  private DatabricksOutputConnection connection = null;
  private final List<Future<Void>> uploadAndCopyFutures;
  private long totalRows;
  private int fileCount;

  public DatabricksCopyBatchInsert(
      JdbcOutputConnector connector,
      JdbcSchema targetTableSchema,
      DatabricksConfig databricksConfig,
      String catalogName,
      String schemaName,
      String volumeName,
      boolean deleteStage,
      boolean deleteStageOnError,
      boolean escapeWithEnclosing)
      throws IOException {
    this.connector = connector;
    this.targetTableSchema = targetTableSchema;
    this.databricksConfig = databricksConfig;
    this.catalogName = catalogName;
    this.schemaName = schemaName;
    this.volumeName = volumeName;
    // TODO: UploadThreadsPerTask
    this.executorService = Executors.newCachedThreadPool();
    this.uploadAndCopyFutures = new ArrayList<>();
    this.deleteStage = deleteStage;
    this.deleteStageOnError = deleteStageOnError;
    this.escapeWithEnclosing = escapeWithEnclosing;
  }

  // AbstractPostgreSQLCopyBatchInsert#appendDelimiter is private, so it is reimplemented here.
  private void appendEnclosedDelimiter() throws IOException {
    if (index != 0) {
      writer.write(delimiterString);
    }
    index++;
  }

  // Enclose field with double quotes. Inside the quotes:
  // - " is escaped as "" (CSV standard)
  // - \0 (null byte) is removed
  // - All other characters (\n, \t, \r, \\) are written as-is
  private void setEnclosedString(String v) throws IOException {
    writer.write('"');
    int len = v.length();
    for (int i = 0; i < len; i++) {
      char c = v.charAt(i);
      if (c == '"') {
        writer.write("\"\"");
      } else if (c != 0) {
        writer.write(c);
      }
    }
    writer.write('"');
  }

  // AbstractPostgreSQLCopyBatchInsert writes string values as backslash-escaped, unenclosed
  // PostgreSQL COPY TEXT fields, but Databricks reads the staged file with Spark's CSV reader,
  // whose quote character is enabled by default. A value such as "foo" is therefore re-interpreted
  // as an enclosed field on read and can swallow the following delimiter, shifting every later
  // column. When escapeWithEnclosing is set, string values are written as RFC 4180 enclosed fields
  // instead, and DatabricksOutputConnection#buildCopySQL configures the reader to match.
  @Override
  public void setString(String v) throws IOException {
    if (!escapeWithEnclosing) {
      super.setString(v);
      return;
    }
    appendEnclosedDelimiter();
    setEnclosedString(v);
  }

  @Override
  public void setNString(String v) throws IOException {
    if (!escapeWithEnclosing) {
      super.setNString(v);
      return;
    }
    appendEnclosedDelimiter();
    setEnclosedString(v);
  }

  // String.valueOf(byte[]) resolves to String.valueOf(Object), so this stages the array's identity
  // string (for example "[B@6d06d69c") rather than the decoded bytes. That is intentional here: it
  // is what AbstractPostgreSQLCopyBatchInsert#setBytes already does, so the option only changes how
  // a value is escaped and never what the value is. Decoding the bytes would be a separate fix and
  // belongs in the parent class, otherwise a binary column would load differently depending on
  // whether escape_with_enclosing happens to be on.
  @Override
  public void setBytes(byte[] v) throws IOException {
    if (!escapeWithEnclosing) {
      super.setBytes(v);
      return;
    }
    appendEnclosedDelimiter();
    setEnclosedString(String.valueOf(v));
  }

  @Override
  public void prepare(TableIdentifier loadTable, JdbcSchema insertSchema) throws SQLException {
    this.connection = (DatabricksOutputConnection) connector.connect(true);
    this.tableIdentifier = loadTable;
  }

  @Override
  public void close() throws IOException, SQLException {
    executorService.shutdownNow();
    try {
      executorService.awaitTermination(60, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
    }

    closeCurrentFile().delete();
    if (connection != null) {
      connection.close();
      connection = null;
    }
  }

  @Override
  public void flush() throws IOException, SQLException {
    File file = closeCurrentFile(); // flush buffered data in writer
    if (batchRows > 0) {
      String uploadFilePath =
          DatabricksAPIClient.createFilePath(
              catalogName,
              schemaName,
              volumeName,
              DatabricksAPIClient.createRandomUnityCatalogObjectName());

      UploadTask uploadTask = new UploadTask(uploadFilePath, batchRows, file);
      Future<Void> uploadFuture = executorService.submit(uploadTask);
      uploadAndCopyFutures.add(uploadFuture);

      CopyTask copyTask = new CopyTask(uploadFuture, uploadFilePath);
      uploadAndCopyFutures.add(executorService.submit(copyTask));
    }
    fileCount++;
    totalRows += batchRows;
    batchRows = 0;

    openNewFile();
  }

  @Override
  public void finish() throws SQLException {
    for (Future<Void> uploadAndCopyFuture : uploadAndCopyFutures) {
      try {
        uploadAndCopyFuture.get();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        if (e.getCause() instanceof SQLException) {
          throw (SQLException) e.getCause();
        }
        throw new RuntimeException(e);
      }
    }

    logger.info("Loaded {} files. ({} rows)", fileCount, totalRows);
  }

  private class UploadTask implements Callable<Void> {
    private final String filePath;
    private final int batchRows;
    private final File file;

    public UploadTask(String filePath, int batchRows, File file) {
      this.filePath = filePath;
      this.batchRows = batchRows;
      this.file = file;
    }

    @Override
    public Void call() throws Exception {
      // TODO retry
      logger.info(
          String.format(
              "Uploading file %s to managed volume (%,d bytes %,d rows)",
              filePath, file.length(), batchRows));
      try {
        long startTime = System.currentTimeMillis();
        DatabricksAPIClient apiClient = new DatabricksAPIClient(databricksConfig);
        apiClient.uploadFile(filePath, Files.newInputStream(file.toPath()));
        double seconds = (System.currentTimeMillis() - startTime) / 1000.0;

        logger.info(String.format("Uploaded file %s (%.2f seconds)", filePath, seconds));
      } finally {
        file.delete();
      }
      return null;
    }
  }

  private class CopyTask implements Callable<Void> {
    private final Future<Void> uploadFuture;
    private final String filePath;

    private CopyTask(Future<Void> uploadFuture, String filePath) {
      this.uploadFuture = uploadFuture;
      this.filePath = filePath;
    }

    @Override
    public Void call() throws Exception {
      try {
        uploadFuture.get();
        logger.info("Running COPY from file {}", filePath);
        try (DatabricksOutputConnection con =
            (DatabricksOutputConnection) connector.connect(true)) {
          long startTime = System.currentTimeMillis();
          con.runCopy(tableIdentifier, filePath, targetTableSchema, escapeWithEnclosing);
          double seconds = (System.currentTimeMillis() - startTime) / 1000.0;
          logger.info(String.format("Loaded file %s (%.2f seconds for COPY)", filePath, seconds));
          if (deleteStage) {
            new DatabricksAPIClient(databricksConfig).deleteFile(filePath);
          }
        }
      } catch (Exception e) {
        if (deleteStage && deleteStageOnError) {
          new DatabricksAPIClient(databricksConfig).deleteFile(filePath);
        }
        throw new RuntimeException(e);
      }
      return null;
    }
  }
}
