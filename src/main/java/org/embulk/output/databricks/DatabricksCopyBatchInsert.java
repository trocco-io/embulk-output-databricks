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
      boolean deleteStageOnError)
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
          con.runCopy(tableIdentifier, filePath, targetTableSchema, delimiterString);
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
