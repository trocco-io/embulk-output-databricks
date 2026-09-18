package org.embulk.output.databricks;

import static org.embulk.output.databricks.util.ConfigUtil.createPluginConfigSource;
import static org.embulk.output.databricks.util.ConnectionUtil.quotedDstTableName;
import static org.embulk.output.databricks.util.ConnectionUtil.runQuery;
import static org.embulk.output.databricks.util.IOUtil.createInputFile;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.embulk.config.ConfigSource;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.junit.Assert;
import org.junit.Test;

public class TestDatabricksOutputPluginByEscapeWithEnclosing
    extends AbstractTestDatabricksOutputPlugin {
  // A value that starts with a double quote and is followed by a line break used to swallow the
  // delimiter, pulling the next column into this one.
  @Test
  public void testQuotedValueEndingWithNewlineKeepsColumns() throws Exception {
    runOutput("\"\"\"quoted\"\"\n\"", "\"quoted\"\n", "next");
  }

  @Test
  public void testQuotedValueKeepsColumns() throws Exception {
    runOutput("\"\"\"quoted\"\"\"", "\"quoted\"", "next");
  }

  @Test
  public void testNewlineIsStoredAsNewline() throws Exception {
    runOutput("\"line1\nline2\"", "line1\nline2", "next");
  }

  @Test
  public void testTabIsStoredAsTab() throws Exception {
    runOutput("\"a\tb\"", "a\tb", "next");
  }

  // Without escape_with_enclosing the reader consumes the double quotes, leaving an empty string.
  @Test
  public void testTwoDoubleQuoteStringIsPreserved() throws Exception {
    runOutput("\"\"\"\"\"\"", "\"\"", "next");
  }

  @Test
  public void testEmptyStringStaysEmptyAndNullStaysNull() throws Exception {
    ConfigSource configSource = enclosingConfigSource();
    String quotedDstTableName = quotedDstTableName(configSource);

    File inputFile = createInputFile(testFolder, "_c0:string,_c1:string", "\"\",");
    embulk.runOutput(configSource, inputFile.toPath());

    Map<String, Object> row = selectSingleRow(quotedDstTableName);
    Assert.assertEquals("", row.get("_c0"));
    Assert.assertNull(row.get("_c1"));
  }

  /**
   * @param csvValue the first column as written in the source CSV file
   * @param expectedFirst the value the first column is expected to hold in Databricks
   * @param expectedSecond the value the second column is expected to hold in Databricks
   */
  private void runOutput(String csvValue, String expectedFirst, String expectedSecond)
      throws IOException {
    ConfigSource configSource = enclosingConfigSource();
    String quotedDstTableName = quotedDstTableName(configSource);

    File inputFile =
        createInputFile(testFolder, "_c0:string,_c1:string", csvValue + "," + expectedSecond);
    embulk.runOutput(configSource, inputFile.toPath());

    Map<String, Object> row = selectSingleRow(quotedDstTableName);
    Assert.assertEquals(expectedFirst, normalizeNewlines((String) row.get("_c0")));
    Assert.assertEquals(expectedSecond, row.get("_c1"));
  }

  /**
   * A line break inside a quoted CSV field reaches the output plugin as whatever the parser decided
   * to put there, which is not part of what this test is about: the point here is that the value
   * survives the round trip as a real line break instead of the literal two characters {@code \n},
   * and that it does not shift the following column. The exact bytes the writer emits for CR, LF
   * and CRLF are pinned by {@link TestDatabricksCopyBatchInsert} instead, which needs no parser.
   */
  private static String normalizeNewlines(String v) {
    return v == null ? null : v.replace("\r\n", "\n").replace('\r', '\n');
  }

  private ConfigSource enclosingConfigSource() {
    return createPluginConfigSource(AbstractJdbcOutputPlugin.Mode.INSERT)
        .set("escape_with_enclosing", true);
  }

  private Map<String, Object> selectSingleRow(String quotedDstTableName) {
    List<Map<String, Object>> results =
        runQuery(String.format("SELECT * FROM %s", quotedDstTableName));
    Assert.assertEquals(1, results.size());
    return results.get(0);
  }
}
