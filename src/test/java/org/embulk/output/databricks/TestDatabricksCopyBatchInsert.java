package org.embulk.output.databricks;

import static org.junit.Assert.assertEquals;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import org.junit.Test;

public class TestDatabricksCopyBatchInsert {
  /**
   * Exposes the staged file and its writer, which AbstractPostgreSQLCopyBatchInsert keeps
   * protected.
   */
  private static class TestableCopyBatchInsert extends DatabricksCopyBatchInsert {
    TestableCopyBatchInsert(boolean escapeWithEnclosing) throws IOException {
      super(null, null, null, "catalog", "schema", "volume", false, false, escapeWithEnclosing);
    }

    File stagedFile() {
      return currentFile;
    }

    BufferedWriter stagedWriter() {
      return writer;
    }
  }

  private String writeAndRead(TestableCopyBatchInsert batch) throws Exception {
    File file = batch.stagedFile();
    batch.stagedWriter().close();
    try {
      return new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
    } finally {
      file.delete();
    }
  }

  // Without the option, the inherited PostgreSQL COPY TEXT escaping must be untouched.

  @Test
  public void testSetStringWithoutEnclosing() throws Exception {
    TestableCopyBatchInsert batch = new TestableCopyBatchInsert(false);
    batch.setString("hello\tworld\n");
    batch.add();

    assertEquals("hello\\tworld\\n\n", writeAndRead(batch));
  }

  @Test
  public void testSetStringWithoutEnclosingKeepsDoubleQuoteBare() throws Exception {
    TestableCopyBatchInsert batch = new TestableCopyBatchInsert(false);
    batch.setString("\"quoted\"");
    batch.add();

    assertEquals("\"quoted\"\n", writeAndRead(batch));
  }

  // With the option, string values become RFC 4180 enclosed fields.

  @Test
  public void testSetStringWithEnclosing() throws Exception {
    TestableCopyBatchInsert batch = new TestableCopyBatchInsert(true);
    batch.setString("hello\tworld\n");
    batch.add();

    assertEquals("\"hello\tworld\n\"\n", writeAndRead(batch));
  }

  @Test
  public void testEnclosingEscapesDoubleQuotes() throws Exception {
    TestableCopyBatchInsert batch = new TestableCopyBatchInsert(true);
    batch.setString("say \"hello\"");
    batch.add();

    assertEquals("\"say \"\"hello\"\"\"\n", writeAndRead(batch));
  }

  // The column shift reported for unenclosed values: a value that both starts with a double quote
  // and is followed by a backslash escape used to swallow the delimiter and the next column.
  @Test
  public void testEnclosingKeepsColumnsSeparatedForQuotedValueEndingWithNewline() throws Exception {
    TestableCopyBatchInsert batch = new TestableCopyBatchInsert(true);
    batch.setString("\"quoted\"\n");
    batch.setString("next");
    batch.add();

    assertEquals("\"\"\"quoted\"\"\n\"\t\"next\"\n", writeAndRead(batch));
  }

  @Test
  public void testEnclosingRemovesNullByte() throws Exception {
    TestableCopyBatchInsert batch = new TestableCopyBatchInsert(true);
    batch.setString("hello\0world");
    batch.add();

    assertEquals("\"helloworld\"\n", writeAndRead(batch));
  }

  @Test
  public void testEnclosingPreservesBackslash() throws Exception {
    TestableCopyBatchInsert batch = new TestableCopyBatchInsert(true);
    batch.setString("path\\to\\file");
    batch.add();

    assertEquals("\"path\\to\\file\"\n", writeAndRead(batch));
  }

  @Test
  public void testEnclosingPreservesCarriageReturn() throws Exception {
    TestableCopyBatchInsert batch = new TestableCopyBatchInsert(true);
    batch.setString("line1\r\nline2");
    batch.add();

    assertEquals("\"line1\r\nline2\"\n", writeAndRead(batch));
  }

  @Test
  public void testEnclosingEmptyString() throws Exception {
    TestableCopyBatchInsert batch = new TestableCopyBatchInsert(true);
    batch.setString("");
    batch.add();

    assertEquals("\"\"\n", writeAndRead(batch));
  }

  // NULL stays an unenclosed \N so that it keeps matching the nullValue format option.
  @Test
  public void testEnclosingLeavesNullUnenclosed() throws Exception {
    TestableCopyBatchInsert batch = new TestableCopyBatchInsert(true);
    batch.setNull(java.sql.Types.VARCHAR);
    batch.setString("value");
    batch.add();

    assertEquals("\\N\t\"value\"\n", writeAndRead(batch));
  }

  // Non-string values stay unenclosed; Spark's CSV reader applies no escaping outside quotes.
  @Test
  public void testEnclosingLeavesNumericUnenclosed() throws Exception {
    TestableCopyBatchInsert batch = new TestableCopyBatchInsert(true);
    batch.setLong(123L);
    batch.setBoolean(true);
    batch.setString("value");
    batch.add();

    assertEquals("123\ttrue\t\"value\"\n", writeAndRead(batch));
  }

  @Test
  public void testMultipleColumnsWithEnclosing() throws Exception {
    TestableCopyBatchInsert batch = new TestableCopyBatchInsert(true);
    batch.setString("col1\nvalue");
    batch.setString("col2\tvalue");
    batch.add();

    assertEquals("\"col1\nvalue\"\t\"col2\tvalue\"\n", writeAndRead(batch));
  }

  @Test
  public void testMultipleRowsWithEnclosing() throws Exception {
    TestableCopyBatchInsert batch = new TestableCopyBatchInsert(true);
    batch.setString("a");
    batch.add();
    batch.setString("b");
    batch.add();

    assertEquals("\"a\"\n\"b\"\n", writeAndRead(batch));
  }
}
