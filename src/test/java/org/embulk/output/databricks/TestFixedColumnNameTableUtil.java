package org.embulk.output.databricks;

import static org.embulk.output.databricks.util.FixedColumnNameTableUtil.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class TestFixedColumnNameTableUtil {
  @Test
  public void TestCreateTableSQL() {
    Assert.assertEquals(
        "CREATE TABLE test (_c0 string, _c1 long)", createTableSQL("test", "string", "long"));
  }

  @Test
  public void TestInsertSQL() {
    Assert.assertEquals(
        "INSERT INTO test VALUES ('val0', 0), ('val1', 1)",
        insertSQL("test", new String[] {"string", "long"}, "val0,0", "val1,1"));
  }

  @Test
  public void TestCsvHeader() {
    Assert.assertEquals("_c0:string,_c1:long", csvHeader("string", "long"));
  }

  @Test
  public void TestAssertTableResults() {
    List<Map<String, Object>> results = new ArrayList<>();
    Map<String, Object> row0 = new HashMap<>();
    row0.put("_c0", "val0");
    row0.put("_c1", 0L);
    results.add(row0);
    Map<String, Object> row1 = new HashMap<>();
    row1.put("_c0", "val1");
    row1.put("_c1", 1L);
    results.add(row1);

    assertTableResults(results, 2, "val0", 0L, "val1", 1L);
  }
}
