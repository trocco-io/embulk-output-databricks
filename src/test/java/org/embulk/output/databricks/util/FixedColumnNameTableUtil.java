package org.embulk.output.databricks.util;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Assert;

// this class column name only assumed _c0, _c1, _c2, ...
public class FixedColumnNameTableUtil {
  public static String columnName(int c) {
    return String.format("_c%d", c);
  }

  public static String createTableSQL(String quotedTableName, String... types) {
    String columns =
        IntStream.range(0, types.length)
            .mapToObj(c -> String.format("%s %s", columnName(c), types[c]))
            .collect(Collectors.joining(", "));
    return String.format("CREATE TABLE %s (%s)", quotedTableName, columns);
  }

  // insertSQL("table", new String[] { "string", "long" }, "val0,val1", "val2,val3")
  public static String insertSQL(String quotedTableName, String[] types, String... rows) {
    String valuesElem =
        Arrays.stream(rows)
            .map(
                row -> {
                  String[] values = row.split(",");
                  String joinedValues =
                      IntStream.range(0, types.length)
                          .mapToObj(i -> convertSQLValue(types[i], values[i]))
                          .collect(Collectors.joining(", "));
                  return String.format("(%s)", joinedValues);
                })
            .collect(Collectors.joining(", "));
    return String.format("INSERT INTO %s VALUES %s", quotedTableName, valuesElem);
  }

  private static String convertSQLValue(String type, String value) {
    if (type.equals("string")) {
      return String.format("'%s'", value);
    }
    return value;
  }

  public static String csvHeader(String... types) {
    return IntStream.range(0, types.length)
        .mapToObj(c -> String.format("%s:%s", columnName(c), types[c]))
        .collect(Collectors.joining(","));
  }

  public static void assertTableResults(
      List<Map<String, Object>> results, int numberOfColumns, Object... data) {
    Assert.assertEquals(0, data.length % numberOfColumns);
    Assert.assertEquals(results.size(), data.length / numberOfColumns);
    for (int i = 0; i < results.size(); i++) {
      for (int c = 0; c < numberOfColumns; c++) {
        Object datum = data[i * numberOfColumns + c];
        Assert.assertEquals(datum, results.get(i).get(columnName(c)));
      }
    }
  }
}
