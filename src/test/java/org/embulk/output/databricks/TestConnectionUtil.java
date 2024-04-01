package org.embulk.output.databricks;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.embulk.output.databricks.util.ConnectionUtil;
import org.junit.Assert;
import org.junit.Test;

public class TestConnectionUtil {
  @Test
  public void TestConnect() {
    try (Connection con = ConnectionUtil.connectByTestTask()) {
      try (Statement stmt = con.createStatement()) {
        try (ResultSet rs = stmt.executeQuery("SELECT 1")) {
          rs.next();
          Assert.assertEquals("1", rs.getString(1));
        }
      }
    } catch (SQLException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
