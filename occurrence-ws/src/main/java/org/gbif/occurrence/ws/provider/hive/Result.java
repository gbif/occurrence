package org.gbif.occurrence.ws.provider.hive;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

/**
 * 
 * Contains classes to Read {@link java.sql.ResultSet}  of different types.
 *
 */
public class Result {

  @FunctionalInterface
  interface Read<T> {
    T apply(ResultSet resultset);
  }

  static class ReadExplain implements Read<String> {

    @Override
    public String apply(ResultSet resultset) {
      StringBuilder sb = new StringBuilder();
      try {
        while (resultset.next()) {
          sb.append(resultset.getString("Explain") + '\n');
        }
      } catch (SQLException e) {
        Throwables.propagate(e);
      }
      return sb.toString();
    }
  }

  static class ReadDescribe implements Read<List<DescribeResult>> {
    
    @Override
    public List<DescribeResult> apply(ResultSet resultset) {
      List<DescribeResult> results = Lists.newArrayList();
      try {
        while (resultset.next()) {
          String columnName = resultset.getString("col_name");
          String dataType = resultset.getString("data_type");
          String comment = resultset.getString("comment");
          results.add(new DescribeResult(columnName, dataType, comment));
        }
      } catch (SQLException e) {
        Throwables.propagate(e);
      }
      return results;
    }
  }
}
