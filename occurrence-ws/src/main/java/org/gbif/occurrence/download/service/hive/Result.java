package org.gbif.occurrence.download.service.hive;

import org.gbif.api.model.occurrence.sql.DescribeResult;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

/**
 * Contains classes to Read {@link java.sql.ResultSet} of different types.
 */
public class Result {

  @FunctionalInterface
  interface Read<T> {

    T apply(ResultSet resultset);
  }

  /**
   * Reads the result set of an explain query to hive.
   */
  static class ReadExplain implements Read<List<String>> {

    @Override
    public List<String> apply(ResultSet resultset) {
      List<String> sb = new ArrayList<>();
      try {
        while (resultset.next()) {
          sb.add(resultset.getString("Explain"));
        }
      } catch (SQLException e) {
        Throwables.propagate(e);
      }
      return sb;
    }
  }

  /**
   * Reads the result set of an describe query to hive.
   */
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
