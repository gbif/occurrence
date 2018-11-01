package org.gbif.occurrence.download.service.hive;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
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

  public static class ReadDescribe implements Read<List<DescribeResult>> {
    
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

  /**
   * Describe HDFS table response.
   */
  public static class DescribeResult {
    private String columnName;
    private String dataType;
    private String comment;

    /**
     * Empty constructor, required for serialization.
     */
    public DescribeResult() {}

    /**
     * Full constructor.
     */
    public DescribeResult(String columnName, String dataType, String comment) {
      this.columnName = columnName;
      this.dataType = dataType;
      this.comment = comment;
    }

    public String getColumnName() {
      return columnName;
    }

    public void setColumnName(String columnName) {
      this.columnName = columnName;
    }

    public String getDataType() {
      return dataType;
    }

    public void setDataType(String dataType) {
      this.dataType = dataType;
    }

    public String getComment() {
      return comment;
    }

    public void setComment(String comment) {
      this.comment = comment;
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("{\"columnName\":\"").append(columnName)
             .append("\", \"dataType\":\"").append(dataType)
             .append("\", \"comment\":\"").append(comment)
             .append("\"}");
      return builder.toString();
    }
  }
}
