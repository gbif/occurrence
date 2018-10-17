package org.gbif.occurrence.ws.provider.hive;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

public class Result {

  @FunctionalInterface
  interface Read {
    public String apply(ResultSet resultset);
  }

  static class ReadExplain implements Read {

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

  static class ReadDescribe implements Read {

    private class DescribeResult {
      private String columnName;
      private String dataType;
      private String comment;

      public DescribeResult() {}

      public DescribeResult(String columnName, String dataType, String comment) {
        super();
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
        builder.append("{\"columnName\":\"").append(columnName).append("\", \"dataType\":\"").append(dataType).append("\", \"comment\":\"")
            .append(comment).append("\"}");
        return builder.toString();
      }
    }

    @Override
    public String apply(ResultSet resultset) {

      try {
        List<DescribeResult> results = Lists.newArrayList();
        while (resultset.next()) {
          String columnName = resultset.getString("col_name");
          String dataType = resultset.getString("data_type");
          String comment = resultset.getString("comment");
          results.add(new DescribeResult(columnName, dataType, comment));
        }
        return results.toString();
      } catch (SQLException e) {
        Throwables.propagate(e);
      }
      return "";
    }
  }
}
