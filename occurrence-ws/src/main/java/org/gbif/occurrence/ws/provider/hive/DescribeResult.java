package org.gbif.occurrence.ws.provider.hive;

/**
 * 
 * Data structure representing result of Describe command to Hive.
 *
 */
public class DescribeResult {
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
    builder.append("{\"columnName\":\"").append(columnName)
           .append("\", \"dataType\":\"").append(dataType)
           .append("\", \"comment\":\"").append(comment)
           .append("\"}");
    return builder.toString();
  }
}