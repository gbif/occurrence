package org.gbif.occurrence.validation;

import javax.annotation.Nullable;

public class DataInputFile {

  private Character delimiterChar = '\t';

  private String[] columns;

  private String fileName;

  private Integer numOfLines;

  private boolean hasHeaders;

  public Character getDelimiterChar() {
    return delimiterChar;
  }

  public void setDelimiterChar(Character delimiterChar) {
    this.delimiterChar = delimiterChar;
  }

  public String[] getColumns() {
    return columns;
  }

  public void setColumns(String[] columns) {
    this.columns = columns;
  }

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public Integer getNumOfLines() {
    return numOfLines;
  }

  @Nullable
  public void setNumOfLines(Integer numOfLines) {
    this.numOfLines = numOfLines;
  }

  public boolean isHasHeaders() {
    return hasHeaders;
  }

  public void setHasHeaders(boolean hasHeaders) {
    this.hasHeaders = hasHeaders;
  }
}
