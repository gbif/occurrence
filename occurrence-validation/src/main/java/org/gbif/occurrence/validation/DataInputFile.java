package org.gbif.occurrence.validation;

import javax.annotation.Nullable;

public class DataInputFile {

  private Character separator;

  private String[] columns;

  private String fileName;

  private Integer numOfLines;

  public Character getSeparator() {
    return separator;
  }

  public void setSeparator(Character separator) {
    this.separator = separator;
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
}
