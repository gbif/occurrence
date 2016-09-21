package org.gbif.occurrence.validation.api;

import java.io.BufferedReader;
import java.io.FileReader;
import javax.annotation.Nullable;

public class DataFile {

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

  public String[] readHeader() {
    try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
      return br.readLine().split(delimiterChar.toString());
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }

  }
}
