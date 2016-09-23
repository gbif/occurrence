package org.gbif.occurrence.validation.tabular.parallel;

import org.gbif.occurrence.validation.api.DataFile;

public class DataWorkResult {

  public enum Result {
    SUCCESS, FAILED;
  }

  private DataFile dataFile;

  private Result result;


  public DataWorkResult() {

  }
  public DataWorkResult(DataFile dataFile, Result result) {
    this.dataFile = dataFile;
    this.result = result;
  }

  public DataFile getDataFile() {
    return dataFile;
  }

  public void setDataFile(DataFile dataFile) {
    this.dataFile = dataFile;
  }

  public Result getResult() {
    return result;
  }

  public void setResult(Result result) {
    this.result = result;
  }

  @Override
  public String toString() {
    return "Result: " + result.name() + " Datafile: " + dataFile.getFileName();
  }
}
