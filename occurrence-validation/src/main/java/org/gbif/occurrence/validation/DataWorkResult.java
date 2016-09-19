package org.gbif.occurrence.validation;

public class DataWorkResult {

  public static enum Result {
    SUCCESS, FAILED;
  }

  private DataInputFile dataInputFile;

  private Result result;


  public DataWorkResult(DataInputFile dataInputFile, Result result) {
    this.dataInputFile = dataInputFile;
    this.result = result;
  }

  public DataInputFile getDataInputFile() {
    return dataInputFile;
  }

  public void setDataInputFile(DataInputFile dataInputFile) {
    this.dataInputFile = dataInputFile;
  }

  public Result getResult() {
    return result;
  }

  public void setResult(Result result) {
    this.result = result;
  }
}
