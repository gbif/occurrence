package org.gbif.occurrence.validation;

import java.io.IOException;
import java.util.UUID;

public class DataFileProcessor {

  private static final int FILE_SPLIT_SIZE = 10000;

  public void validate(String inputFile) throws IOException {
    int numOfLines = FileBashUtilities.countLines(inputFile);
    int splitSize = numOfLines > FILE_SPLIT_SIZE ? (numOfLines / FILE_SPLIT_SIZE) : numOfLines;
    String outDir = UUID.randomUUID().toString();
    String[] splits = FileBashUtilities.splitFile(inputFile, splitSize, outDir);
  }
}
