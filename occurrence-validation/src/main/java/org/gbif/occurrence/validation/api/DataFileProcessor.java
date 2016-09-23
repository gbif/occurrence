package org.gbif.occurrence.validation.api;

public interface DataFileProcessor {

  DataFileValidationResult process(DataFile dataFile);
}
