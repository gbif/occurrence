package org.gbif.occurrence.validation.api;

public interface DataFileProcessor<T> {

  T process(DataFile dataFile);
}
