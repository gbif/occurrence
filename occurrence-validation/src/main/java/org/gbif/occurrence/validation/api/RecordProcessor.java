package org.gbif.occurrence.validation.api;

public interface RecordProcessor<T> {

  T process(String line);

}
