package org.gbif.occurrence.validation.api;

public interface RecordProcessorFactory<T> {

  RecordProcessor<T> create();
}
