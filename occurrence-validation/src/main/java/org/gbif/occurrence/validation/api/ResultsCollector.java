package org.gbif.occurrence.validation.api;

public interface ResultsCollector<T> {

  void accumulate(T result);
}
