package org.gbif.occurrence.validation.api;

public interface ResultsCollector<T,R> {

  void accumulate(T result);

  R getAggregatedResult();
}
