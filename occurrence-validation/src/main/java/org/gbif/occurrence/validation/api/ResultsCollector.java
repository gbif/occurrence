package org.gbif.occurrence.validation.api;

import org.gbif.occurrence.validation.model.RecordInterpretionBasedEvaluationResult;

public interface ResultsCollector<R> {

  void accumulate(RecordInterpretionBasedEvaluationResult result);

  R getAggregatedResult();
}
