package org.gbif.occurrence.validation.api;

import org.gbif.occurrence.validation.model.RecordInterpretionBasedEvaluationResult;

public interface RecordProcessor {

  RecordInterpretionBasedEvaluationResult process(String line);

}
