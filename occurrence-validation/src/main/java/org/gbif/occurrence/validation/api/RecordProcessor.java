package org.gbif.occurrence.validation.api;

import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.validation.model.RecordInterpretionBasedEvaluationResult;

import java.util.Map;

/**
 * C.G. I would rename this class RecordEvaluator
 */
public interface RecordProcessor {

  RecordInterpretionBasedEvaluationResult process(Map<Term, String> record);

}
