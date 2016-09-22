package org.gbif.occurrence.validation.tabular;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.occurrence.processor.interpreting.result.OccurrenceInterpretationResult;
import org.gbif.occurrence.validation.api.ResultsCollector;
import org.gbif.occurrence.validation.model.RecordInterpretionBasedEvaluationResult;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

public class SimpleValidationCollector implements ResultsCollector<Map<OccurrenceIssue, Integer>> {

  private HashMap<OccurrenceIssue, Integer> issuesCounter = new HashMap(OccurrenceIssue.values().length);
  private long recordCount;

  @Override
  public void accumulate(RecordInterpretionBasedEvaluationResult result) {
    recordCount += 1;
    /*result.getUpdated().getIssues().forEach(
      issue -> issuesCounter.compute(issue, (k,v) -> (v == null) ? 1 : v++)
    );  */
  }

  @Override
  public Map<OccurrenceIssue, Integer> getAggregatedResult(){
    return issuesCounter;
  }

  @Override
  public String toString() {
    return "Record count: " + recordCount + " Issues: " + issuesCounter.toString();
  }

}
