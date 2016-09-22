package org.gbif.occurrence.validation.tabular;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.occurrence.validation.api.ResultsCollector;
import org.gbif.occurrence.validation.model.RecordInterpretionBasedEvaluationResult;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;


public class ConcurrentValidationCollector implements ResultsCollector<Map<OccurrenceIssue, LongAdder>> {

  private ConcurrentHashMap<OccurrenceIssue, LongAdder> issuesCounter = new ConcurrentHashMap(OccurrenceIssue.values().length);
  private LongAdder recordCount = new LongAdder();

  @Override
  public void accumulate(RecordInterpretionBasedEvaluationResult result) {
    recordCount.increment();
    result.getDetails().forEach(
            details -> issuesCounter.computeIfAbsent(details.getIssueFlag(), k -> new LongAdder()).increment()
    );
  }

  @Override
  public Map<OccurrenceIssue, LongAdder> getAggregatedResult() {
    return issuesCounter;
  }

  @Override
  public String toString() {
    return "Record count: " + recordCount.toString() + " Issues: " + issuesCounter.toString();
  }

}
