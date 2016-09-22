package org.gbif.occurrence.validation.tabular;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.occurrence.processor.interpreting.result.OccurrenceInterpretationResult;
import org.gbif.occurrence.validation.api.ResultsCollector;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;


public class OccurrenceValidationCollector implements ResultsCollector<OccurrenceInterpretationResult,Map<OccurrenceIssue, LongAdder>> {

  private ConcurrentHashMap<OccurrenceIssue, LongAdder> issuesCounter = new ConcurrentHashMap(OccurrenceIssue.values().length);
  private LongAdder recordCount = new LongAdder();

  @Override
  public void accumulate(OccurrenceInterpretationResult result) {
    recordCount.increment();
    result.getUpdated().getIssues().forEach(
      issue -> issuesCounter.computeIfAbsent(issue, k -> new LongAdder()).increment()
    );
  }

  @Override
  public Map<OccurrenceIssue, LongAdder> getAggregatedResult(){
    return issuesCounter;
  }

  @Override
  public String toString() {
    return "Record count: " + recordCount.toString() + " Issues: " + issuesCounter.toString();
  }

}
