package org.gbif.occurrence.validation.tabular.parallel;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.occurrence.validation.api.ResultsCollector;
import org.gbif.occurrence.validation.model.RecordInterpretionBasedEvaluationResult;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

public class ConcurrentValidationCollector implements ResultsCollector<Map<OccurrenceIssue, Long>> {

  private final ConcurrentHashMap<OccurrenceIssue, LongAdder> issuesCounter;
  private final LongAdder recordCount;

  public ConcurrentValidationCollector(){
    issuesCounter = new ConcurrentHashMap(OccurrenceIssue.values().length);
    recordCount = new LongAdder();
  }

  @Override
  public void accumulate(RecordInterpretionBasedEvaluationResult result) {
    recordCount.increment();
    result.getDetails().forEach(
            details -> issuesCounter.computeIfAbsent(details.getIssueFlag(), k -> new LongAdder()).increment()
    );
  }

  @Override
  public Map<OccurrenceIssue, Long> getAggregatedResult() {
    return issuesCounter.entrySet().stream().collect(Collectors.toMap(entry -> entry.getKey(),
                                                                      entry -> entry.getValue().longValue()));
  }

  @Override
  public String toString() {
    return "ConcurrentValidationCollector{" +
           "issuesCounter=" + issuesCounter +
           ", recordCount=" + recordCount +
           '}';
  }
}
