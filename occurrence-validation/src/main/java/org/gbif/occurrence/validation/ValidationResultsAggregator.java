package org.gbif.occurrence.validation;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.occurrence.processor.interpreting.result.OccurrenceInterpretationResult;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import com.google.common.base.MoreObjects;

public class ValidationResultsAggregator {

  private ConcurrentHashMap<OccurrenceIssue, LongAdder> issuesCounter = new ConcurrentHashMap(OccurrenceIssue.values().length);

  public void accumulateResult(OccurrenceInterpretationResult result) {
    result.getUpdated().getIssues().forEach(
      issue -> issuesCounter.computeIfAbsent(issue, k -> new LongAdder()).increment()
    );
  }

  @Override
  public String toString() {
    return issuesCounter.toString();
  }

}
