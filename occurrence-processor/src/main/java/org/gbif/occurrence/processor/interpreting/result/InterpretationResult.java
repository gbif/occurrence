package org.gbif.occurrence.processor.interpreting.result;

import org.gbif.api.vocabulary.OccurrenceIssue;

import java.util.EnumSet;
import java.util.Set;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * Base class for interpretation results that modify validation issues.
 * The payload T is a single interpreted value which most interpretations use.
 * For more complex results subclass and add more properties.
 */
public class InterpretationResult<T> {
  private final T payload;
  private final Set<OccurrenceIssue> issues = EnumSet.noneOf(OccurrenceIssue.class);

  public InterpretationResult(T payload) {
    this.payload = payload;
  }

  public T getPayload() {
    return payload;
  }

  public void addIssue(OccurrenceIssue issue) {
    Preconditions.checkNotNull("Issue must be specified", issue);
    issues.add(issue);
  }

  public Set<OccurrenceIssue> getIssues() {
    return issues;
  }


  @Override
  public int hashCode() {
    return Objects.hashCode(payload, issues);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final InterpretationResult other = (InterpretationResult) obj;
    return Objects.equal(this.payload, other.payload)
           && Objects.equal(this.issues, other.issues);
  }
}
