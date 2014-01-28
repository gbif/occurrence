package org.gbif.occurrence.processor.interpreting.result;

import org.gbif.api.vocabulary.OccurrenceIssue;

import java.util.Date;
import java.util.Set;

import com.google.common.base.Objects;

/**
 * The immutable result of a Date interpretation.
 */
public class DateInterpretationResult extends InterpretationResult<Date> {

  private final Integer year;
  private final Integer month;
  private final Integer day;
  private final Integer time;

  /**
   * Creates an empty "no" result with all recorded date validation rules passed.
   */
  public DateInterpretationResult() {
    super(null);
    this.year = null;
    this.month = null;
    this.day = null;
    this.time = null;
  }

  public DateInterpretationResult(Integer year, Integer month, Integer day, Integer time, Date date, Set<OccurrenceIssue> issues) {
    super(date);
    this.year = year;
    this.month = month;
    this.day = day;
    this.time = time;
    if (issues != null) {
      this.getIssues().addAll(issues);
    }
  }

  public Integer getYear() {
    return year;
  }

  public Integer getMonth() {
    return month;
  }

  public Date getDate() {
    return getPayload();
  }

  public Integer getDay() {
    return day;
  }

  public Integer getTime() {
    return time;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), year, month, day);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    if (!super.equals(obj)) {
      return false;
    }
    final DateInterpretationResult other = (DateInterpretationResult) obj;
    return Objects.equal(this.year, other.year)
           && Objects.equal(this.month, other.month)
           && Objects.equal(this.day, other.day)
           && Objects.equal(this.time, other.time);
  }
}
