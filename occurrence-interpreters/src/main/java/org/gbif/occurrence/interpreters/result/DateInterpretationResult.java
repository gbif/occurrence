package org.gbif.occurrence.interpreters.result;

import org.gbif.api.vocabulary.OccurrenceValidationRule;

import java.util.Date;

import com.google.common.base.Objects;

/**
 * The immutable result of a Date interpretation.
 */
public class DateInterpretationResult extends InterpretationResult{

  private final Integer year;
  private final Integer month;
  private final Integer day;
  private final Integer time;
  private final Date date;

  /**
   * Creates an empty "no" result with all recorded date validation rules passed.
   */
  public DateInterpretationResult() {
    this.date = null;
    this.year = null;
    this.month = null;
    this.day = null;
    this.time = null;
    this.setValidationRule(OccurrenceValidationRule.RECORDED_DATE_INVALID, true);
    this.setValidationRule(OccurrenceValidationRule.RECORDED_DATE_MISMATCH, true);
    this.setValidationRule(OccurrenceValidationRule.RECORDED_YEAR_UNLIKELY, true);
  }

  public DateInterpretationResult(Integer year, Integer month, Integer day, Integer time, Date date,
                                  boolean invalid, boolean mismatch, boolean unlikely) {
    this.date = date;
    this.year = year;
    this.month = month;
    this.day = day;
    this.time = time;
    this.setValidationRule(OccurrenceValidationRule.RECORDED_DATE_INVALID, invalid);
    this.setValidationRule(OccurrenceValidationRule.RECORDED_DATE_MISMATCH, mismatch);
    this.setValidationRule(OccurrenceValidationRule.RECORDED_YEAR_UNLIKELY, unlikely);
  }

  public Integer getYear() {
    return year;
  }

  public Integer getMonth() {
    return month;
  }

  public Date getDate() {
    return date;
  }

  public Integer getDay() {
    return day;
  }

  public Integer getTime() {
    return time;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(year, month, day, date);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final DateInterpretationResult other = (DateInterpretationResult) obj;
    return Objects.equal(this.year, other.year) && Objects.equal(this.month, other.month) && Objects
      .equal(this.date, other.date) && Objects.equal(this.day, other.day);
  }
}
