package org.gbif.occurrencestore.interpreters.result;

import java.util.Date;

import com.google.common.base.Objects;

/**
 * The immutable result of a Date interpretation.
 */
public class DateInterpretationResult {

  private final Integer year;
  private final Integer month;
  private final Date date;
  private final String dateString;

  public DateInterpretationResult() {
    this.year = null;
    this.month = null;
    this.date = null;
    this.dateString = null;
  }

  public DateInterpretationResult(Integer year, Integer month, Date date, String dateString) {
    this.year = year;
    this.month = month;
    this.date = date;
    this.dateString = dateString;
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

  public String getDateString() {
    return dateString;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(year, month, date, dateString);
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
      .equal(this.date, other.date) && Objects.equal(this.dateString, other.dateString);
  }
}
