/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.trino.processor.result;

import com.google.common.base.Objects;

import java.util.Date;

/**
 * The immutable result of a Date interpretation.
 */
public class DateYearMonthDay{
  private final Date date;
  private final Integer year;
  private final Integer month;
  private final Integer day;
  private final Integer time;

  /**
   * Creates an empty "no" result with all recorded date validation rules passed.
   */
  public DateYearMonthDay() {
    this.date = null;
    this.year = null;
    this.month = null;
    this.day = null;
    this.time = null;
  }

  public DateYearMonthDay(Integer year, Integer month, Integer day, Integer time, Date date) {
    this.date = date;
    this.year = year;
    this.month = month;
    this.day = day;
    this.time = time;
  }

  public Integer getYear() {
    return year;
  }

  public Integer getMonth() {
    return month;
  }

  public Integer getDay() {
    return day;
  }

  public Integer getTime() {
    return time;
  }

  public Date getDate() {
    return date;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(year, month, day, time, date);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final DateYearMonthDay other = (DateYearMonthDay) obj;
    return Objects.equal(this.year, other.year)
           && Objects.equal(this.month, other.month)
           && Objects.equal(this.day, other.day)
           && Objects.equal(this.time, other.time)
           && Objects.equal(this.date, other.date);
  }
}
