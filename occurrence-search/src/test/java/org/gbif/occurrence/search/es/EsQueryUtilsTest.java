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
package org.gbif.occurrence.search.es;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.Date;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.gbif.occurrence.search.es.EsQueryUtils.LOWER_BOUND_RANGE_PARSER;
import static org.gbif.occurrence.search.es.EsQueryUtils.STRING_TO_DATE;
import static org.gbif.occurrence.search.es.EsQueryUtils.UPPER_BOUND_RANGE_PARSER;
import static org.junit.jupiter.api.Assertions.*;

public class EsQueryUtilsTest {

  @Test
  @Disabled
  public void dateParserTest() {
    Date date = STRING_TO_DATE.apply("2019");
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    assertEquals(2019, cal.get(Calendar.YEAR));
    assertEquals(0, cal.get(Calendar.MONTH));
    assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));

    date = STRING_TO_DATE.apply("2019-04");
    cal.setTime(date);
    assertEquals(2019, cal.get(Calendar.YEAR));
    assertEquals(3, cal.get(Calendar.MONTH));
    assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));

    date = STRING_TO_DATE.apply("2019-04-02");
    cal.setTime(date);
    assertEquals(2019, cal.get(Calendar.YEAR));
    assertEquals(3, cal.get(Calendar.MONTH));
    assertEquals(2, cal.get(Calendar.DAY_OF_MONTH));

    date = STRING_TO_DATE.apply("2019-04-15T17:17:48.191 +02:00");
    cal.setTime(date);
    assertEquals(2019, cal.get(Calendar.YEAR));
    assertEquals(3, cal.get(Calendar.MONTH));
    assertEquals(15, cal.get(Calendar.DAY_OF_MONTH));

    date = STRING_TO_DATE.apply("2019-04-15T17:17:48.191");
    cal.setTime(date);
    assertEquals(2019, cal.get(Calendar.YEAR));
    assertEquals(3, cal.get(Calendar.MONTH));
    assertEquals(15, cal.get(Calendar.DAY_OF_MONTH));

    date = STRING_TO_DATE.apply("2019-04-15T17:17:48.023+02:00");
    cal.setTime(date);
    assertEquals(2019, cal.get(Calendar.YEAR));
    assertEquals(3, cal.get(Calendar.MONTH));
    assertEquals(15, cal.get(Calendar.DAY_OF_MONTH));

    date = STRING_TO_DATE.apply("2019-11-12T13:24:56.963591");
    cal.setTime(date);
    assertEquals(2019, cal.get(Calendar.YEAR));
    assertEquals(10, cal.get(Calendar.MONTH));
    assertEquals(12, cal.get(Calendar.DAY_OF_MONTH));
  }

  @Test
  @Disabled
  public void dateWithYearZeroTest() {
    Date date = STRING_TO_DATE.apply("0000");
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    assertEquals(1, cal.get(Calendar.YEAR));
    assertEquals(0, cal.get(Calendar.MONTH));
    assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));

    date = STRING_TO_DATE.apply("0000-01");
    cal.setTime(date);
    assertEquals(1, cal.get(Calendar.YEAR));
    assertEquals(0, cal.get(Calendar.MONTH));
    assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));

    date = STRING_TO_DATE.apply("0000-01-01");
    cal.setTime(date);
    assertEquals(1, cal.get(Calendar.YEAR));
    assertEquals(0, cal.get(Calendar.MONTH));
    assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));

    date = STRING_TO_DATE.apply("0000-01-01T00:00:01.100");
    cal.setTime(date);
    assertEquals(1, cal.get(Calendar.YEAR));
    assertEquals(0, cal.get(Calendar.MONTH));
    assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));

    date = STRING_TO_DATE.apply("0000-01-01T17:17:48.191 +02:00");
    cal.setTime(date);
    assertEquals(1, cal.get(Calendar.YEAR));
    assertEquals(0, cal.get(Calendar.MONTH));
    assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));

    date = STRING_TO_DATE.apply("0000-01-01T13:24:56.963591");
    cal.setTime(date);
    assertEquals(1, cal.get(Calendar.YEAR));
    assertEquals(0, cal.get(Calendar.MONTH));
    assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));

    date = STRING_TO_DATE.apply("0000-01-01T17:17:48.023+02:00");
    cal.setTime(date);
    assertEquals(1, cal.get(Calendar.YEAR));
    assertEquals(0, cal.get(Calendar.MONTH));
    assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));
  }

  @Test
  public void lowerBoundRangeTest() {
    assertEquals(
        LocalDate.of(2019, 10, 01).atTime(LocalTime.MIN),
        LOWER_BOUND_RANGE_PARSER.apply("2019-10"));
    assertEquals(
        LocalDate.of(2019, 01, 01).atTime(LocalTime.MIN), LOWER_BOUND_RANGE_PARSER.apply("2019"));
    assertEquals(
        LocalDate.of(2019, 10, 2).atTime(LocalTime.MIN),
        LOWER_BOUND_RANGE_PARSER.apply("2019-10-02"));
  }

  @Test
  public void upperBoundRangeTest() {
    assertEquals(
        LocalDate.of(2019, 10, 31).atTime(LocalTime.MAX),
        UPPER_BOUND_RANGE_PARSER.apply("2019-10"));
    assertEquals(
        LocalDate.of(2019, 12, 31).atTime(LocalTime.MAX), UPPER_BOUND_RANGE_PARSER.apply("2019"));
    assertEquals(
        LocalDate.of(2019, 10, 2).atTime(LocalTime.MAX),
        UPPER_BOUND_RANGE_PARSER.apply("2019-10-02"));
  }
}
