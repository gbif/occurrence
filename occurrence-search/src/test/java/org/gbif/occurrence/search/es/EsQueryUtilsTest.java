package org.gbif.occurrence.search.es;

import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalTime;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EsQueryUtilsTest {

  @Test(expected = Test.None.class)
  public void dateParserTest() throws ParseException {
    EsQueryUtils.STRING_TO_DATE.apply("2019");

    EsQueryUtils.STRING_TO_DATE.apply("2019-04");

    EsQueryUtils.STRING_TO_DATE.apply("2019-04-01");

    EsQueryUtils.STRING_TO_DATE.apply("2019-04-15T17:17:48.191 +02:00");

    EsQueryUtils.STRING_TO_DATE.apply("2019-04-15T17:17:48.191");

    EsQueryUtils.STRING_TO_DATE.apply("2019-04-15T17:17:48.023+02:00");
  }

  @Test
  public void lowerBoundRangeTest() {
    assertEquals(
        LocalDate.of(2019, 10, 01).atTime(LocalTime.MIN),
        EsQueryUtils.LOWER_BOUND_RANGE_PARSER.apply("2019-10"));
    assertEquals(
      LocalDate.of(2019, 01, 01).atTime(LocalTime.MIN),
      EsQueryUtils.LOWER_BOUND_RANGE_PARSER.apply("2019"));
    assertEquals(
      LocalDate.of(2019, 10, 2).atTime(LocalTime.MIN),
      EsQueryUtils.LOWER_BOUND_RANGE_PARSER.apply("2019-10-02"));
  }

  @Test
  public void upperBoundRangeTest() {
    assertEquals(
      LocalDate.of(2019, 10, 31).atTime(LocalTime.MAX),
      EsQueryUtils.UPPER_BOUND_RANGE_PARSER.apply("2019-10"));
    assertEquals(
      LocalDate.of(2019, 12, 31).atTime(LocalTime.MAX),
      EsQueryUtils.UPPER_BOUND_RANGE_PARSER.apply("2019"));
    assertEquals(
      LocalDate.of(2019, 10, 2).atTime(LocalTime.MAX),
      EsQueryUtils.UPPER_BOUND_RANGE_PARSER.apply("2019-10-02"));
  }
}
