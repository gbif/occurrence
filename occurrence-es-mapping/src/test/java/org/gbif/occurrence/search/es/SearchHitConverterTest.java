package org.gbif.occurrence.search.es;

import java.util.Calendar;
import java.util.Date;

import org.junit.jupiter.api.Test;

import static org.gbif.occurrence.search.es.SearchHitConverter.STRING_TO_DATE;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SearchHitConverterTest {

  @Test
  public void millisecondsNormalizationTest() {
    Date date = STRING_TO_DATE.apply("2021-03-24T11:27:54.123456+01:00");
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    assertEquals(2021, cal.get(Calendar.YEAR));
    assertEquals(2, cal.get(Calendar.MONTH));
    assertEquals(24, cal.get(Calendar.DAY_OF_MONTH));
    assertEquals(27, cal.get(Calendar.MINUTE));
    assertEquals(54, cal.get(Calendar.SECOND));
    assertEquals(123, cal.get(Calendar.MILLISECOND));

    date = STRING_TO_DATE.apply("2021-03-24T11:27:54.123756+01:00");
    cal.setTime(date);
    assertEquals(2021, cal.get(Calendar.YEAR));
    assertEquals(2, cal.get(Calendar.MONTH));
    assertEquals(24, cal.get(Calendar.DAY_OF_MONTH));
    assertEquals(27, cal.get(Calendar.MINUTE));
    assertEquals(54, cal.get(Calendar.SECOND));
    assertEquals(123, cal.get(Calendar.MILLISECOND));

    date = STRING_TO_DATE.apply("2021-03-24T11:27:54.123156+01:00");
    cal.setTime(date);
    assertEquals(2021, cal.get(Calendar.YEAR));
    assertEquals(2, cal.get(Calendar.MONTH));
    assertEquals(24, cal.get(Calendar.DAY_OF_MONTH));
    assertEquals(27, cal.get(Calendar.MINUTE));
    assertEquals(54, cal.get(Calendar.SECOND));
    assertEquals(123, cal.get(Calendar.MILLISECOND));

    date = STRING_TO_DATE.apply("2021-03-24T11:27:54.1+01:00");
    cal.setTime(date);
    assertEquals(2021, cal.get(Calendar.YEAR));
    assertEquals(2, cal.get(Calendar.MONTH));
    assertEquals(24, cal.get(Calendar.DAY_OF_MONTH));
    assertEquals(27, cal.get(Calendar.MINUTE));
    assertEquals(54, cal.get(Calendar.SECOND));
    assertEquals(100, cal.get(Calendar.MILLISECOND));

    date = STRING_TO_DATE.apply("2021-03-24T11:27:54.1");
    cal.setTime(date);
    assertEquals(2021, cal.get(Calendar.YEAR));
    assertEquals(2, cal.get(Calendar.MONTH));
    assertEquals(24, cal.get(Calendar.DAY_OF_MONTH));
    assertEquals(27, cal.get(Calendar.MINUTE));
    assertEquals(54, cal.get(Calendar.SECOND));
    assertEquals(100, cal.get(Calendar.MILLISECOND));

    date = STRING_TO_DATE.apply("2021-03-24T11:27:54+01:00");
    cal.setTime(date);
    assertEquals(2021, cal.get(Calendar.YEAR));
    assertEquals(2, cal.get(Calendar.MONTH));
    assertEquals(24, cal.get(Calendar.DAY_OF_MONTH));
    assertEquals(27, cal.get(Calendar.MINUTE));
    assertEquals(54, cal.get(Calendar.SECOND));
    assertEquals(0, cal.get(Calendar.MILLISECOND));

    date = STRING_TO_DATE.apply("2021-03-24T11:27+01:00");
    cal.setTime(date);
    assertEquals(2021, cal.get(Calendar.YEAR));
    assertEquals(2, cal.get(Calendar.MONTH));
    assertEquals(24, cal.get(Calendar.DAY_OF_MONTH));
    assertEquals(27, cal.get(Calendar.MINUTE));
    assertEquals(0, cal.get(Calendar.SECOND));
    assertEquals(0, cal.get(Calendar.MILLISECOND));
  }

  @Test
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
}
