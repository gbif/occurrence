package org.gbif.occurrence.search.es;

import java.text.ParseException;

import org.junit.Test;

public class EsQueryUtilsTest {

  @Test(expected = Test.None.class)
  public void dateParserTest() throws ParseException {
    EsQueryUtils.STRING_TO_DATE.apply("2019-04-15T17:17:48.191 +02:00");

    EsQueryUtils.STRING_TO_DATE.apply("2019-04-15T17:17:48.191");

    EsQueryUtils.STRING_TO_DATE.apply("2019-04-15T17:17:48.023+02:00");
  }
}
