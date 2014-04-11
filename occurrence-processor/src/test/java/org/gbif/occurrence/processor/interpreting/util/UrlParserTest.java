package org.gbif.occurrence.processor.interpreting.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 *
 */
public class UrlParserTest {

  @Test
  public void testParse() throws Exception {
    assertNull(UrlParser.parse(null));
    assertNull(UrlParser.parse(""));
    assertNull(UrlParser.parse(" "));
    assertNull(UrlParser.parse("-"));
    assertNull(UrlParser.parse("tim.png"));
    assertNull(UrlParser.parse("images/logo.gif"));

    assertEquals("http://www.gbif.org/logo.png", UrlParser.parse(" http://www.gbif.org/logo.png").toString());
    assertEquals("http://www.gbif.org/logo.png", UrlParser.parse("www.gbif.org/logo.png").toString());
    assertEquals("https://www.gbif.org/logo.png", UrlParser.parse(" https://www.gbif.org/logo.png").toString());
    assertEquals("ftp://www.gbif.org/logo.png", UrlParser.parse(" ftp://www.gbif.org/logo.png").toString());
    assertEquals("http://www.gbif.org/image?id=12&format=gif,jpg", UrlParser.parse("http://www.gbif.org/image?id=12&format=gif,jpg").toString());
  }
}
