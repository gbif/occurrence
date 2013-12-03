package org.gbif.occurrencestore.interpreters;

import org.gbif.occurrencestore.interpreters.result.DateInterpretationResult;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class DateInterpreterTest {

  @Test
  public void testGoodDate() {
    DateInterpretationResult result = DateInterpreter.interpretDate("1984", "3", "22", null);
    assertEquals(1984, result.getYear().intValue());
    assertEquals(3, result.getMonth().intValue());
    assertEquals("1984-03-22", result.getDateString());
  }

  @Test
  public void testGoodOldDate() {
    DateInterpretationResult result = DateInterpreter.interpretDate("1957", "3", "22", null);
    assertEquals(1957, result.getYear().intValue());
    assertEquals(3, result.getMonth().intValue());
    assertEquals("1957-03-22", result.getDateString());
  }

  @Test
  public void test0Month() {
    DateInterpretationResult result = DateInterpreter.interpretDate("1984", "0", "22", null);
    assertEquals(1984, result.getYear().intValue());
    assertNull(result.getMonth());
    assertNull(result.getDateString());
  }

  @Test
  public void testOldYear() {
    DateInterpretationResult result = DateInterpreter.interpretDate("1684", "3", "22", null);
    assertNotNull(result);
    assertNull(result.getMonth());
    assertNull(result.getDateString());
    assertNull(result.getYear());
    assertNull(result.getDate());
  }

  @Test
  public void testFutureYear() {
    DateInterpretationResult result = DateInterpreter.interpretDate("2100", "3", "22", null);
    assertNull(result.getYear());
    assertEquals(3, result.getMonth().intValue());
    assertNull(result.getDateString());
  }

  @Test
  public void testBadDay() {
    DateInterpretationResult result = DateInterpreter.interpretDate("1984", "3", "32", null);
    assertEquals(1984, result.getYear().intValue());
    assertEquals(3, result.getMonth().intValue());
    assertNull(result.getDateString());
  }

  @Test
  public void testStringGood() {
    DateInterpretationResult result = DateInterpreter.interpretDate(null, null, null, "1984-03-22");
    assertEquals(1984, result.getYear().intValue());
    assertEquals(3, result.getMonth().intValue());
    assertEquals("1984-03-22", result.getDateString());
  }

  @Test
  public void testStringBad() {
    DateInterpretationResult result = DateInterpreter.interpretDate(null, null, null, "22-17-1984");
    assertNotNull(result);
    assertNull(result.getMonth());
    assertNull(result.getDateString());
    assertNull(result.getYear());
    assertNull(result.getDate());
  }

  @Test
  public void testStringWins() {
    DateInterpretationResult result = DateInterpreter.interpretDate("1984", "3", null, "1984-03-22");
    assertEquals(1984, result.getYear().intValue());
    assertEquals(3, result.getMonth().intValue());
    assertEquals("1984-03-22", result.getDateString());
  }

  @Test
  public void testStringLoses() {
    DateInterpretationResult result = DateInterpreter.interpretDate("1984", "3", null, "22-17-1984");
    assertEquals(1984, result.getYear().intValue());
    assertEquals(3, result.getMonth().intValue());
    assertNull(result.getDateString());
  }

  @Test
  public void testOnlyMonth() {
    DateInterpretationResult result = DateInterpreter.interpretDate(null, "3", null, null);
    assertNotNull(result);
    assertEquals(3, result.getMonth().intValue());
  }

  @Test
  public void testOnlyDay() {
    DateInterpretationResult result = DateInterpreter.interpretDate(null, null, "23", null);
    assertNotNull(result);
    assertNull(result.getMonth());
    assertNull(result.getDateString());
    assertNull(result.getYear());
    assertNull(result.getDate());
  }

  @Test
  public void testAllNulls() {
    DateInterpretationResult result = DateInterpreter.interpretDate(null, null, null, null);
    assertNotNull(result);
    assertNull(result.getMonth());
    assertNull(result.getDateString());
    assertNull(result.getYear());
    assertNull(result.getDate());
  }

  @Test
  public void testDateStrings() {
    String dateString = "1999-07-19";
    DateInterpretationResult result = DateInterpreter.interpretDate(null, null, null, dateString);
    assertEquals("1999-07-19", result.getDateString());

    dateString = "19-07-1999";
    result = DateInterpreter.interpretDate(null, null, null, dateString);
    assertEquals("1999-07-19", result.getDateString());

    // we can't yet handle month-day-year
    dateString = "07-19-1999";
    result = DateInterpreter.interpretDate(null, null, null, dateString);
    assertNull(result.getDateString());

    dateString = "07-06-1999";
    result = DateInterpreter.interpretDate(null, null, null, dateString);
    assertEquals("1999-06-07", result.getDateString());

    dateString = "1999/7/19";
    result = DateInterpreter.interpretDate(null, null, null, dateString);
    assertEquals("1999-07-19", result.getDateString());

    dateString = "19/7/1999";
    result = DateInterpreter.interpretDate(null, null, null, dateString);
    assertEquals("1999-07-19", result.getDateString());

    dateString = "1999.7.19";
    result = DateInterpreter.interpretDate(null, null, null, dateString);
    assertEquals("1999-07-19", result.getDateString());

    dateString = "19.7.1999";
    result = DateInterpreter.interpretDate(null, null, null, dateString);
    assertEquals("1999-07-19", result.getDateString());
  }
}
