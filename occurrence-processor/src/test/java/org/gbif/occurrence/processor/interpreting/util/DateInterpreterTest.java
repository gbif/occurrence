package org.gbif.occurrence.processor.interpreting.util;

import org.gbif.occurrence.interpreters.result.DateInterpretationResult;

import java.text.SimpleDateFormat;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class DateInterpreterTest {

  @Test
  public void testGoodDate() {
    DateInterpretationResult result = DateInterpreter.interpretRecordedDate("1984", "3", "22", null);
    assertEquals(1984, result.getYear().intValue());
    assertEquals(3, result.getMonth().intValue());
    assertDate("1984-03-22", result);
  }

  @Test
  public void testGoodOldDate() {
    DateInterpretationResult result = DateInterpreter.interpretRecordedDate("1957", "3", "22", null);
    assertEquals(1957, result.getYear().intValue());
    assertEquals(3, result.getMonth().intValue());
    assertDate("1957-03-22", result);
  }

  @Test
  public void test0Month() {
    DateInterpretationResult result = DateInterpreter.interpretRecordedDate("1984", "0", "22", null);
    assertEquals(1984, result.getYear().intValue());
    Assert.assertNull(result.getMonth());
    Assert.assertNull(result.getDate());
  }

  @Test
  public void testOldYear() {
    DateInterpretationResult result = DateInterpreter.interpretRecordedDate("1684", "3", "22", null);
    Assert.assertNotNull(result);
    Assert.assertNull(result.getDate());
    Assert.assertNull(result.getYear());
    assertEquals(3, result.getMonth().intValue());
    assertEquals(22, result.getDay().intValue());
  }

  @Test
  public void testFutureYear() {
    DateInterpretationResult result = DateInterpreter.interpretRecordedDate("2100", "3", "22", null);
    assertDate(null, result);
  }

  @Test
  public void testBadDay() {
    DateInterpretationResult result = DateInterpreter.interpretRecordedDate("1984", "3", "32", null);
    assertEquals(1984, result.getYear().intValue());
    assertEquals(3, result.getMonth().intValue());
    Assert.assertNull(result.getDate());
  }

  @Test
  public void testStringGood() {
    DateInterpretationResult result = DateInterpreter.interpretRecordedDate(null, null, null, "1984-03-22");
    assertEquals(1984, result.getYear().intValue());
    assertEquals(3, result.getMonth().intValue());
    assertDate("1984-03-22", result);
  }

  @Test
  public void testStringBad() {
    DateInterpretationResult result = DateInterpreter.interpretRecordedDate(null, null, null, "22-17-1984");
    Assert.assertNotNull(result);
    Assert.assertNull(result.getMonth());
    Assert.assertNull(result.getDate());
    Assert.assertNull(result.getYear());
    Assert.assertNull(result.getDate());
  }

  @Test
  public void testStringWins() {
    DateInterpretationResult result = DateInterpreter.interpretRecordedDate("1984", "3", null, "1984-03-22");
    assertEquals(1984, result.getYear().intValue());
    assertEquals(3, result.getMonth().intValue());
    assertDate("1984-03-22", result);
  }

  @Test
  public void testStringLoses() {
    DateInterpretationResult result = DateInterpreter.interpretRecordedDate("1984", "3", null, "22-17-1984");
    assertEquals(1984, result.getYear().intValue());
    assertEquals(3, result.getMonth().intValue());
    Assert.assertNull(result.getDate());
  }

  @Test
  public void testOnlyMonth() {
    DateInterpretationResult result = DateInterpreter.interpretRecordedDate(null, "3", null, null);
    Assert.assertNotNull(result);
    assertEquals(3, result.getMonth().intValue());
  }

  @Test
  public void testOnlyDay() {
    DateInterpretationResult result = DateInterpreter.interpretRecordedDate(null, null, "23", null);
    Assert.assertNotNull(result);
    Assert.assertNull(result.getMonth());
    Assert.assertNull(result.getDate());
    Assert.assertNull(result.getYear());
    Assert.assertNull(result.getDate());
  }

  @Test
  public void testAllNulls() {
    DateInterpretationResult result = DateInterpreter.interpretRecordedDate(null, null, null, null);
    Assert.assertNotNull(result);
    Assert.assertNull(result.getMonth());
    Assert.assertNull(result.getDate());
    Assert.assertNull(result.getYear());
    Assert.assertNull(result.getDate());
  }

  @Test
  public void testDateStrings() {
    assertValidDate("1999-07-19", "1999-07-19");
    assertValidDate("1999-07-19", "19-07-1999");
    assertValidDate("1999-07-19", "07-19-1999");
    assertValidDate("1999-09-07", "07-09-1999");
    assertValidDate("1999-06-07", "07-06-1999");
    assertValidDate("1999-07-19", "19/7/1999");
    assertValidDate("1999-07-19", "1999.7.19");
    assertValidDate("1999-07-19", "19.7.1999");
  }

  private void assertValidDate(String expected, String input){
    assertDate(expected, DateInterpreter.interpretRecordedDate(null, null, null, input));
  }

  /**
   * @param expected expected date in ISO yyyy-MM-dd format
   */
  private void assertDate(String expected, DateInterpretationResult result){
    if (expected == null) {
      Assert.assertNull(result.getDate());
    } else {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
      Assert.assertNotNull("Missing date", result.getDate());
      Assert.assertEquals(expected, sdf.format(result.getDate()));
    }
  }
}
