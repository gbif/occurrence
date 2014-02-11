package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.processor.interpreting.result.DateYearMonthDay;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TemporalInterpreterTest {

  @Test
  public void testAllDates() {
    VerbatimOccurrence v = new VerbatimOccurrence();
    v.setVerbatimField(DwcTerm.year, "1879");
    v.setVerbatimField(DwcTerm.month, "11 ");
    v.setVerbatimField(DwcTerm.day, "1");
    v.setVerbatimField(DwcTerm.eventDate, "1.11.1879");
    v.setVerbatimField(DwcTerm.dateIdentified, "2012-01-11");
    v.setVerbatimField(DcTerm.modified, "2014-01-11");

    Occurrence o = new Occurrence();
    TemporalInterpreter.interpretTemporal(v,o);

    assertDate("2014-01-11", o.getModified());
    assertDate("2012-01-11", o.getDateIdentified());
    assertDate("1879-11-01", o.getEventDate());
    assertEquals(1879, o.getYear().intValue());
    assertEquals(11, o.getMonth().intValue());
    assertEquals(1, o.getDay().intValue());

    assertEquals(0, o.getIssues().size());
  }

  @Test
  public void testLikelyYearRanges() {
    assertTrue(TemporalInterpreter.VALID_RECORDED_YEAR_RANGE.contains(1900));
    assertTrue(TemporalInterpreter.VALID_RECORDED_YEAR_RANGE.contains(1901));
    assertTrue(TemporalInterpreter.VALID_RECORDED_YEAR_RANGE.contains(2010));
    assertTrue(TemporalInterpreter.VALID_RECORDED_YEAR_RANGE.contains(2014));

    assertFalse(TemporalInterpreter.VALID_RECORDED_YEAR_RANGE.contains(1580));
    assertFalse(TemporalInterpreter.VALID_RECORDED_YEAR_RANGE.contains(900));
    assertFalse(TemporalInterpreter.VALID_RECORDED_YEAR_RANGE.contains(90));
    assertFalse(TemporalInterpreter.VALID_RECORDED_YEAR_RANGE.contains(0));
    assertFalse(TemporalInterpreter.VALID_RECORDED_YEAR_RANGE.contains(-1900));

    assertFalse(TemporalInterpreter.VALID_RECORDED_YEAR_RANGE.contains(2100));
  }

  @Test
  public void testLikelyIdentifiedRanges() {
    Calendar cal = Calendar.getInstance();

    cal.set(1987, 0, 31);
    assertTrue(TemporalInterpreter.VALID_RECORDED_DATE_RANGE.contains(cal.getTime()));

    cal.set(1787, 2, 27);
    assertTrue(TemporalInterpreter.VALID_RECORDED_DATE_RANGE.contains(cal.getTime()));

    cal.set(2014, 0, 11);
    assertTrue(TemporalInterpreter.VALID_RECORDED_DATE_RANGE.contains(cal.getTime()));

    cal.set(2020, 0, 31);
    assertFalse(TemporalInterpreter.VALID_RECORDED_DATE_RANGE.contains(cal.getTime()));

    assertFalse(TemporalInterpreter.VALID_RECORDED_DATE_RANGE.contains(cal.getTime()));
  }

  @Test
  public void testIdentificationDates() {
    assertDate("2013-05-10", TemporalInterpreter.interpretDate("2013-05-10", TemporalInterpreter.VALID_RECORDED_DATE_RANGE, null).getPayload());
  }

  @Test
  public void testGoodDate() {
    ParseResult<DateYearMonthDay> result = interpretRecordedDate("1984", "3", "22", null);
    assertResult(1984, 3, 22, "1984-03-22", result);
  }

  @Test
  public void testGoodOldDate() {
    ParseResult<DateYearMonthDay> result = interpretRecordedDate("1957", "3", "22", null);
    assertResult(1957, 3, 22, "1957-03-22", result);
  }

  @Test
  public void test0Month() {
    ParseResult<DateYearMonthDay> result = interpretRecordedDate("1984", "0", "22", null);
    assertResult(1984, null, 22, null, result);
  }

  @Test
  public void testOldYear() {
    ParseResult<DateYearMonthDay> result = interpretRecordedDate("1599", "3", "22", null);
    assertNullResult(result);
  }

  @Test
  public void testFutureYear() {
    ParseResult<DateYearMonthDay> result = interpretRecordedDate("2100", "3", "22", null);
    assertNullResult(result);
  }

  @Test
  public void testBadDay() {
    ParseResult<DateYearMonthDay> result = interpretRecordedDate("1984", "3", "32", null);
    assertResult(1984, 3, null, null, result);
  }

  @Test
  public void testStringGood() {
    ParseResult<DateYearMonthDay> result = interpretRecordedDate(null, null, null, "1984-03-22");
    assertResult(1984, 3, 22, "1984-03-22", result);
  }

  @Test
  public void testStringBad() {
    ParseResult<DateYearMonthDay> result = interpretRecordedDate(null, null, null, "22-17-1984");
    assertNullResult(result);
  }

  @Test
  public void testStringWins() {
    ParseResult<DateYearMonthDay> result = interpretRecordedDate("1984", "3", null, "1984-03-22");
    assertResult(1984, 3, 22, "1984-03-22", result);
  }

  @Test
  public void testStringLoses() {
    ParseResult<DateYearMonthDay> result = interpretRecordedDate("1984", "3", null, "22-17-1984");
    assertResult(1984, 3, null, null, result);
  }

  @Test
  public void testOnlyMonth() {
    ParseResult<DateYearMonthDay> result = interpretRecordedDate(null, "3", null, null);
    assertResult(null, 3, null, null, result);
  }

  @Test
  public void testOnlyDay() {
    ParseResult<DateYearMonthDay> result = interpretRecordedDate(null, null, "23", null);
    assertResult(null, null, 23, null, result);
  }

  @Test
  public void testAllNulls() {
    ParseResult<DateYearMonthDay> result = interpretRecordedDate(null, null, null, null);
    assertNullResult(result);
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
    assertDate(expected, interpretRecordedDate(null, null, null, input).getPayload());
  }

  /**
   * @param expected expected date in ISO yyyy-MM-dd format
   */
  private void assertDate(String expected, DateYearMonthDay result){
    if (expected == null) {
      assertNull(result.getDate());
    } else {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
      assertNotNull("Missing date", result.getDate());
      assertEquals(expected, sdf.format(result.getDate()));
    }
  }

  /**
   * @param expected expected date in ISO yyyy-MM-dd format
   */
  private void assertDate(String expected, Date result){
    if (expected == null) {
      assertNull(result);
    } else {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
      assertNotNull("Missing date", result);
      assertEquals(expected, sdf.format(result));
    }
  }

  private void assertInts(Integer expected, Integer x) {
    if (expected == null) {
      assertNull(x);
    } else {
      assertEquals(expected, x);
    }
  }

  private void assertResult(Integer y, Integer m, Integer d, String date, ParseResult<DateYearMonthDay> result){
    assertNotNull(result);
    assertInts(y, result.getPayload().getYear());
    assertInts(m, result.getPayload().getMonth());
    assertInts(d, result.getPayload().getDay());
    assertDate(date, result.getPayload());
  }

  private void assertNullResult(ParseResult<DateYearMonthDay> result){
    assertNotNull(result);
    assertNull(result.getPayload());
  }

  private ParseResult<DateYearMonthDay> interpretRecordedDate(String y, String m, String d, String date){
    VerbatimOccurrence v = new VerbatimOccurrence();
    v.setVerbatimField(DwcTerm.year, y);
    v.setVerbatimField(DwcTerm.month, m);
    v.setVerbatimField(DwcTerm.day, d);
    v.setVerbatimField(DwcTerm.eventDate, date);

    return TemporalInterpreter.interpretRecordedDate(v);
  }

}
