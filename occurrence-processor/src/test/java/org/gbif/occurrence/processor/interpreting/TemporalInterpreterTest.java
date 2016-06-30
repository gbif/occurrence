package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.junit.Test;
import org.threeten.bp.LocalDate;
import org.threeten.bp.LocalDateTime;
import org.threeten.bp.Year;
import org.threeten.bp.YearMonth;
import org.threeten.bp.temporal.ChronoUnit;
import org.threeten.bp.temporal.TemporalAccessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TemporalInterpreterTest {

  @Test
  public void testAllDates() {
    VerbatimOccurrence v = new VerbatimOccurrence();
    v.setVerbatimField(DwcTerm.year, "1879");
    v.setVerbatimField(DwcTerm.month, "11 "); //keep the space at the end
    v.setVerbatimField(DwcTerm.day, "1");
    v.setVerbatimField(DwcTerm.eventDate, "1.11.1879");
    v.setVerbatimField(DwcTerm.dateIdentified, "2012-01-11");
    v.setVerbatimField(DcTerm.modified, "2014-01-11");

    Occurrence o = new Occurrence();
    TemporalInterpreter.interpretTemporal(v, o);

    assertDate("2014-01-11", o.getModified());
    assertDate("2012-01-11", o.getDateIdentified());
    assertDate("1879-11-01", o.getEventDate());
    assertEquals(1879, o.getYear().intValue());
    assertEquals(11, o.getMonth().intValue());
    assertEquals(1, o.getDay().intValue());

    assertEquals(0, o.getIssues().size());
  }

  @Test
  public void testTemporalInterpreter(){
    assertTrue(TemporalInterpreter.isValidDate(Year.of(2005), true));
    assertTrue(TemporalInterpreter.isValidDate(YearMonth.of(2005, 1), true));
    assertTrue(TemporalInterpreter.isValidDate(LocalDate.of(2005, 1, 1), true));
    assertTrue(TemporalInterpreter.isValidDate(LocalDateTime.of(2005, 1, 1, 2, 3, 4), true));
    assertTrue(TemporalInterpreter.isValidDate(LocalDate.now(), true));
    assertTrue(TemporalInterpreter.isValidDate(LocalDateTime.now().plus(23, ChronoUnit.HOURS), true));

    // Dates out of bounds
    assertFalse(TemporalInterpreter.isValidDate(YearMonth.of(1599, 12), true));

    // we tolerate a offset of 1 day
    assertFalse(TemporalInterpreter.isValidDate(LocalDate.now().plusDays(2), true));
  }

  @Test
  public void testLikelyIdentified() {
    VerbatimOccurrence v = new VerbatimOccurrence();
    v.setVerbatimField(DwcTerm.year, "1879");
    v.setVerbatimField(DwcTerm.month, "11 ");
    v.setVerbatimField(DwcTerm.day, "1");
    v.setVerbatimField(DwcTerm.eventDate, "1.11.1879");
    v.setVerbatimField(DcTerm.modified, "2014-01-11");
    Occurrence o = new Occurrence();

    v.setVerbatimField(DwcTerm.dateIdentified, "1987-01-31");
    TemporalInterpreter.interpretTemporal(v, o);
    assertEquals(0, o.getIssues().size());

    v.setVerbatimField(DwcTerm.dateIdentified, "1787-03-27");
    TemporalInterpreter.interpretTemporal(v, o);
    assertEquals(0, o.getIssues().size());

    v.setVerbatimField(DwcTerm.dateIdentified, "2014-01-11");
    TemporalInterpreter.interpretTemporal(v, o);
    assertEquals(0, o.getIssues().size());

    Calendar cal = Calendar.getInstance();
    v.setVerbatimField(DwcTerm.dateIdentified, (cal.get(Calendar.YEAR)+1) + "-01-11");
    TemporalInterpreter.interpretTemporal(v, o);
    assertEquals(1, o.getIssues().size());
    assertEquals(OccurrenceIssue.IDENTIFIED_DATE_UNLIKELY, o.getIssues().iterator().next());

    v.setVerbatimField(DwcTerm.dateIdentified, "1599-01-11");
    TemporalInterpreter.interpretTemporal(v, o);
    assertEquals(1, o.getIssues().size());
    assertEquals(OccurrenceIssue.IDENTIFIED_DATE_UNLIKELY, o.getIssues().iterator().next());
  }

  @Test
  public void testLikelyModified() {
    VerbatimOccurrence v = new VerbatimOccurrence();
    v.setVerbatimField(DwcTerm.year, "1879");
    v.setVerbatimField(DwcTerm.month, "11 ");
    v.setVerbatimField(DwcTerm.day, "1");
    v.setVerbatimField(DwcTerm.eventDate, "1.11.1879");
    v.setVerbatimField(DwcTerm.dateIdentified, "1987-01-31");
    Occurrence o = new Occurrence();

    v.setVerbatimField(DcTerm.modified, "2014-01-11");
    TemporalInterpreter.interpretTemporal(v, o);
    assertEquals(0, o.getIssues().size());

    Calendar cal = Calendar.getInstance();
    v.setVerbatimField(DcTerm.modified, (cal.get(Calendar.YEAR) + 1) + "-01-11");
    TemporalInterpreter.interpretTemporal(v, o);
    assertEquals(1, o.getIssues().size());
    assertEquals(OccurrenceIssue.MODIFIED_DATE_UNLIKELY, o.getIssues().iterator().next());

    v.setVerbatimField(DcTerm.modified, "1969-12-31");
    TemporalInterpreter.interpretTemporal(v, o);
    assertEquals(1, o.getIssues().size());
    assertEquals(OccurrenceIssue.MODIFIED_DATE_UNLIKELY, o.getIssues().iterator().next());
  }

  @Test
  public void testLikelyRecorded() {
    VerbatimOccurrence v = new VerbatimOccurrence();
    Calendar cal = Calendar.getInstance();
    v.setVerbatimField(DwcTerm.eventDate, "24.12." + (cal.get(Calendar.YEAR) + 1));

    Occurrence o = new Occurrence();
    TemporalInterpreter.interpretTemporal(v, o);

    assertEquals(1, o.getIssues().size());
    assertEquals(OccurrenceIssue.RECORDED_DATE_UNLIKELY, o.getIssues().iterator().next());
  }

  @Test
  public void testGoodDate() {
    ParseResult<TemporalAccessor> result = interpretRecordedDate("1984", "3", "22", null);
    assertResult(1984, 3, 22, result);
  }

  @Test
  public void testGoodOldDate() {
    ParseResult<TemporalAccessor> result = interpretRecordedDate("1957", "3", "22", null);
    assertResult(1957, 3, 22, result);
  }

  /**
   * 0 month now fails.
   */
  @Test
  public void test0Month() {
    ParseResult<TemporalAccessor> result = interpretRecordedDate("1984", "0", "22", null);
    //assertResult(1984, null, 22, null, result);
    assertFalse(result.isSuccessful());
  }

  @Test
  public void testOldYear() {
    ParseResult<TemporalAccessor> result = interpretRecordedDate("1599", "3", "22", null);
    assertNullResult(result);
  }

  @Test
  public void testFutureYear() {
    ParseResult<TemporalAccessor> result = interpretRecordedDate("2100", "3", "22", null);
    assertNullResult(result);
  }

  @Test
  public void testBadDay() {
    ParseResult<TemporalAccessor> result = interpretRecordedDate("1984", "3", "32", null);
    assertNullResult(result);
  }

  @Test
  public void testStringGood() {
    ParseResult<TemporalAccessor> result = interpretRecordedDate(null, null, null, "1984-03-22");
    assertResult(1984, 3, 22, result);
  }

  @Test
  public void testStringTimestamp() {
    ParseResult<TemporalAccessor> result = interpretRecordedDate(null, null, null, "1984-03-22T00:00");
    assertResult(LocalDateTime.of(1984, 3, 22, 0, 0), result);
  }

  @Test
  public void testStringBad() {
    ParseResult<TemporalAccessor> result = interpretRecordedDate(null, null, null, "22-17-1984");
    assertNullResult(result);
  }

  @Test
  public void testStringWins() {
    ParseResult<TemporalAccessor> result = interpretRecordedDate("1984", "3", null, "1984-03-22");
    assertResult(1984, 3, 22, result);
  }

  @Test
  public void testStrange() {
    OccurrenceParseResult<TemporalAccessor> result = interpretRecordedDate("16", "6", "1990", "16-6-1990");
    assertResult(1990, 6, 16, result);
    assertEquals(ParseResult.CONFIDENCE.PROBABLE, result.getConfidence());
    assertEquals(OccurrenceIssue.RECORDED_DATE_MISMATCH, result.getIssues().iterator().next());
  }

  @Test
  public void testStringLoses() {
    OccurrenceParseResult<TemporalAccessor> result = interpretRecordedDate("1984", "3", null, "22-17-1984");
    assertResult(1984, 3, result);
    assertEquals(OccurrenceIssue.RECORDED_DATE_MISMATCH, result.getIssues().iterator().next());
  }

  // these two tests demonstrate the problem from POR-2120
  @Test
  public void testOnlyYear() {
    ParseResult<TemporalAccessor> result = interpretRecordedDate("1984", null, null, null);
    assertResult(1984, result);

    result = interpretRecordedDate(null, null, null, "1984");
    assertResult(1984, result);

    result = interpretRecordedDate("1984", null, null, "1984");
    assertResult(1984, result);
  }

  @Test
  public void testYearWithZeros() {
    // providing 0 will cause a RECORDED_DATE_MISMATCH since 0 could be null but also January
    OccurrenceParseResult<TemporalAccessor> result = interpretRecordedDate("1984", "0", "0", "1984");
    assertResult(1984, result);
    assertEquals(ParseResult.CONFIDENCE.PROBABLE, result.getConfidence());
    assertEquals(OccurrenceIssue.RECORDED_DATE_MISMATCH, result.getIssues().iterator().next());

    result = interpretRecordedDate(null, null, null, "1984");
    assertEquals(ParseResult.CONFIDENCE.DEFINITE, result.getConfidence());
    assertTrue(result.getIssues().isEmpty());

    // This is not supported for the moment
    result = interpretRecordedDate("1984", "0", "0", null);
    assertNullResult(result);

    result = interpretRecordedDate(null, null, null, "0-0-1984");
    assertEquals(ParseResult.STATUS.FAIL, result.getStatus());
    assertNull(result.getPayload());
  }

  @Test
  public void testYearMonthNoDay() {
    ParseResult<TemporalAccessor> result = interpretRecordedDate("1984", "3", null, null);
    assertResult(1984, 3, result);

    result = interpretRecordedDate("1984", "3", null, "1984-03");
    assertResult(1984, 3,result);

    result = interpretRecordedDate(null, null, null, "1984-03");
    assertResult(1984, 3, result);
  }

  /**
   * Only month now fails
   */
  @Test
  public void testOnlyMonth() {
    ParseResult<TemporalAccessor> result = interpretRecordedDate(null, "3", null, null);
   // assertResult(null, 3, null, null, result);
    assertFalse(result.isSuccessful());
  }

  /**
   * Only day now fails
   */
  @Test
  public void testOnlyDay() {
    ParseResult<TemporalAccessor> result = interpretRecordedDate(null, null, "23", null);
    //assertResult(null, null, 23, null, result);
    assertFalse(result.isSuccessful());
  }

  /**
   * Tests that a date representing 'now' is interpreted with CONFIDENCE.DEFINITE even after TemporalInterpreter
   * was instantiated. See POR-2860.
   */
  @Test
  public void testNow() {

    // Makes sure the static content is loaded
    ParseResult<TemporalAccessor> result = interpretEventDate(DateFormatUtils.ISO_DATETIME_FORMAT.format(Calendar.getInstance()));
    assertEquals(ParseResult.CONFIDENCE.DEFINITE, result.getConfidence());

    // Sorry for this Thread.sleep, we need to run the TemporalInterpreter at least 1 second later until
    // we refactor to inject a Calendar or we move to new Java 8 Date/Time API
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      fail(e.getMessage());
    }

    Calendar cal = Calendar.getInstance();
    result = interpretEventDate(DateFormatUtils.ISO_DATETIME_FORMAT.format(cal.getTime()));
    assertEquals(ParseResult.CONFIDENCE.DEFINITE, result.getConfidence());
  }

  @Test
  public void testAllNulls() {
    ParseResult<TemporalAccessor> result = interpretRecordedDate(null, null, null, null);
    assertNullResult(result);
  }

  @Test
  public void testDateStrings() {
    testEventDate(1999, 7, 19, "1999-07-19");
    testEventDate(1999, 7, 19, "19-07-1999");
    testEventDate(1999, 7, 19, "07-19-1999");
    testEventDate(1999, 7, 19, "19/7/1999");
    testEventDate(1999, 7, 19, "1999.7.19");
    testEventDate(1999, 7, 19, "19.7.1999");
    testEventDate(1999, 7, 19, "19990719");
    testEventDate(2012, 5, 6, "20120506");

    assertResult(LocalDateTime.of(1999, 7, 19, 0, 0), interpretRecordedDate(null, null, null, "1999-07-19T00:00:00"));
  }

  private void testEventDate(int y, int m, int d, String input) {
    assertResult(y, m, d, interpretRecordedDate(null, null, null, input));
  }

  /**
   * @param expected expected date in ISO yyyy-MM-dd format
   */
  private void assertDate(String expected, Date result) {
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

  /**
   * Utility method to assert a ParseResult when a LocalDate is expected.
   * This method should not be used to test expected null results.
   * @param y
   * @param m
   * @param d
   * @param result
   */
  private void assertResult(Integer y, Integer m, Integer d, ParseResult<TemporalAccessor> result) {
    // sanity checks
    assertNotNull(result);

    LocalDate localDate = result.getPayload().query(LocalDate.FROM);
    assertInts(y, localDate.getYear());
    assertInts(m, localDate.getMonthValue());
    assertInts(d, localDate.getDayOfMonth());

    assertEquals(LocalDate.of(y, m, d), result.getPayload());
  }

  private void assertResult(TemporalAccessor expectedTA, ParseResult<TemporalAccessor> result) {
    assertEquals(expectedTA, result.getPayload());
  }

  /**
   * Utility method to assert a ParseResult when a YearMonth is expected.
   * This method should not be used to test expected null results.
   * @param y
   * @param m
   * @param result
   */
  private void assertResult(Integer y, Integer m, ParseResult<TemporalAccessor> result) {
    // sanity checks
    assertNotNull(result);

    YearMonth yearMonthDate = result.getPayload().query(YearMonth.FROM);
    assertInts(y, yearMonthDate.getYear());
    assertInts(m, yearMonthDate.getMonthValue());

    assertEquals(YearMonth.of(y, m), result.getPayload());
  }

  private void assertResult(Integer y, ParseResult<TemporalAccessor> result) {
    // sanity checks
    assertNotNull(result);

    Year yearDate = result.getPayload().query(Year.FROM);
    assertInts(y, yearDate.getValue());

    assertEquals(Year.of(y), result.getPayload());
  }

  private void assertNullResult(ParseResult<TemporalAccessor> result) {
    assertFalse(result.isSuccessful());
    assertNotNull(result);
    assertNull(result.getPayload());
  }

  private OccurrenceParseResult<TemporalAccessor> interpretRecordedDate(String y, String m, String d, String date) {
    VerbatimOccurrence v = new VerbatimOccurrence();
    v.setVerbatimField(DwcTerm.year, y);
    v.setVerbatimField(DwcTerm.month, m);
    v.setVerbatimField(DwcTerm.day, d);
    v.setVerbatimField(DwcTerm.eventDate, date);

    return TemporalInterpreter.interpretRecordedDate(v);
  }

  private ParseResult<TemporalAccessor> interpretEventDate(String date) {
    VerbatimOccurrence v = new VerbatimOccurrence();
    v.setVerbatimField(DwcTerm.eventDate, date);

    return TemporalInterpreter.interpretRecordedDate(v);
  }

}
