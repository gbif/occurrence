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
package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.util.IsoDateInterval;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TemporalInterpreterTest {

  @Test
  public void testAllDates() {
    VerbatimOccurrence v = new VerbatimOccurrence();
    v.setVerbatimField(DwcTerm.year, "1879");
    v.setVerbatimField(DwcTerm.month, "11 "); //keep the space at the end
    v.setVerbatimField(DwcTerm.day, "1");
    v.setVerbatimField(DwcTerm.eventDate, "1.11.1879/3.12.1879");
    v.setVerbatimField(DwcTerm.dateIdentified, "2012-01-11");
    v.setVerbatimField(DcTerm.modified, "2014-01-11");

    Occurrence o = new Occurrence();
    TemporalInterpreter.interpretTemporal(v, o);

    assertDate("2014-01-11", o.getModified());
    assertDate("2012-01-11", o.getDateIdentified());
    assertEquals("1879-11-01/1879-12-03", o.getEventDate().toString());
    assertEquals(1879, o.getYear());
    assertNull(o.getMonth());
    assertNull(o.getDay());

    assertEquals(0, o.getIssues().size());
  }

  @Test
  public void testInterpretRecordedDate(){
    OccurrenceParseResult<IsoDateInterval> result;

    result = TemporalInterpreter.interpretRecordedDate("2005", "1", "", "2005-01-01", "", "");
    assertEquals(YearMonth.of(2005, 1), result.getPayload().getFrom());
    assertEquals(1, result.getIssues().size());
    assertEquals(OccurrenceIssue.RECORDED_DATE_MISMATCH, result.getIssues().iterator().next());

    //ensure that eventDate with more precision will not record an issue and the one with most precision
    //will be returned
    result = TemporalInterpreter.interpretRecordedDate("1996", "1", "26", "1996-01-26T01:00Z", "", "");
    assertEquals(ZonedDateTime.of(LocalDateTime.of(1996, 1, 26, 1, 0), ZoneOffset.UTC), result.getPayload().getFrom());
    assertEquals(0, result.getIssues().size());

    //check date ranges handled
    result = TemporalInterpreter.interpretRecordedDate("1996", "1", "26", "1996-01-26T01:02:03Z/1996-01-31T04:05:06Z", "", "");
    assertEquals(ZonedDateTime.of(LocalDateTime.of(1996, 1, 26, 01, 02, 03), ZoneOffset.UTC), result.getPayload().getFrom());
    assertEquals(ZonedDateTime.of(LocalDateTime.of(1996, 1, 31, 04, 05, 06), ZoneOffset.UTC), result.getPayload().getTo());
    assertEquals(0, result.getIssues().size());

    //check date ranges handled
    result = TemporalInterpreter.interpretRecordedDate("1996", "1", "26", "1996-01-26T01:02:03/1996-01-31T04:05:06", "", "");
    assertEquals(LocalDateTime.of(1996, 1, 26, 01, 02, 03), result.getPayload().getFrom());
    assertEquals(LocalDateTime.of(1996, 1, 31, 04, 05, 06), result.getPayload().getTo());
    assertEquals(0, result.getIssues().size());

    //check date ranges handled
    result = TemporalInterpreter.interpretRecordedDate("1996", "1", "26", "1996-01-26/1996-01-31", "", "");
    assertEquals(LocalDate.of(1996, 1, 26), result.getPayload().getFrom());
    assertEquals(LocalDate.of(1996, 1, 31), result.getPayload().getTo());
    assertEquals(0, result.getIssues().size());

    //check date ranges handled
    result = TemporalInterpreter.interpretRecordedDate("1996", "", "", "1996-01/1996-02", "", "");
    assertEquals(YearMonth.of(1996, 1), result.getPayload().getFrom());
    assertEquals(YearMonth.of(1996, 2), result.getPayload().getTo());
    assertEquals(0, result.getIssues().size());

    //check date ranges handled
    result = TemporalInterpreter.interpretRecordedDate("1996", "", "", "1996", "", "");
    assertEquals(Year.of(1996), result.getPayload().getFrom());
    assertEquals(Year.of(1996), result.getPayload().getTo());
    assertEquals(0, result.getIssues().size());

    // if dates contradict, return the most that agrees
    result = TemporalInterpreter.interpretRecordedDate("2005", "1", "2", "2005-01-05", "", "");
    assertEquals(YearMonth.of(2005, 01), result.getPayload().getFrom());
    assertEquals(YearMonth.of(2005, 01), result.getPayload().getTo());
    assertEquals(1, result.getIssues().size());
    assertEquals(OccurrenceIssue.RECORDED_DATE_MISMATCH, result.getIssues().iterator().next());

    // a long time ago (note in Hive from_unixtime has problems with 16th century dates)
    result = TemporalInterpreter.interpretRecordedDate("1566", "", "", "1566", "", "");
    assertEquals(Year.of(1566), result.getPayload().getFrom());
    assertEquals(Year.of(1566), result.getPayload().getTo());
    assertEquals(0, result.getIssues().size());
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

    v.setVerbatimField(DwcTerm.dateIdentified, "1997");
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

    o = new Occurrence();
    Calendar cal = Calendar.getInstance();
    v.setVerbatimField(DcTerm.modified, (cal.get(Calendar.YEAR) + 1) + "-01-11");
    TemporalInterpreter.interpretTemporal(v, o);
    assertEquals(1, o.getIssues().size());
    assertEquals(OccurrenceIssue.MODIFIED_DATE_UNLIKELY, o.getIssues().iterator().next());

    o = new Occurrence();
    v.setVerbatimField(DcTerm.modified, "1969-12-31");
    TemporalInterpreter.interpretTemporal(v, o);
    assertEquals(1, o.getIssues().size());
    assertEquals(OccurrenceIssue.MODIFIED_DATE_UNLIKELY, o.getIssues().iterator().next());

    o = new Occurrence();
    v.setVerbatimField(DcTerm.modified, "2018-10-15 16:21:48");
    TemporalInterpreter.interpretTemporal(v, o);
    assertEquals(0, o.getIssues().size());
    assertDate("2018-10-15", o.getModified());
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
    ParseResult<IsoDateInterval> result = interpretRecordedDate("1984", "3", "22", null);
    assertRangeResult(1984, 3, 22, result);
  }

  @Test
  public void testGoodOldDate() {
    ParseResult<IsoDateInterval> result = interpretRecordedDate("1957", "3", "22", null);
    assertRangeResult(1957, 3, 22, result);
  }

  /**
   * 0 month now fails.
   */
  @Test
  public void test0Month() {
    ParseResult<IsoDateInterval> result = interpretRecordedDate("1984", "0", "22", null);
    //assertResult(1984, null, 22, null, result);
    assertFalse(result.isSuccessful());
  }

  @Test
  public void testOldYear() {
    OccurrenceParseResult<IsoDateInterval> result = interpretRecordedDate("1499", "3", "22", null);
    assertNullPayload(result, OccurrenceIssue.RECORDED_DATE_UNLIKELY);
  }

  @Test
  public void testFutureYear() {
    OccurrenceParseResult<IsoDateInterval> result = interpretRecordedDate("2100", "3", "22", null);
    assertNullPayload(result, OccurrenceIssue.RECORDED_DATE_UNLIKELY);
  }

  @Test
  public void testBadDay() {
    OccurrenceParseResult<IsoDateInterval> result = interpretRecordedDate("1984", "3", "32", null);
    assertNullPayload(result, OccurrenceIssue.RECORDED_DATE_INVALID);
  }

  @Test
  public void testStringGood() {
    ParseResult<IsoDateInterval> result = interpretRecordedDate(null, null, null, "1984-03-22");
    assertRangeResult(1984, 3, 22, result);
  }

  @Test
  public void testStringTimestamp() {
    ParseResult<IsoDateInterval> result = interpretRecordedDate(null, null, null, "1984-03-22T00:00");
    assertRangeResult(LocalDateTime.of(1984, 3, 22, 0, 0), result);
  }

  @Test
  public void testStringBad() {
    OccurrenceParseResult<IsoDateInterval> result = interpretRecordedDate(null, null, null, "22-17-1984");
    assertNullPayload(result, OccurrenceIssue.RECORDED_DATE_INVALID);
  }

  @Test
  public void testStrange() {
    OccurrenceParseResult<IsoDateInterval> result = interpretRecordedDate("16", "6", "1990", "16-6-1990");
    assertRangeResult(1990, 6, 16, result);
    // TODO assertEquals(ParseResult.CONFIDENCE.PROBABLE, result.getConfidence());
    assertEquals(OccurrenceIssue.RECORDED_DATE_INVALID, result.getIssues().iterator().next());
  }

  @Test
  public void testStringLoses() {
    OccurrenceParseResult<IsoDateInterval> result = interpretRecordedDate("1984", "3", null, "22-17-1984");
    assertRangeResult(1984, 3, result);
    assertEquals(OccurrenceIssue.RECORDED_DATE_INVALID, result.getIssues().iterator().next());
  }

  // these two tests demonstrate the problem from POR-2120
  @Test
  public void testOnlyYear() {
    ParseResult<IsoDateInterval> result = interpretRecordedDate("1984", null, null, null);
    assertRangeResult(1984, result);

    result = interpretRecordedDate(null, null, null, "1984");
    assertRangeResult(1984, result);

    result = interpretRecordedDate("1984", null, null, "1984");
    assertRangeResult(1984, result);
  }

  @Test
  public void testYearWithZeros() {
    // providing 0 will cause a RECORDED_DATE_MISMATCH since 0 could be null but also January
    OccurrenceParseResult<IsoDateInterval> result = interpretRecordedDate("1984", "0", "0", "1984");
    assertRangeResult(1984, result);
    // TODO assertEquals(ParseResult.CONFIDENCE.PROBABLE, result.getConfidence());
    assertEquals(OccurrenceIssue.RECORDED_DATE_INVALID, result.getIssues().iterator().next());

    result = interpretRecordedDate(null, null, null, "1984");
    assertEquals(ParseResult.CONFIDENCE.DEFINITE, result.getConfidence());
    assertTrue(result.getIssues().isEmpty());

    // This is not supported
    result = interpretRecordedDate("1984", "0", "0", null);
    assertNullPayload(result, OccurrenceIssue.RECORDED_DATE_INVALID);

    result = interpretRecordedDate(null, null, null, "0-0-1984");
    assertEquals(ParseResult.STATUS.FAIL, result.getStatus());
    assertNull(result.getPayload());
  }

  @Test
  public void testYearMonthNoDay() {
    OccurrenceParseResult<IsoDateInterval> result = interpretRecordedDate("1984", "3", null, null);
    assertRangeResult(1984, 3, result);

    result = interpretRecordedDate("1984", "3", null, "1984-03");
    assertRangeResult(1984, 3,result);

    result = interpretRecordedDate(null, null, null, "1984-03");
    assertRangeResult(1984, 3, result);

    result = interpretRecordedDate("1984", "3", null, "1984-03");
    assertRangeResult(1984, 3, result);
    assertEquals(0, result.getIssues().size());
  }

  /**
   * https://github.com/gbif/parsers/issues/8
   */
  @Test
  public void testDifferentResolutions() {
    OccurrenceParseResult<IsoDateInterval> result;

    result = interpretRecordedDate("1984", "3", "18", "1984-03");
    assertRangeResult(1984, 3, result);
    assertEquals(1, result.getIssues().size());
    assertEquals(OccurrenceIssue.RECORDED_DATE_MISMATCH, result.getIssues().iterator().next());

    result = interpretRecordedDate("1984", "3", null, "1984-03-18");
    assertRangeResult(1984, 3, result);
    assertEquals(1, result.getIssues().size());
    assertEquals(OccurrenceIssue.RECORDED_DATE_MISMATCH, result.getIssues().iterator().next());

    result = interpretRecordedDate("1984", null, null, "1984-03-18");
    assertRangeResult(1984, result);
    assertEquals(1, result.getIssues().size());
    assertEquals(OccurrenceIssue.RECORDED_DATE_MISMATCH, result.getIssues().iterator().next());

    result = interpretRecordedDate("1984", "3", null, "1984");
    assertRangeResult(1984, result);
    assertEquals(1, result.getIssues().size());
    assertEquals(OccurrenceIssue.RECORDED_DATE_MISMATCH, result.getIssues().iterator().next());

    result = interpretRecordedDate("1984", "05", "02", "1984-05-02T19:34");
    assertRangeResult(LocalDateTime.of(1984, 5, 2, 19, 34, 00), result);
    assertEquals(0, result.getIssues().size());
  }

  /**
   * https://github.com/gbif/parsers/issues/6
   */
  @Test
  public void testLessConfidentMatch() {
    OccurrenceParseResult<IsoDateInterval> result;

    result = interpretRecordedDate("2014", "2", "5", "5/2/2014");
    assertRangeResult(2014, 2, 5, result);
    assertEquals(1, result.getIssues().size());
    assertEquals(OccurrenceIssue.RECORDED_DATE_INVALID, result.getIssues().iterator().next());
  }

  /**
   * Only month now fails
   */
  @Test
  public void testOnlyMonth() {
    ParseResult<IsoDateInterval> result = interpretRecordedDate(null, "3", null, null);
   // assertResult(null, 3, null, null, result);
    assertFalse(result.isSuccessful());
  }

  /**
   * Only day now fails
   */
  @Test
  public void testOnlyDay() {
    ParseResult<IsoDateInterval> result = interpretRecordedDate(null, null, "23", null);
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
    ParseResult<IsoDateInterval> result = interpretEventDate(DateFormatUtils.ISO_DATETIME_FORMAT.format(Calendar.getInstance()));
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
    OccurrenceParseResult<IsoDateInterval> result = interpretRecordedDate(null, null, null, null);
    // null and/or empty string will not return an error
    assertNullPayload(result, null);
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

    assertRangeResult(LocalDateTime.of(1999, 7, 19, 0, 0), interpretRecordedDate(null, null, null, "1999-07-19T00:00:00"));
  }

  private void testEventDate(int y, int m, int d, String input) {
    assertRangeResult(y, m, d, interpretRecordedDate(null, null, null, input));
  }

  @Test
  public void testIsoDateIntervals() {
    // Some of the many ways of providing a range.
    assertRangeResult(Year.of(1999), interpretEventDate("1999"));
    assertRangeResult(Year.of(1999), interpretEventDate("1999/2000"));
    assertRangeResult(YearMonth.of(1999, 01), interpretEventDate("1999-01/1999-12"));
    assertRangeResult(ZonedDateTime.of(2004, 12, 30, 00, 00, 00, 00, ZoneOffset.UTC),
      interpretEventDate("2004-12-30T00:00:00+0000/2005-03-13T23:59:59+0000"));
  }

  /**
   * @param expected expected date in ISO yyyy-MM-dd format
   */
  private void assertDate(String expected, Date result) {
    if (expected == null) {
      assertNull(result);
    } else {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
      assertNotNull(result,"Missing date");
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
   */
  private void assertRangeResult(Integer y, Integer m, Integer d, ParseResult<IsoDateInterval> result) {
    // sanity checks
    assertNotNull(result);

    LocalDate localDate = result.getPayload().getFrom().query(LocalDate::from);
    assertInts(y, localDate.getYear());
    assertInts(m, localDate.getMonthValue());
    assertInts(d, localDate.getDayOfMonth());

    assertEquals(LocalDate.of(y, m, d), result.getPayload().getFrom());
  }

  private void assertRangeResult(TemporalAccessor expectedTA, ParseResult<IsoDateInterval> result) {
    assertEquals(expectedTA, result.getPayload().getFrom());
  }

  /**
   * Utility method to assert a ParseResult when a YearMonth is expected.
   * This method should not be used to test expected null results.
   */
  private void assertRangeResult(Integer y, Integer m, ParseResult<IsoDateInterval> result) {
    // sanity checks
    assertNotNull(result);

    YearMonth yearMonthDate = result.getPayload().getFrom().query(YearMonth::from);
    assertInts(y, yearMonthDate.getYear());
    assertInts(m, yearMonthDate.getMonthValue());

    assertEquals(YearMonth.of(y, m), result.getPayload().getFrom());
  }

  private void assertRangeResult(Integer y, ParseResult<IsoDateInterval> result) {
    // sanity checks
    assertNotNull(result);

    Year yearDate = result.getPayload().getFrom().query(Year::from);
    assertInts(y, yearDate.getValue());

    assertEquals(Year.of(y), result.getPayload().getFrom());
  }

  /** Utility method to assert a ParseResult when a LocalDate is expected.
   * This method should not be used to test expected null results.
   */
  private void assertResult(Integer y, Integer m, Integer d, ParseResult<TemporalAccessor> result) {
    // sanity checks
    assertNotNull(result);

    LocalDate localDate = result.getPayload().query(LocalDate::from);
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
   */
  private void assertResult(Integer y, Integer m, ParseResult<TemporalAccessor> result) {
    // sanity checks
    assertNotNull(result);

    YearMonth yearMonthDate = result.getPayload().query(YearMonth::from);
    assertInts(y, yearMonthDate.getYear());
    assertInts(m, yearMonthDate.getMonthValue());

    assertEquals(YearMonth.of(y, m), result.getPayload());
  }

  private void assertResult(Integer y, ParseResult<TemporalAccessor> result) {
    // sanity checks
    assertNotNull(result);

    Year yearDate = result.getPayload().query(Year::from);
    assertInts(y, yearDate.getValue());

    assertEquals(Year.of(y), result.getPayload());
  }

  private void assertNullPayload(OccurrenceParseResult<?> result, OccurrenceIssue expectedIssue) {
    System.out.println("Expecting null, got " + result);
    assertNotNull(result);
    assertFalse(result.isSuccessful());
    assertNull(result.getPayload());

    if(expectedIssue != null) {
      System.out.println("Expecting issues "+expectedIssue+", got " + result.getIssues());
      assertTrue(result.getIssues().contains(expectedIssue));
    }
  }

  private OccurrenceParseResult<IsoDateInterval> interpretRecordedDate(String y, String m, String d, String date) {
    VerbatimOccurrence v = new VerbatimOccurrence();
    v.setVerbatimField(DwcTerm.year, y);
    v.setVerbatimField(DwcTerm.month, m);
    v.setVerbatimField(DwcTerm.day, d);
    v.setVerbatimField(DwcTerm.eventDate, date);

    return TemporalInterpreter.interpretRecordedDate(v);
  }

  private ParseResult<IsoDateInterval> interpretEventDate(String date) {
    VerbatimOccurrence v = new VerbatimOccurrence();
    v.setVerbatimField(DwcTerm.eventDate, date);

    return TemporalInterpreter.interpretRecordedDate(v);
  }

}
