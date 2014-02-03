package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.date.DateParseUtils;
import org.gbif.common.parsers.date.YearMonthDay;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.processor.interpreting.result.DateYearMonthDay;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Set;

import com.beust.jcommander.internal.Sets;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interprets date representations into a Date.
 */
public class TemporalInterpreter {

  private static final Logger LOG = LoggerFactory.getLogger(TemporalInterpreter.class);
  // we accept 13h difference between dates due to timezone trouble
  private static final long MAX_DIFFERENCE = 13 * 1000*60*60;
  // max is next year
  @VisibleForTesting
  protected static final Range<Integer> VALID_RECORDED_YEAR_RANGE =
    Range.closed(1700, Calendar.getInstance().get(Calendar.YEAR) + 1);
  @VisibleForTesting
  protected static final Range<Date> VALID_RECORDED_DATE_RANGE =
    Range.closed(new GregorianCalendar(1700, 0, 1).getTime(), new Date());

  // modified date for a record cant be before unix time
  @VisibleForTesting
  protected static final Range<Date> VALID_MODIFIED_DATE_RANGE = Range.closed(new Date(0), new Date());

  private TemporalInterpreter() {
  }

  public static void interpretTemporal(VerbatimOccurrence verbatim, Occurrence occ) {
    ParseResult<DateYearMonthDay> eventResult = interpretRecordedDate(verbatim);
    if (eventResult.isSuccessful()) {
      occ.setEventDate(eventResult.getPayload().getDate());
      occ.setMonth(eventResult.getPayload().getMonth());
      occ.setYear(eventResult.getPayload().getYear());
      occ.setDay(eventResult.getPayload().getDay());
    }
    occ.getIssues().addAll(eventResult.getIssues());

    if (verbatim.hasField(DcTerm.modified)) {
      ParseResult<Date> parsed = interpretDate(verbatim.getField(DcTerm.modified),
                                               VALID_MODIFIED_DATE_RANGE,OccurrenceIssue.MODIFIED_DATE_UNLIKLEY);
      occ.setModified(parsed.getPayload());
      occ.getIssues().addAll(parsed.getIssues());
    }

    if (verbatim.hasField(DwcTerm.dateIdentified)) {
      ParseResult<Date> parsed = interpretDate(verbatim.getField(DwcTerm.dateIdentified),
                                               VALID_RECORDED_DATE_RANGE, OccurrenceIssue.IDENTIFIED_DATE_UNLIKLEY);
      occ.setDateIdentified(parsed.getPayload());
      occ.getIssues().addAll(parsed.getIssues());
    }
  }

  /**
   * Given possibly both of year, month, day and a dateString, produces a single date.
   * When year, month and day are all populated and parseable they are given priority,
   * but if any field is missing or illegal and dateString is parseable dateString is preferred.
   *
   * Partially valid dates are not supported and null will be returned instead. The only exception is the year alone
   * which will be used as the last resort if nothing else works.
   * Years are verified to be before or next year and after 1700.
   *
   * @return interpretation result, never null
   */
  public static ParseResult<DateYearMonthDay> interpretRecordedDate(VerbatimOccurrence verbatim) {
    final String year = verbatim.getField(DwcTerm.year);
    final String month = verbatim.getField(DwcTerm.month);
    final String day = verbatim.getField(DwcTerm.day);
    final String dateString = verbatim.getField(DwcTerm.eventDate);

    if (year == null && month == null && day == null && Strings.isNullOrEmpty(dateString)) {
      return ParseResult.fail();
    }

    Set<OccurrenceIssue> issues = Sets.newHashSet();

    /**
     * First, attempt year, month, day parsing
     * If the parse result is SUCCESS it means that a whole date could be extracted (with year,
     * month and day). If it is a failure but the normalizer returned a meaningful result (e.g. it could extract just
     * a year) we're going to return a result with all the fields set that we could parse.
     */
    // note that ymd filters out any years greater than current calendar year
    YearMonthDay ymd = DateParseUtils.normalize(year, month, day);
    ParseResult<Date> parseResult = DateParseUtils.parse(ymd.getYear(), ymd.getMonth(), ymd.getDay());
    final Date ymdDate = parseResult.getStatus() == ParseResult.STATUS.SUCCESS ? parseResult.getPayload() : null;
    final Integer ymdYear = ymd.getIntegerYear();
    if (!Strings.isNullOrEmpty(year) && ymd.getIntegerYear() == null) {
      issues.add(OccurrenceIssue.RECORDED_YEAR_UNLIKELY);
    }

    // string based date
    Date stringDate = DateParseUtils.parse(dateString).getPayload();
    Integer stringYear = year(stringDate);

    // if both inputs exist verify that they match
    if (ymdYear != null && stringYear != null) {
      if (!ymdYear.equals(stringYear)) {
        LOG.debug("String and YMD based years differ: {} vs {}.", ymdYear, stringYear);
        issues.add(OccurrenceIssue.RECORDED_DATE_MISMATCH);
        // ignore string based date
        stringDate = null;

      } else if (ymdDate != null && stringDate != null) {
        long diff = ymdDate.getTime() - stringDate.getTime();
        if (diff > MAX_DIFFERENCE) {
          LOG.debug("String and YMD based dates differ: {} vs {}.", ymdDate, stringDate);
          issues.add(OccurrenceIssue.RECORDED_DATE_MISMATCH);
          // ignore string based date
          stringDate = null;
        }
      }
    }

    ParseResult<DateYearMonthDay> result = null;
    // stringDate will be null if not matching with YMD
    if (stringDate != null) {
      // verify we've got a sensible year
      if (VALID_RECORDED_YEAR_RANGE.contains(stringYear)) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(stringDate);
        result = ParseResult.success(ParseResult.CONFIDENCE.DEFINITE,
                 new DateYearMonthDay(stringYear, cal.get(Calendar.MONTH) + 1, cal.get(Calendar.DAY_OF_MONTH), null, stringDate));
      } else {
        issues.add(OccurrenceIssue.RECORDED_YEAR_UNLIKELY);
        LOG.debug("Bad recording year: [{}] / [{}].", dateString, ymd);
      }
    }

    if (result == null) {
      if (!ymd.representsNull()) {
        // try to use partial dates from YMD as last resort
        if ( (ymd.getIntegerYear() != null && VALID_RECORDED_YEAR_RANGE.contains(ymd.getIntegerYear()))
          || (ymd.getIntegerYear() == null && !issues.contains(OccurrenceIssue.RECORDED_YEAR_UNLIKELY))
        ) {
          result = ParseResult.success(ParseResult.CONFIDENCE.DEFINITE,
                                       new DateYearMonthDay(ymd.getIntegerYear(), ymd.getIntegerMonth(), ymd.getIntegerDay(), null, ymdDate));
        }
      }
    }

    // if its still null then its a fail
    if (result == null) {
      result = ParseResult.fail();
    }

      // return result with all issues
    result.getIssues().addAll(issues);
    return result;
  }

  private static Integer year(Date d) {
    if (d != null) {
      Calendar cal = Calendar.getInstance();
      cal.setTime(d);
      return cal.get(Calendar.YEAR);
    }
    return null;
  }

  // TODO deal with partial ISO dates: http://dev.gbif.org/issues/browse/POR-1742
  public static ParseResult<Date> interpretModifiedDate(String dateString) {
    if (!Strings.isNullOrEmpty(dateString)) {
      ParseResult<Date> result = DateParseUtils.parse(dateString);
      if (result.isSuccessful()) {
        // check year makes sense
        if (VALID_MODIFIED_DATE_RANGE.contains(result.getPayload())) {
          LOG.debug("Unlikely date parsed, ignore [{}].", dateString);
          // Use correct new issue for dc modified
          result.addIssue(OccurrenceIssue.MODIFIED_DATE_UNLIKLEY);
        }
      }
      return result;
    }
    return ParseResult.fail();
  }

  public static ParseResult<Date> interpretDate(String dateString, Range<Date> likelyRange, OccurrenceIssue unlikelyIssue) {
    if (!Strings.isNullOrEmpty(dateString)) {
      ParseResult<Date> result = DateParseUtils.parse(dateString);
      if (result.isSuccessful()) {
        // check year makes sense
        if (!likelyRange.contains(result.getPayload())) {
          LOG.debug("Unlikely date parsed, ignore [{}].", dateString);
          result.addIssue(unlikelyIssue);
        }
      }
      return result;
    }
    return ParseResult.fail();
  }
}
