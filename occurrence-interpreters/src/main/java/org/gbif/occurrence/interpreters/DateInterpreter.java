package org.gbif.occurrence.interpreters;

import org.gbif.common.parsers.ParseResult;
import org.gbif.common.parsers.date.DateParseUtils;
import org.gbif.common.parsers.date.YearMonthDay;
import org.gbif.occurrence.interpreters.result.DateInterpretationResult;

import java.util.Calendar;
import java.util.Date;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interprets date representations into a Date.
 */
public class DateInterpreter {

  private static final Logger LOG = LoggerFactory.getLogger(DateInterpreter.class);
  private static final Integer EARLIEST_RECORDED_YEAR = 1700;
  // max is next year
  private static final Integer LATEST_RECORDED_YEAR = Calendar.getInstance().get(Calendar.YEAR) + 1;
  // we accept 13h difference between dates due to timezone trouble
  private static final long MAX_DIFFERENCE = 13 * 1000*60*60;

  private DateInterpreter() {
  }

  /**
   * Given possibly both of year, month, day and a dateString, produces a single date.
   * When year, month and day are all populated and parseable they are given priority,
   * but if any field is missing or illegal and dateString is parseable dateString is preferred.
   *
   * Partially valid dates are not supported and null will be returned instead. The only exception is the year alone
   * which will be used as the last resort if nothing else works.
   * Years are verified to be before or next year and after 1700.
   */
  public static DateInterpretationResult interpretRecordedDate(String year, String month, String day, String dateString) {
    if (year == null && month == null && day == null && Strings.isNullOrEmpty(dateString)) {
      return new DateInterpretationResult();
    }

    boolean invalid = false;
    boolean mismatch = false;
    boolean unlikely = false;

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

    // string based date
    Date stringDate = interpretDate(dateString);
    Integer stringYear = year(stringDate);

    // if both inputs exist verify that they match
    if (ymdYear != null && stringYear != null) {
      if (!ymdYear.equals(stringYear)) {
        LOG.debug("String and YMD based years differ: {} vs {}.", ymdYear, stringYear);
        mismatch = true;
        // ignore string based date
        stringDate = null;

      } else if (ymdDate != null && stringDate != null) {
        long diff = ymdDate.getTime() - stringDate.getTime();
        if (diff > MAX_DIFFERENCE) {
          LOG.debug("String and YMD based dates differ: {} vs {}.", ymdDate, stringDate);
          mismatch = true;
          // ignore string based date
          stringDate = null;
        }
      }
    }

    // stringDate will be null if not matching with YMD
    if (stringDate != null) {
      // verify we've got a sensible year
      if (stringYear >= EARLIEST_RECORDED_YEAR && stringYear <= LATEST_RECORDED_YEAR) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(stringDate);
        return new DateInterpretationResult(stringYear, cal.get(Calendar.MONTH)+1, cal.get(Calendar.DAY_OF_MONTH), null, stringDate, invalid, mismatch, unlikely);
      }
      unlikely = true;
      LOG.debug("Bad recording year: [{}] / [{}].", dateString, ymd);

    }

    // try to use partial dates from YMD as last resort:
    return interpretYMD(ymd, ymdDate, invalid, mismatch, unlikely);
  }

  private static Integer year(Date d) {
    if (d != null) {
      Calendar cal = Calendar.getInstance();
      cal.setTime(d);
      return cal.get(Calendar.YEAR);
    }
    return null;
  }

  private static DateInterpretationResult interpretYMD(YearMonthDay ymd, Date ymdDate,
                              boolean invalid, boolean mismatch, boolean unlikely) {
    Integer year = ymd.getIntegerYear();
    if (year != null && (year < EARLIEST_RECORDED_YEAR || year > LATEST_RECORDED_YEAR)) {
      year = null;
      ymdDate = null;
      unlikely = true;
    }
    return new DateInterpretationResult(year, ymd.getIntegerMonth(), ymd.getIntegerDay(), null, ymdDate, invalid, mismatch, unlikely);
  }

  // TODO deal with partial ISO dates: http://dev.gbif.org/issues/browse/POR-1742
  public static Date interpretDate(String dateString) {
    return interpretDate(dateString, null);
  }

  // TODO deal with partial ISO dates: http://dev.gbif.org/issues/browse/POR-1742
  public static Date interpretDate(String dateString, Integer minYear) {
    if (!Strings.isNullOrEmpty(dateString)) {
      ParseResult<Date> result = DateParseUtils.parse(dateString);
      if (result.isSuccessful()) {
        Date d = result.getPayload();
        // check year makes sense
        if (minYear != null) {
          Calendar c = Calendar.getInstance();
          c.setTime(d);
          if (c.get(Calendar.YEAR) < minYear) {
            LOG.debug("Unlikely date parsed, ignore [{}].", dateString);
            return null;
          }
        }
        return d;
      } else {
        LOG.debug("Failed to parse dateString [{}].", dateString);
      }
    }
    return null;
  }
}
