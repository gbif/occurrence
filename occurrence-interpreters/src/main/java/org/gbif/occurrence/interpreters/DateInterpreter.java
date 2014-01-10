package org.gbif.occurrence.interpreters;

import org.gbif.common.parsers.ParseResult;
import org.gbif.common.parsers.date.DateParseUtils;
import org.gbif.common.parsers.date.YearMonthDay;
import org.gbif.occurrence.interpreters.result.DateInterpretationResult;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interprets date representations into a Date.
 */
public class DateInterpreter {

  private static final Logger LOG = LoggerFactory.getLogger(DateInterpreter.class);

  private static final String OUTPUT_DATE_FORMAT = "yyyy-MM-dd";
  private static final Integer EARLIEST_YEAR = 1700;

  private DateInterpreter() {
  }

  /**
   * Given possibly both of year, month, day and a dateString, produces a single date. When year, month and day are
   * all populated and parseable they are given priority, but if any field is missing and dateString is parseable,
   * dateString is preferred.
   */
  public static DateInterpretationResult interpretDate(String year, String month, String day, String dateString) {
    if (year == null && month == null && day == null && dateString == null) return new DateInterpretationResult();

    boolean tryDateString = (dateString != null);
    Date finalDate = null;
    // first, attempt year, month, day parsing

    // note that ymd filters out any years greater than current calendar year
    YearMonthDay ymd = DateParseUtils.normalize(year, month, day);
    ParseResult<Date> parseResult = DateParseUtils.parse(ymd.getYear(), ymd.getMonth(), ymd.getDay());
    // DateFormats are not thread safe, hence a new one for each call. Returning a formatted string is itself a
    // legacy holdover.
    SimpleDateFormat sdf = new SimpleDateFormat(OUTPUT_DATE_FORMAT);

    /**
     * If the parse result is SUCCESS it means that a whole date could be extracted (with year,
     * month and day). If it is a failure but the normalizer returned a meaningful result (e.g. it could extract just
     * a year) we're going to return a result with all the fields set that we could parse.
     */
    DateInterpretationResult result = new DateInterpretationResult();
    switch (parseResult.getStatus()) {
      case SUCCESS:
        tryDateString = false;
        finalDate = parseResult.getPayload();
        break;
      case FAIL:
        // TODO: check requirements - seems we should return null here if we have null/illegal year
        if (!ymd.representsNull()) {
          result = new DateInterpretationResult(ymd.getIntegerYear(), ymd.getIntegerMonth(), null, null);
          break;
        }
    }

    // will be true when dateString is not null and the ymd parse ended in FAIL or ERROR
    if (tryDateString) {
      Date fromString = parseDateString(dateString);
      if (fromString != null) {
        finalDate = fromString;
      }
    }

    // finalDate will be null in the ymd FAIL case where tryDateString didn't work
    if (finalDate != null) {
      Calendar c = Calendar.getInstance();
      c.setTime(finalDate);

      if (c.get(Calendar.YEAR) >= EARLIEST_YEAR) {
        // months are 0 based
        result = new DateInterpretationResult(c.get(Calendar.YEAR), c.get(Calendar.MONTH) + 1, c.getTime(),
          sdf.format(c.getTime()));
      }
    }

    return result;
  }

  // TODO this is very naive string parsing - make it better
  private static Date parseDateString(String dateString) {
    Date result = null;
    String[] dateFormats =
      new String[] {"yyyy-MM-dd", "dd-MM-yyyy", "yyyy/MM/dd", "dd/MM/yyyy", "dd.MM.yyyy", "yyyy.MM.dd"};

    for (String dateFormat : dateFormats) {
      try {
        SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
        // lenient sdf will happily match 24-12-2001 to yyyy-MM-dd with completely wrong results
        sdf.setLenient(false);
        result = sdf.parse(dateString);
        break;
      } catch (ParseException e) {
        LOG.debug("Failed to parse dateString [{}] with pattern [{}], trying other patterns.", dateString, dateFormat);
      }
    }

    if (result == null) LOG.debug("Failed to parse dateString [{}] - giving up.", dateString);
    return result;
  }
}
