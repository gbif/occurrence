package org.gbif.occurrence.search;

import org.gbif.api.util.IsoDateParsingUtils;
import org.gbif.api.util.IsoDateParsingUtils.IsoDateFormat;
import org.gbif.api.util.SearchTypeValidator;

import java.util.Date;

import com.google.common.collect.Range;

import static org.gbif.api.util.IsoDateParsingUtils.getFirstDateFormatMatch;
import static org.gbif.api.util.IsoDateParsingUtils.parseDateRange;
import static org.gbif.api.util.IsoDateParsingUtils.toLastDayOfMonth;
import static org.gbif.api.util.IsoDateParsingUtils.toLastDayOfYear;
import static org.gbif.common.search.util.QueryUtils.toDateQueryFormat;
import static org.gbif.common.search.util.QueryUtils.toPhraseQuery;
import static org.gbif.common.search.util.SolrConstants.DEFAULT_FILTER_QUERY;
import static org.gbif.common.search.util.SolrConstants.RANGE_FORMAT;

/**
 * Utility class that contains functions for building occurrence date queries.
 */
public class OccurrenceSearchDateUtils {

  /**
   * Private constructor.
   */
  private OccurrenceSearchDateUtils() {
    //utility classes must be have private constructors
  }

  /**
   * Converts the value parameter into a date range query.
   */
  public static String toDateQuery(String value) {
    return SearchTypeValidator.isRange(value) ? toRangeDateQueryFormat(value): toSingleDateQueryFormat(value);
  }

  private static String toRangeDateQueryFormat(String value) {
    final Range<Date> dateRange = parseDateRange(value);
    if (!dateRange.hasLowerBound() && !dateRange.hasUpperBound()) {
      return DEFAULT_FILTER_QUERY;
    } else if (dateRange.hasLowerBound() && !dateRange.hasUpperBound()) {
      return String.format(RANGE_FORMAT, toDateQueryFormat(dateRange.lowerEndpoint()), DEFAULT_FILTER_QUERY);
    } else if (!dateRange.hasLowerBound() && dateRange.hasUpperBound()) {
      return String.format(RANGE_FORMAT, DEFAULT_FILTER_QUERY, toDateQueryFormat(dateRange.upperEndpoint()));
    } else {
      return String.format(RANGE_FORMAT, toDateQueryFormat(dateRange.lowerEndpoint()),
                           toDateQueryFormat(dateRange.upperEndpoint()));
    }
  }

  /**
   * Transforms a single ISO date value into a solr query being a range query depending on the precision of the
   * ISO date. For example a single date 1989-09 results in a solr range for the entire month.
   */
  private static String toSingleDateQueryFormat(String value) {
    final IsoDateFormat occDateFormat = getFirstDateFormatMatch(value);
    final Date lowerDate = IsoDateParsingUtils.parseDate(value);
    final String lowerDateStr = toDateQueryFormat(lowerDate);
    if (occDateFormat == IsoDateFormat.YEAR_MONTH) {
      // Generated query: [yyyy-MM-01T00:00:00Z TO yyyy-'MM-LAST_DATE_OF_THE_MONTH'T00:00:00Z]
      return String.format(RANGE_FORMAT, lowerDateStr, toDateQueryFormat(toLastDayOfMonth(lowerDate)));
    } else if (occDateFormat == IsoDateFormat.YEAR) {
      // Generated query: [yyyy-01-01T00:00:00Z TO yyyy-'LAST_DATE_OF_THE_YEAR'T00:00:00Z]
      return String.format(RANGE_FORMAT, lowerDateStr, toDateQueryFormat(toLastDayOfYear(lowerDate)));
    } else {
      return toPhraseQuery(lowerDateStr);
    }
  }
}
