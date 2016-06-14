package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.date.AtomizedLocalDate;
import org.gbif.common.parsers.date.TextDateParser;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;

import java.util.Date;
import java.util.EnumSet;
import java.util.Set;
import javax.annotation.Nullable;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.Range;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.DateTimeUtils;
import org.threeten.bp.LocalDate;
import org.threeten.bp.LocalDateTime;
import org.threeten.bp.Year;
import org.threeten.bp.YearMonth;
import org.threeten.bp.ZoneId;
import org.threeten.bp.ZonedDateTime;
import org.threeten.bp.temporal.ChronoField;
import org.threeten.bp.temporal.TemporalAccessor;
import org.threeten.bp.temporal.TemporalQueries;

/**
 * Interprets date representations into a Date.
 */
public class TemporalInterpreter {

  private static final Logger LOG = LoggerFactory.getLogger(TemporalInterpreter.class);

  static ZoneId UTC_ZONE_ID = ZoneId.of("UTC");

  static final LocalDate MIN_LOCAL_DATE = LocalDate.of(1600, 1, 1);
  static final LocalDate MIN_EPOCH_LOCAL_DATE = LocalDate.ofEpochDay(0);

  private static final TextDateParser TEXTDATE_PARSER = new TextDateParser();

  private TemporalInterpreter() {
  }

  public static void interpretTemporal(VerbatimOccurrence verbatim, Occurrence occ) {
    OccurrenceParseResult<TemporalAccessor> eventResult = interpretRecordedDate(verbatim);
    if (eventResult.isSuccessful()) {
      TemporalAccessor temporalAccessor = eventResult.getPayload();
      LocalDate localDate = temporalAccessor.query(TemporalQueries.localDate());

      // Try from partial date
      if(localDate == null) {
        YearMonth yearMonth = temporalAccessor.query(YearMonth.FROM);
        if (yearMonth != null) {
          localDate = yearMonth.atDay(1);
        }
      }

      if(localDate == null) {
        Year year = temporalAccessor.query(Year.FROM);
        if (year != null) {
          localDate = year.atDay(1);
        }
      }

      //Get eventDate as java.util.Date in UTC
      Date eventDate = DateTimeUtils.toDate(localDate.atStartOfDay(UTC_ZONE_ID).toInstant());

      occ.setEventDate(eventDate);
      occ.setYear(localDate.getYear());
      occ.setMonth(localDate.getMonthValue());
      occ.setDay(localDate.getDayOfMonth());
    }
    occ.getIssues().addAll(eventResult.getIssues());

    LocalDate upperBound = LocalDate.now().plusDays(1);
    if (verbatim.hasVerbatimField(DcTerm.modified)) {
      Range<LocalDate> validModifiedDateRange = Range.closed(MIN_EPOCH_LOCAL_DATE, upperBound);
      OccurrenceParseResult<TemporalAccessor> parsed = interpretLocalDate(verbatim.getVerbatimField(DcTerm.modified),
              validModifiedDateRange, OccurrenceIssue.MODIFIED_DATE_UNLIKELY);
      occ.setModified(toUTCDate(parsed.getPayload()));
      occ.getIssues().addAll(parsed.getIssues());
    }

    if (verbatim.hasVerbatimField(DwcTerm.dateIdentified)) {
      Range<LocalDate> validRecordedDateRange = Range.closed(MIN_LOCAL_DATE, upperBound);
      OccurrenceParseResult<TemporalAccessor> parsed = interpretLocalDate(verbatim.getVerbatimField(DwcTerm.dateIdentified),
              validRecordedDateRange, OccurrenceIssue.IDENTIFIED_DATE_UNLIKELY);
      if(parsed.isSuccessful()) {
        occ.setDateIdentified(toUTCDate(parsed.getPayload()));
      }
      occ.getIssues().addAll(parsed.getIssues());
    }
  }

  /**
   * A convenience method that calls interpretRecordedDate with the verbatim recordedDate values from the
   * VerbatimOccurrence.
   *
   * @param verbatim the VerbatimOccurrence containing a recordedDate
   * @return the interpretation result which is never null
   */
  public static OccurrenceParseResult<TemporalAccessor> interpretRecordedDate(VerbatimOccurrence verbatim) {
    final String year = verbatim.getVerbatimField(DwcTerm.year);
    final String month = verbatim.getVerbatimField(DwcTerm.month);
    final String day = verbatim.getVerbatimField(DwcTerm.day);
    final String dateString = verbatim.getVerbatimField(DwcTerm.eventDate);

    return interpretRecordedDate(year, month, day, dateString);
  }

  public static OccurrenceParseResult<AtomizedLocalDate> interpretEventDate(String year, String month, String day,
                                                                              String dateString) {
    OccurrenceParseResult<TemporalAccessor> ta = interpretRecordedDate(year,  month,  day, dateString);
    return new OccurrenceParseResult<AtomizedLocalDate>(ta.getStatus(), ta.getConfidence(),
            AtomizedLocalDate.fromTemporalAccessor(ta.getPayload()), ta.getError());
  }

  /**
   * Given possibly both of year, month, day and a dateString, produces a single date.
   * When year, month and day are all populated and parseable they are given priority,
   * but if any field is missing or illegal and dateString is parseable dateString is preferred.
   * Partially valid dates are not supported and null will be returned instead. The only exception is the year alone
   * which will be used as the last resort if nothing else works.
   * Years are verified to be before or next year and after 1600.
   *x
   * @return interpretation result, never null
   */
  public static OccurrenceParseResult<TemporalAccessor> interpretRecordedDate(String year, String month, String day,
    String dateString) {

    boolean atomizedDateProvided = StringUtils.isNotBlank(year) || StringUtils.isNotBlank(month)
            || StringUtils.isNotBlank(day);
    boolean dateStringProvided = StringUtils.isNotBlank(dateString);

    if (!atomizedDateProvided && !dateStringProvided) {
      return OccurrenceParseResult.fail();
    }

    Set<OccurrenceIssue> issues = EnumSet.noneOf(OccurrenceIssue.class);

    /**
     * First, attempt year, month, day parsing
     * If the parse result is SUCCESS it means that a whole date could be extracted (with year,
     * month and day). If it is a failure but the normalizer returned a meaningful result (e.g. it could extract just
     * a year) we're going to return a result with all the fields set that we could parse.
     */

    TemporalAccessor parsedTemporalAccessor = null;
    ParseResult.CONFIDENCE confidence = null;

    ParseResult<TemporalAccessor> parsedYMDResult = atomizedDateProvided ? TEXTDATE_PARSER.parse(year, month, day) :
            ParseResult.<TemporalAccessor>fail();
    ParseResult<TemporalAccessor> parsedDateResult = dateStringProvided ? TEXTDATE_PARSER.parse(dateString) :
            ParseResult.<TemporalAccessor>fail();

    // If both inputs exist verify that they match
    if(atomizedDateProvided && dateStringProvided &&
            !ObjectUtils.equals(parsedYMDResult.getPayload(), parsedDateResult.getPayload())){

      issues.add(OccurrenceIssue.RECORDED_DATE_MISMATCH);

      LOG.debug("Date mismatch: [{} vs {}]].", parsedYMDResult.getPayload(), parsedDateResult.getPayload());

      Optional<? extends TemporalAccessor> bestResolution = getBestResolutionTemporalAccessor(parsedYMDResult.getPayload(), parsedDateResult.getPayload());
      if(bestResolution.isPresent()){
        parsedTemporalAccessor = bestResolution.get();
        // if one of the 2 result is null we can not set the confidence to DEFINITE
        confidence = (parsedYMDResult.getPayload() == null || parsedDateResult.getPayload() == null) ?
                ParseResult.CONFIDENCE.PROBABLE :ParseResult.CONFIDENCE.DEFINITE;
      }
      else{
        return OccurrenceParseResult.fail(issues);
      }
    }
    else{
      parsedTemporalAccessor = parsedYMDResult.getPayload() != null ? parsedYMDResult.getPayload() :
              parsedDateResult.getPayload();
      confidence = parsedYMDResult.getPayload() != null ? parsedYMDResult.getConfidence() :
              parsedDateResult.getConfidence();
    }

    if(!isValidDate(parsedTemporalAccessor, true)){
      issues.add(OccurrenceIssue.RECORDED_DATE_UNLIKELY);
      LOG.debug("Invalid date: [{}]].", parsedTemporalAccessor);
      return OccurrenceParseResult.fail(issues);
    }

    return OccurrenceParseResult.success(confidence, parsedTemporalAccessor, issues);
  }

  public static boolean isValidDate(TemporalAccessor temporalAccessor, boolean acceptPartialDate){
    LocalDate upperBound = LocalDate.now().plusDays(1);
    return isValidDate(temporalAccessor, acceptPartialDate, Range.closed(MIN_LOCAL_DATE, upperBound));
  }

  /**
   * Check if a date express as TemporalAccessor falls between the provided range.
   *
   * @param temporalAccessor
   * @return
   */
  public static boolean isValidDate(TemporalAccessor temporalAccessor, boolean acceptPartialDate,
                                    Range<LocalDate> likelyRange){

    if(temporalAccessor == null){
      return false;
    }

    if(!acceptPartialDate){
      LocalDate localDate = temporalAccessor.query(TemporalQueries.localDate());
      if(localDate == null){
        return false;
      }
      return likelyRange.contains(localDate);
    }

    //if partial dates should be considered valid
    int year, month = 1, day = 1;
    if(temporalAccessor.isSupported(ChronoField.YEAR)){
      year = temporalAccessor.get(ChronoField.YEAR);
    }
    else{
      return false;
    }

    if(temporalAccessor.isSupported(ChronoField.MONTH_OF_YEAR)){
      month = temporalAccessor.get(ChronoField.MONTH_OF_YEAR);
    }

    if(temporalAccessor.isSupported(ChronoField.DAY_OF_MONTH)){
      day = temporalAccessor.get(ChronoField.DAY_OF_MONTH);
    }

    return likelyRange.contains(LocalDate.of(year, month, day));
  }

  /**
   *
   * @param dateString
   * @param likelyRange
   * @param unlikelyIssue
   * @return TemporalAccessor that represents a LocalDate or LocalDateTime
   */
  public static OccurrenceParseResult<TemporalAccessor> interpretLocalDate(String dateString, Range<LocalDate> likelyRange,
    OccurrenceIssue unlikelyIssue) {
    if (!Strings.isNullOrEmpty(dateString)) {
      OccurrenceParseResult<TemporalAccessor> result = new OccurrenceParseResult(TEXTDATE_PARSER.parse(dateString));
      // check year makes sense
      if (result.isSuccessful()) {
        if(!isValidDate(result.getPayload(), false, likelyRange)) {
          LOG.debug("Unlikely date parsed, ignore [{}].", dateString);
          result.addIssue(unlikelyIssue);
        }
      }
      return result;
    }
    return OccurrenceParseResult.fail();
  }

  /**
   * The idea of "best resolution" TemporalAccessor is to get the TemporalAccessor that offers more resolution than
   * the other but they must NOT contradict.
   * e.g. 2005-01 and 2005-01-01 will return 2005-01-01.
   *
   * Note that if one of the 2 parameters is null the other one will be considered having the best resolution
   *
   * @param ta1
   * @param ta2
   * @return
   */
  public static Optional<? extends TemporalAccessor> getBestResolutionTemporalAccessor(@Nullable TemporalAccessor ta1,
                                                                                       @Nullable TemporalAccessor ta2){
    //handle nulls combinations
    if(ta1 == null && ta2 == null){
      return Optional.absent();
    }
    if(ta1 == null){
      return Optional.of(ta2);
    }
    if(ta2 == null){
      return Optional.of(ta1);
    }

    AtomizedLocalDate ymd1 = AtomizedLocalDate.fromTemporalAccessor(ta1);
    AtomizedLocalDate ymd2 = AtomizedLocalDate.fromTemporalAccessor(ta2);

    // If they both provide the year, it must match
    if(ymd1.getYear() != null && ymd2.getYear() != null && !ymd1.getYear().equals(ymd2.getYear())){
      return Optional.absent();
    }
    // If they both provide the month, it must match
    if(ymd1.getMonth() != null && ymd2.getMonth() != null && !ymd1.getMonth().equals(ymd2.getMonth())){
      return Optional.absent();
    }
    // If they both provide the day, it must match
    if(ymd1.getDay() != null && ymd2.getDay() != null && !ymd1.getDay().equals(ymd2.getDay())){
      return Optional.absent();
    }

    if(ymd1.getResolution() > ymd2.getResolution()){
      return Optional.of(ta1);
    }

    return Optional.of(ta2);
  }

  /**
   * Transform a TemporalAccessor to a java.util.Date in the UTC time zone.
   *
   * @param temporalAccessor
   * @return
   */
  public static Date toUTCDate(TemporalAccessor temporalAccessor){
    if(temporalAccessor.isSupported(ChronoField.OFFSET_SECONDS)){
      return DateTimeUtils.toDate(temporalAccessor.query(ZonedDateTime.FROM).toInstant());
    }

    if(temporalAccessor.isSupported(ChronoField.SECOND_OF_DAY)){
      return DateTimeUtils.toDate(temporalAccessor.query(LocalDateTime.FROM).atZone(UTC_ZONE_ID).toInstant());
    }

    LocalDate localDate = temporalAccessor.query(LocalDate.FROM);
    if (localDate != null) {
      return DateTimeUtils.toDate(localDate.atStartOfDay(UTC_ZONE_ID).toInstant());
    }

    return null;
  }

}
