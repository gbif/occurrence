package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.date.AtomizedLocalDate;
import org.gbif.common.parsers.date.DateParsers;
import org.gbif.common.parsers.date.TemporalAccessorUtils;
import org.gbif.common.parsers.date.TemporalParser;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;

import java.util.Date;
import java.util.EnumSet;
import java.util.Set;

import com.google.common.base.Strings;
import com.google.common.collect.Range;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.LocalDate;
import org.threeten.bp.temporal.ChronoField;
import org.threeten.bp.temporal.TemporalAccessor;
import org.threeten.bp.temporal.TemporalQueries;

/**
 * Interprets date representations into a Date.
 */
public class TemporalInterpreter {

  private static final Logger LOG = LoggerFactory.getLogger(TemporalInterpreter.class);

  static final LocalDate MIN_LOCAL_DATE = LocalDate.of(1600, 1, 1);
  static final LocalDate MIN_EPOCH_LOCAL_DATE = LocalDate.ofEpochDay(0);

  private static final TemporalParser TEXTDATE_PARSER = DateParsers.defaultTemporalParser();

  private TemporalInterpreter() {
  }

  public static void interpretTemporal(VerbatimOccurrence verbatim, Occurrence occ) {
    OccurrenceParseResult<TemporalAccessor> eventResult = interpretRecordedDate(verbatim);
    if (eventResult.isSuccessful()) {
      TemporalAccessor temporalAccessor = eventResult.getPayload();

      //Get eventDate as java.util.Date and ignore the offset (timezone) if provided
      //Note for debug: be careful if you inspect the content of 'eventDate' it will contain your machine timezone.
      Date eventDate = TemporalAccessorUtils.toDate(temporalAccessor, true);
      AtomizedLocalDate atomizedLocalDate = AtomizedLocalDate.fromTemporalAccessor(temporalAccessor);

      occ.setEventDate(eventDate);
      occ.setYear(atomizedLocalDate.getYear());
      occ.setMonth(atomizedLocalDate.getMonth());
      occ.setDay(atomizedLocalDate.getDay());
    }
    occ.getIssues().addAll(eventResult.getIssues());

    LocalDate upperBound = LocalDate.now().plusDays(1);
    if (verbatim.hasVerbatimField(DcTerm.modified)) {
      Range<LocalDate> validModifiedDateRange = Range.closed(MIN_EPOCH_LOCAL_DATE, upperBound);
      OccurrenceParseResult<TemporalAccessor> parsed = interpretLocalDate(verbatim.getVerbatimField(DcTerm.modified),
              validModifiedDateRange, OccurrenceIssue.MODIFIED_DATE_UNLIKELY);
      occ.setModified(TemporalAccessorUtils.toDate(parsed.getPayload()));
      occ.getIssues().addAll(parsed.getIssues());
    }

    if (verbatim.hasVerbatimField(DwcTerm.dateIdentified)) {
      Range<LocalDate> validRecordedDateRange = Range.closed(MIN_LOCAL_DATE, upperBound);
      OccurrenceParseResult<TemporalAccessor> parsed = interpretLocalDate(verbatim.getVerbatimField(DwcTerm.dateIdentified),
              validRecordedDateRange, OccurrenceIssue.IDENTIFIED_DATE_UNLIKELY);
      if(parsed.isSuccessful()) {
        occ.setDateIdentified(TemporalAccessorUtils.toDate(parsed.getPayload()));
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

      LOG.debug("Date mismatch: [{} vs {}].", parsedYMDResult.getPayload(), parsedDateResult.getPayload());

      TemporalAccessor bestResolution =
              TemporalAccessorUtils.getBestResolutionTemporalAccessor(parsedYMDResult.getPayload(), parsedDateResult.getPayload());
      if(bestResolution != null){
        parsedTemporalAccessor = bestResolution;
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
      if(parsedTemporalAccessor == null) {
        issues.add(OccurrenceIssue.RECORDED_DATE_INVALID);
      }
      else{
        issues.add(OccurrenceIssue.RECORDED_DATE_UNLIKELY);
      }

      LOG.debug("Invalid date: [{}]].", parsedTemporalAccessor);
      return OccurrenceParseResult.fail(issues);
    }

    return OccurrenceParseResult.success(confidence, parsedTemporalAccessor, issues);
  }

  /**
   * Check if a date express as TemporalAccessor falls between the predefined range.
   * Lower bound defined by {@link #MIN_LOCAL_DATE} and upper bound by current date + 1 day
   * @param temporalAccessor
   * @param acceptPartialDate
   * @return valid or not according to the predefined range.
   */
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

}
