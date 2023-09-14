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

import com.google.common.collect.Range;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.util.IsoDateInterval;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.date.MultiinputTemporalParser;
import org.gbif.common.parsers.date.TemporalAccessorUtils;
import org.gbif.common.parsers.date.TemporalRangeParser;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.temporal.ChronoField;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;

import static org.gbif.common.parsers.core.ParseResult.CONFIDENCE.DEFINITE;
import static org.gbif.common.parsers.date.DateComponentOrdering.DMY_FORMATS;

/**
 * Interprets date representations into a Date.
 *
 * Intended to be very close to the Pipelines one.
 */
public class TemporalInterpreter {

  private static final Logger LOG = LoggerFactory.getLogger(TemporalInterpreter.class);

  static final LocalDate MIN_LOCAL_DATE = LocalDate.of(1600, 1, 1);
  static final LocalDate MIN_EPOCH_LOCAL_DATE = LocalDate.ofEpochDay(0);

  private static final MultiinputTemporalParser TEMPORAL_PARSER = MultiinputTemporalParser.create(Arrays.asList(DMY_FORMATS)).create();
  private static final TemporalRangeParser TEMPORAL_RANGE_PARSER = TemporalRangeParser.builder().temporalParser(TEMPORAL_PARSER).create();

  private TemporalInterpreter() {
  }

  public static void interpretTemporal(VerbatimOccurrence verbatim, Occurrence occ) {
    OccurrenceParseResult<IsoDateInterval> eventResult = interpretRecordedDate(verbatim);

    if (eventResult.isSuccessful()) {
      IsoDateInterval isoDateInterval = eventResult.getPayload();

      occ.setEventDate(isoDateInterval);
      if (isoDateInterval.getTo() != null) {
        // Set dwc:year, dwc:month, dwc:day
        if (isoDateInterval.getFrom().isSupported(ChronoField.YEAR) && isoDateInterval.getFrom().get(ChronoField.YEAR) == isoDateInterval.getTo().get(ChronoField.YEAR)) {
          occ.setYear(isoDateInterval.getFrom().get(ChronoField.YEAR));
          if (isoDateInterval.getFrom().isSupported(ChronoField.MONTH_OF_YEAR) && isoDateInterval.getFrom().get(ChronoField.MONTH_OF_YEAR) == isoDateInterval.getTo().get(ChronoField.MONTH_OF_YEAR)) {
            occ.setMonth(isoDateInterval.getFrom().get(ChronoField.MONTH_OF_YEAR));
            if (isoDateInterval.getFrom().isSupported(ChronoField.DAY_OF_MONTH) && isoDateInterval.getFrom().get(ChronoField.DAY_OF_MONTH) == isoDateInterval.getTo().get(ChronoField.DAY_OF_MONTH)) {
              occ.setDay(isoDateInterval.getFrom().get(ChronoField.DAY_OF_MONTH));
            }
          }
        }
        // Set dwc:startDayOfYear, dwc:endDayOfYear — except these aren't interpreted fields yet.
        //if (isoDateInterval.getFrom().isSupported(ChronoField.DAY_OF_YEAR)) {
        //  occ.setStartDayOfYear(isoDateInterval.getFrom().get(ChronoField.DAY_OF_YEAR));
        //}
        //if (isoDateInterval.getTo().isSupported(ChronoField.DAY_OF_YEAR)) {
        //  occ.setEndDayOfYear(isoDateInterval.getTo().get(ChronoField.DAY_OF_YEAR));
        //}
      }
    }
    occ.getIssues().addAll(eventResult.getIssues());

    LocalDate upperBound = LocalDate.now().plusDays(1);
    if (verbatim.hasVerbatimField(DcTerm.modified)) {
      Range<LocalDate> validModifiedDateRange = Range.closed(MIN_EPOCH_LOCAL_DATE, upperBound);
      OccurrenceParseResult<TemporalAccessor> parsed = TEMPORAL_PARSER.parseLocalDate(verbatim.getVerbatimField(DcTerm.modified), validModifiedDateRange, OccurrenceIssue.MODIFIED_DATE_UNLIKELY);
      if (parsed.isSuccessful()) {
        occ.setModified(TemporalAccessorUtils.toDate(parsed.getPayload()));
      }
      occ.getIssues().addAll(parsed.getIssues());
    }

    if (verbatim.hasVerbatimField(DwcTerm.dateIdentified)) {
      Range<LocalDate> validRecordedDateRange = Range.closed(MIN_LOCAL_DATE, upperBound);
      OccurrenceParseResult<TemporalAccessor> parsed = TEMPORAL_PARSER.parseLocalDate(verbatim.getVerbatimField(DwcTerm.dateIdentified), validRecordedDateRange, OccurrenceIssue.IDENTIFIED_DATE_UNLIKELY);
      if (parsed.isSuccessful()) {
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
  public static OccurrenceParseResult<IsoDateInterval> interpretRecordedDate(VerbatimOccurrence verbatim) {
    final String year = verbatim.getVerbatimField(DwcTerm.year);
    final String month = verbatim.getVerbatimField(DwcTerm.month);
    final String day = verbatim.getVerbatimField(DwcTerm.day);
    final String dateString = verbatim.getVerbatimField(DwcTerm.eventDate);
    final String startDayOfYear = verbatim.getVerbatimField(DwcTerm.startDayOfYear);
    final String endDayOfYear = verbatim.getVerbatimField(DwcTerm.endDayOfYear);

    return interpretRecordedDate(year, month, day, dateString, startDayOfYear, endDayOfYear);
  }

  /**
   * Given possibly both of year, month, day, a dateString, and start/endDayOfYear, produces a single date.
   * When dates conflict, the most that agrees is returned, except if year+month+day falls within a range given
   * by dateString.
   *
   * Years are verified to be before or next year and after 1600.
   *
   * @return interpretation result, never null
   */
  public static OccurrenceParseResult<IsoDateInterval> interpretRecordedDate(String year, String month, String day,
    String dateString, String startDayOfYear, String endDayOfYear) {

    OccurrenceParseResult<IsoDateInterval> eventRange =
      TEMPORAL_RANGE_PARSER.parse(year, month, day, dateString, startDayOfYear, endDayOfYear);

    Optional<TemporalAccessor> fromTa = Optional.ofNullable(eventRange.getPayload()).map(IsoDateInterval::getFrom);
    Optional<TemporalAccessor> toTa = Optional.ofNullable(eventRange.getPayload()).map(IsoDateInterval::getTo);
    Set<OccurrenceIssue> issues = eventRange.getIssues();

    if (fromTa.isPresent()) {
      IsoDateInterval di = toTa.isPresent() ?
        new IsoDateInterval((Temporal) fromTa.get(), (Temporal) toTa.get()) :
        new IsoDateInterval((Temporal) fromTa.get());
      return OccurrenceParseResult.success(DEFINITE, di, issues);
    } else {
      return OccurrenceParseResult.fail(issues);
    }
  }
}
