package org.gbif.occurrence.download.file;

import org.apache.commons.lang3.tuple.Pair;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;
import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.common.json.MediaSerDeserUtils;
import org.gbif.occurrence.download.hive.DownloadTerms;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
import org.gbif.occurrence.persistence.hbase.Columns;
import org.gbif.occurrence.persistence.hbase.ExtResultReader;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.beust.jcommander.internal.Sets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import static org.gbif.occurrence.common.download.DownloadUtils.DELIMETERS_MATCH_PATTERN;

/**
 * Reads a occurrence record from HBase and return it in a Map<String,Object>.
 */
public class OccurrenceMapReader {

  private static final Joiner SEMICOLON_JOINER = Joiner.on(';').skipNulls();
  private final String occurrenceTableName;
  private final Connection connection;

  /**
   * Utility to build an API Occurrence record as a Map<String,Object> from an HBase row.
   *
   * @return A complete occurrence, or null
   */
  public static Map<String, String> buildInterpretedOccurrenceMap(@Nullable Result row) {
    if (row == null || row.isEmpty()) {
      return null;
    } else {
      Map<String, String> occurrence = new HashMap<>();
      for (Term term : TermUtils.interpretedTerms()) {
        if (TermUtils.isInterpretedDate(term)) {
          occurrence.put(term.simpleName(), toISO8601Date(ExtResultReader.getDate(row, term)));
        } else if (TermUtils.isInterpretedDouble(term)) {
          Double value = ExtResultReader.getDouble(row, term);
          occurrence.put(term.simpleName(), value != null ? value.toString() : null);
        } else if (TermUtils.isInterpretedNumerical(term)) {
          Integer value = ExtResultReader.getInteger(row, term);
          occurrence.put(term.simpleName(), value != null ? value.toString() : null);
        } else if (term == GbifTerm.issue) {
          occurrence.put(GbifTerm.issue.simpleName(), extractOccurrenceIssues(row));
        } else if (term == GbifTerm.mediaType) {
          occurrence.put(GbifTerm.mediaType.simpleName(), extractMediaTypes(row));
        } else if (!TermUtils.isComplexType(term)) {
          occurrence.put(term.simpleName(), getCleanString(row, term));
        }
      }
      occurrence.put(GbifTerm.hasGeospatialIssues.simpleName(), Boolean.toString(hasGeospatialIssues(row)));
      occurrence.put(GbifTerm.hasCoordinate.simpleName(),
                     Boolean.toString(occurrence.get(DwcTerm.decimalLatitude.simpleName()) != null
                                      && occurrence.get(DwcTerm.decimalLongitude.simpleName()) != null));
      occurrence.put(GbifTerm.repatriated.simpleName(), getRepatriated(row).orElse(null));
      return occurrence;
    }
  }

  /**
   * Utility to build an API Occurrence record as a Map<String,Object> from an HBase row.
   *
   * @return A complete occurrence, or null
   */
  public static Map<String, String> buildOccurrenceMap(@Nullable Result row, Collection<Pair<DownloadTerms.Group, Term>> terms) {
    if (row == null || row.isEmpty()) {
      return null;
    } else {
      Map<String, String> occurrence = new HashMap<>();
      for (Pair<DownloadTerms.Group, Term> termPair : terms) {
        Term term = termPair.getRight();
        String simpleName = DownloadTerms.simpleName(termPair);
        if (termPair.getLeft().equals(DownloadTerms.Group.VERBATIM)) {
          // In the CSV, the verbatim field should be prefixed "verbatim".
          occurrence.put(simpleName, getCleanVerbatimString(row, term));
        } else if (TermUtils.isInterpretedDate(term)) {
          occurrence.put(simpleName, toISO8601Date(ExtResultReader.getDate(row, term)));
        } else if (TermUtils.isInterpretedDouble(term)) {
          Double value = ExtResultReader.getDouble(row, term);
          occurrence.put(simpleName, value != null ? value.toString() : null);
        } else if (TermUtils.isInterpretedNumerical(term)) {
          Integer value = ExtResultReader.getInteger(row, term);
          occurrence.put(simpleName, value != null ? value.toString() : null);
        } else if (term == GbifTerm.issue) {
          occurrence.put(simpleName, extractOccurrenceIssues(row));
        } else if (term == GbifTerm.mediaType) {
          occurrence.put(simpleName, extractMediaTypes(row));
        } else if (term == GbifTerm.hasGeospatialIssues) {
          occurrence.put(simpleName, Boolean.toString(hasGeospatialIssues(row)));
        } else if (term == GbifTerm.hasCoordinate) {
          occurrence.put(simpleName,
                         Boolean.toString(occurrence.get(DwcTerm.decimalLatitude.simpleName()) != null
                                          && occurrence.get(DwcTerm.decimalLongitude.simpleName()) != null));
        } else if (term == GbifTerm.repatriated) {
          occurrence.put(simpleName, getRepatriated(row).orElse(null));
        } else if (!TermUtils.isComplexType(term)) {
          occurrence.put(simpleName, getCleanString(row, term));
        }
      }
      return occurrence;
    }
  }

  /**
   * Validates if the occurrence record it's a repatriated record.
   */
  private static Optional<String> getRepatriated(Result result) {
    String publishingCountry = ExtResultReader.getString(result,Columns.column(GbifTerm.publishingCountry));
    String countryCode = ExtResultReader.getString(result,Columns.column(DwcTerm.countryCode));

    if (publishingCountry != null && countryCode != null) {
      return Optional.of(Boolean.toString(!publishingCountry.equalsIgnoreCase(countryCode)));
    }
    return Optional.empty();
  }

  /**
   * Extracts the media types from the hbase result.
   */
  private static String extractMediaTypes(Result result) {
    Optional<byte[]> val = Optional.ofNullable(result.getValue(Columns.CF,
                                                               Bytes.toBytes(Columns.column(Extension.MULTIMEDIA))));
    return val.map( v -> SEMICOLON_JOINER.join(MediaSerDeserUtils.extractMediaTypes(v))).orElse("");
  }

  /**
   * Extracts the spatial issues from the hbase result.
   */
  private static String extractOccurrenceIssues(Result result) {
    Set<String> issues = Sets.newHashSet();
    for (OccurrenceIssue issue : OccurrenceIssue.values()) {
      byte[] val = result.getValue(Columns.CF, Bytes.toBytes(Columns.column(issue)));
      if (val != null) {
        issues.add(issue.name());
      }
    }

    return SEMICOLON_JOINER.join(issues);
  }

  /**
   * Extracts the spatial issues from the HBase result.
   */
  private static Boolean hasGeospatialIssues(Result result) {
    for (OccurrenceIssue issue : OccurrenceIssue.GEOSPATIAL_RULES) {
      String column = Columns.column(issue);
      byte[] val = result.getValue(Columns.CF, Bytes.toBytes(column));
      if (val != null) {
        return true;
      }
    }
    return false;
  }

  /**
   * Utility to build an API Occurrence from an HBase row.
   *
   * @return A complete occurrence, or null
   */
  public static Map<String, String> buildVerbatimOccurrenceMap(@Nullable Result row) {
    if (row == null || row.isEmpty()) {
      return null;
    }
    Map<String, String> occurrence = new HashMap<>();
    for (Term term : TermUtils.verbatimTerms()) {
      occurrence.put(term.simpleName(), getCleanVerbatimString(row, term));
    }
    return occurrence;
  }

  /**
   * Cleans specials characters from a string value.
   * Removes tabs, line breaks and new lines.
   */
  private static String getCleanString(Result row, Term term) {
    return cleanString(ExtResultReader.getString(row, term));
  }

  /**
   * Cleans specials characters from a string value.
   * Removes tabs, line breaks and new lines.
   */
  private static String getCleanVerbatimString(Result row, Term term) {
    return cleanString(ExtResultReader.getString(row, Columns.verbatimColumn(term)));
  }

  private static String cleanString(String value) {
    return Optional.ofNullable(value).map(v -> DELIMETERS_MATCH_PATTERN.matcher(v).replaceAll(" ")).orElse(value);
  }

  /**
   * Converts a date object into a String in IS0 8601 format.
   */
  private static String toISO8601Date(Date date) {
    return date != null ? DownloadUtils.ISO_8601_FORMAT.format(date.toInstant().atZone(ZoneOffset.UTC)) : null;
  }

  @Inject
  public OccurrenceMapReader(@Named(DownloadWorkflowModule.DefaultSettings.OCC_HBASE_TABLE_KEY) String tableName,
                             Connection connection) {
    occurrenceTableName = tableName;
    this.connection = connection;
  }

  /**
   * Reads an occurrence record from HBase into Map.
   * The occurrence record
   */
  public Result get(@Nonnull Long key) throws IOException {
    Preconditions.checkNotNull(key, "Occurrence key can't be null");
    try (Table table = connection.getTable(TableName.valueOf(occurrenceTableName))) {
      return table.get(new Get(Bytes.toBytes(key)));
    }
  }

}
