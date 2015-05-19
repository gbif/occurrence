package org.gbif.occurrence.download.file;

import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;
import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.common.json.MediaSerDeserUtils;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
import org.gbif.occurrence.persistence.hbase.Columns;
import org.gbif.occurrence.persistence.hbase.ExtResultReader;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.beust.jcommander.internal.Sets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Reads a occurrence record from HBase and return it in a Map<String,Object>.
 */
public class OccurrenceMapReader {

  private final String occurrenceTableName;
  private final HTablePool tablePool;
  private static final Joiner SEMICOLON_JOINER = Joiner.on(';').skipNulls();

  @Inject
  public OccurrenceMapReader(@Named(DownloadWorkflowModule.DefaultSettings.OCC_HBASE_TABLE_KEY) String occurrenceTableName, HTablePool tablePool) {
    this.occurrenceTableName = occurrenceTableName;
    this.tablePool = tablePool;
  }


  /**
   * Utility to build an API Occurrence record as a Map<String,Object> from an HBase row.
   *
   * @return A complete occurrence, or null
   */
  public static Map<String, String> buildInterpretedOccurrenceMap(@Nullable Result row) {
    if (row == null || row.isEmpty()) {
      return null;
    } else {
      Map<String, String> occurrence = new HashMap<String, String>();
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
      occurrence.put(
        GbifTerm.hasCoordinate.simpleName(),
        Boolean.toString(occurrence.get(DwcTerm.decimalLatitude.simpleName()) != null
          && occurrence.get(DwcTerm.decimalLongitude.simpleName()) != null));
      return occurrence;
    }
  }


  /**
   * Utility to build an API Occurrence record as a Map<String,Object> from an HBase row.
   *
   * @return A complete occurrence, or null
   */
  public static Map<String, String> buildOccurrenceMap(@Nullable Result row, Collection<Term> terms) {
    if (row == null || row.isEmpty()) {
      return null;
    } else {
      Map<String, String> occurrence = new HashMap<String, String>();
      for (Term term : terms) {
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
        } else if (term == GbifTerm.hasGeospatialIssues) {
          occurrence.put(GbifTerm.hasGeospatialIssues.simpleName(), Boolean.toString(hasGeospatialIssues(row)));
        } else if (term == GbifTerm.hasCoordinate) {
          occurrence.put(
            GbifTerm.hasCoordinate.simpleName(),
            Boolean.toString(occurrence.get(DwcTerm.decimalLatitude.simpleName()) != null
                             && occurrence.get(DwcTerm.decimalLongitude.simpleName()) != null));
        } else if (!TermUtils.isComplexType(term)) {
          occurrence.put(term.simpleName(), getCleanString(row, term));
        }
      }
      return occurrence;
    }
  }

  /**
   * Extracts the media types from the hbase result.
   */
  private static String extractMediaTypes(Result result) {
    byte[] val = result.getValue(Columns.CF, Bytes.toBytes(Columns.column(Extension.MULTIMEDIA)));
    return (val != null) ? SEMICOLON_JOINER.join(MediaSerDeserUtils.extractMediaTypes(val)) : "";
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
    Map<String, String> occurrence = new HashMap<String, String>();
    for (Term term : TermUtils.verbatimTerms()) {
      occurrence.put(term.simpleName(), getCleanVerbatimString(row, term));
    }
    return occurrence;
  }

  /**
   * Cleans specials characters from a string value.
   * Removes tabs, line breaks and new lines.
   */
  public static String getCleanString(Result row, Term term) {
    String value = ExtResultReader.getString(row, term);
    return value != null ? value.replaceAll(DownloadUtils.DELIMETERS_MATCH, " ") : value;
  }

  /**
   * Cleans specials characters from a string value.
   * Removes tabs, line breaks and new lines.
   */
  public static String getCleanVerbatimString(Result row, Term term) {
    String value = ExtResultReader.getString(row, Columns.verbatimColumn(term));
    return value != null ? value.replaceAll(DownloadUtils.DELIMETERS_MATCH, " ") : value;
  }

  /**
   * Converts a date object into a String in IS0 8601 format.
   */
  public static String toISO8601Date(Date date) {
    return date != null ? new SimpleDateFormat(DownloadUtils.ISO_8601_FORMAT).format(date) : null;
  }


  /**
   * Reads an occurrence record from HBase into Map.
   * The occurrence record
   */
  public Result get(@Nonnull Integer key) throws IOException {
    Preconditions.checkNotNull(key, "Occurrence key can't be null");
    try (HTableInterface table = tablePool.getTable(occurrenceTableName) ) {
      return table.get(new Get(Bytes.toBytes(key)));
    }
  }

}
