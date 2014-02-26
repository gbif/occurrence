package org.gbif.occurrence.download.file;

import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;
import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.persistence.hbase.Columns;
import org.gbif.occurrence.persistence.hbase.ExtResultReader;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
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

  @Inject
  public OccurrenceMapReader(@Named("occurrence_table_name") String occurrenceTableName, HTablePool tablePool) {
    this.occurrenceTableName = occurrenceTableName;
    this.tablePool = tablePool;
  }


  /**
   * Utility to build an API Occurrence record as a Map<String,Object> from an HBase row.
   * 
   * @return A complete occurrence, or null
   */
  public static Map<String, String> buildOccurrenceMap(@Nullable Result row) {
    if (row == null || row.isEmpty()) {
      return null;
    } else {
      Map<String, String> occurrence = new HashMap<String, String>();
      for (Term term : TermUtils.interpretedTerms()) {
        final String hiveColumn = TermUtils.getHiveColumn(term);
        if (TermUtils.isInterpretedDate(term)) {
          occurrence.put(hiveColumn, toISO8601Date(ExtResultReader.getDate(row, term)));
        } else if (TermUtils.isInterpretedDouble(term)) {
          Double value = ExtResultReader.getDouble(row, term);
          occurrence.put(hiveColumn, value != null ? value.toString() : null);
        } else if (TermUtils.isInterpretedNumerical(term)) {
          Integer value = ExtResultReader.getInteger(row, term);
          occurrence.put(hiveColumn, value != null ? value.toString() : null);
        } else {
          occurrence.put(hiveColumn, getCleanString(row, term));
        }
      }
      return occurrence;
    }
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
      occurrence.put(TermUtils.getHiveColumn(term), getCleanVerbatimString(row, term));
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
    if (date != null) {
      return new SimpleDateFormat(DownloadUtils.ISO_8601_FORMAT).format(date);
    }
    return null;
  }


  /**
   * Reads an occurrence record from HBase into Map.
   * The occurrence record
   */
  public Result get(@Nonnull Integer key) throws IOException {
    Preconditions.checkNotNull(key, "Ocurrence key can't be null");
    HTableInterface table = null;
    Closer closer = Closer.create();
    try {
      table = tablePool.getTable(occurrenceTableName);
      closer.register(table);
      Get get = new Get(Bytes.toBytes(key));
      return table.get(get);
    } catch (IOException e) {
      throw new ServiceUnavailableException("Could not read from HBase", e);
    } finally {
      closer.close();
    }
  }

}
