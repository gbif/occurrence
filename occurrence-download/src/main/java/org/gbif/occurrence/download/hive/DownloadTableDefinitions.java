package org.gbif.occurrence.download.hive;

import org.gbif.dwc.terms.GbifTerm;
import org.gbif.occurrence.common.TermUtils;

import java.util.List;

import com.google.common.collect.ImmutableList;

/**
 * TODO: this documentation is wrong
 * This provides the definitions required to construct the hdfs tables for querying during download.
 * <p/>
 * This class provides table definitions for full and simple downloads.  Simple is a subset of the full download and
 * exists only to help improve performance by reducing the amount of data scanned.
 */
public class DownloadTableDefinitions {

  private static final String JOIN_ARRAY_FMT = "if(%1$s IS NULL,'',join_array(%1$s,'\\;')) AS %1$s";

  /**
   * Generates the definition for the occurrence tables when used in hive.
   *
   * @return a list of fields for use in the <code>CREATE TABLE simple AS SELECT _fields_ FROM occurrence_HDFS</code>
   */
  public static List<String> fullDownload() {
    // start with the complete table definition and trim it to those we are interested in
    ImmutableList.Builder<String> builder = ImmutableList.builder();
    for (InitializableField field : OccurrenceHDFSTableDefinition.definition()) {
        builder.add(downloadableField(field));
    }
    return builder.build();
  }

  /**
   * Transforms a field into UDF expression that makes field suitable for download.
   */
  private static String downloadableField(InitializableField field){
    if (TermUtils.isInterpretedDate(field.getTerm())) {
      return "toISO8601(" + field.getHiveField() + ") AS " + field.getHiveField();
    } else if (TermUtils.isInterpretedNumerical(field.getTerm()) || TermUtils.isInterpretedDouble(field.getTerm())) {
      return "cleanNull(" + field.getHiveField() + ") AS " + field.getHiveField();
    } else if (TermUtils.isInterpretedBoolean(field.getTerm())) {
      return field.getHiveField();
    } else if (GbifTerm.issue == field.getTerm()) {
      // OccurrenceIssues are exposed as an String separate by ;
      return String.format(JOIN_ARRAY_FMT, field.getHiveField());
    } else if (field.getTerm() == GbifTerm.mediaType) {
      // OccurrenceIssues are exposed as an String separate by ;
      return String.format(JOIN_ARRAY_FMT, field.getHiveField());
    } else if (!TermUtils.isComplexType(field.getTerm())) {
      // complex type fields are not added to the select statement
      return "cleanDelimiters(" + field.getHiveField() + ") AS " + field.getHiveField();
    }
    return field.getHiveField();
  }


  /**
   * Generates the conceptual definition for the occurrence tables when used in hive.
   *
   * @return a list of fields for use in the <code>CREATE TABLE simple AS SELECT _fields_ FROM occurrence_HDFS</code>
   */
  public static List<String> simpleDownload() {
    // start with the complete table definition and trim it to those we are interested in
    // selecting only the verbatim and interpreted fields of interest
    ImmutableList.Builder<String> builder = ImmutableList.builder();
    for (InitializableField field : OccurrenceHDFSTableDefinition.definition()) {
      // allow any in the interpreted or verbatim table definitions
      if (DownloadTerms.SimpleDownload.SIMPLE_DOWNLOAD_TERMS.contains(field.getTerm())) {
        builder.add(field.getHiveField());
      }

    }

    return builder.build();
  }
}
