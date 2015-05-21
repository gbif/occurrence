package org.gbif.occurrence.download.hive;

import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;

import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;

/**
 * Utilities related to the actual queries executed at runtime.
 * The queries relate closely to the data definitions (obviously) and this class provides the bridge between the
 * definitions and the queries.
 */
class Queries {

  /**
   * @return the select fields for the verbatim table in the simple download
   */
  static List<InitializableField> selectVerbatimFields() {
    ImmutableList.Builder<InitializableField> builder = ImmutableList.builder();
    // always add the GBIF ID
    builder.add(new InitializableField(GbifTerm.gbifID,
                                       HiveColumns.columnFor(GbifTerm.gbifID),
                                       HiveDataTypes.typeForTerm(GbifTerm.gbifID, true)));

    for (Term term : DownloadTerms.DOWNLOAD_VERBATIM_TERMS) {
      if (GbifTerm.gbifID == term) {
        continue; // for safety, we code defensively as it may be added
      }

      // TODO: add UDFs
      builder.add(new InitializableField(term,
                                         HiveColumns.VERBATIM_COL_PREFIX + term.simpleName().toLowerCase(),
                                         // no escape needed due to prefix
                                         HiveDataTypes.TYPE_STRING));
    }
    return builder.build();
  }

  /**
   * @return the select fields for the interpreted table in the simple download
   */
  static List<InitializableField> selectInterpretedFields(boolean useInitializers) {
    return selectDownloadFields(DownloadTerms.DOWNLOAD_INTERPRETED_TERMS, useInitializers);
  }

  /**
   * @return the select fields for the table in the simple download
   */
  static List<InitializableField> selectSimpleDownloadFields() {
    return selectDownloadFields(DownloadTerms.SIMPLE_DOWNLOAD_TERMS, true);
  }

  /**
   * @return the select fields for the interpreted table in the simple download
   */
  private static List<InitializableField> selectDownloadFields(Set<Term> terms, boolean useInitializers) {
    ImmutableList.Builder<InitializableField> builder = ImmutableList.builder();
    // always add the GBIF ID
    builder.add(new InitializableField(GbifTerm.gbifID,
                                       HiveColumns.columnFor(GbifTerm.gbifID),
                                       HiveDataTypes.typeForTerm(GbifTerm.gbifID, true)));

    for (Term term : terms) {
      if (GbifTerm.gbifID == term) {
        continue; // for safety, we code defensively as it may be added
      }
      if (useInitializers && TermUtils.isInterpretedDate(term)) {
        builder.add(new InitializableField(term, toISO8601Initializer(term), HiveDataTypes.TYPE_STRING));
      } else {
        builder.add(new InitializableField(term, HiveColumns.columnFor(term), HiveDataTypes.TYPE_STRING));
      }
    }
    return builder.build();
  }

  /**
   * Transforms the term into toISO8601(hiveColumn) expression.
   */
  private static String toISO8601Initializer(Term term) {
    final String column = HiveColumns.columnFor(term);
    return "toISO8601(" + column + ") AS " + column;
  }

  /**
   * Hidden constructor.
   */
  private Queries() {
    //empty constructor
  }
}
