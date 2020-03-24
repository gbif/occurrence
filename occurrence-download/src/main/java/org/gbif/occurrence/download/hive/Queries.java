package org.gbif.occurrence.download.hive;

import org.apache.commons.lang3.tuple.Pair;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.HiveColumnsUtils;
import org.gbif.occurrence.common.TermUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

import static org.gbif.occurrence.download.hive.Queries.Initializers.AVRO;
import static org.gbif.occurrence.download.hive.Queries.Initializers.RAW;
import static org.gbif.occurrence.download.hive.Queries.Initializers.TEXT;

/**
 * Utilities related to the actual queries executed at runtime.
 * The queries relate closely to the data definitions (obviously) and this class provides the bridge between the
 * definitions and the queries.
 */
class Queries {

  enum Initializers {
    RAW,
    TEXT,
    AVRO
  }

  private static final String JOIN_ARRAY_FMT = "if(%1$s IS NULL,'',joinArray(%1$s,'\\\\;')) AS %1$s";

  /**
   * @return the select fields for the verbatim table for an AVRO-format download
   */
  static Map<String, InitializableField> selectAvroVerbatimFields() {
    // The sorted map is only for easier debugging during development.
    return selectVerbatimFields().stream()
      .collect(Collectors.toMap(i -> i.getTerm().simpleName(), Function.identity(), (o1, o2) -> o1, TreeMap::new));
  }

  /**
   * @return the select fields for the verbatim table in the DwCA download
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

      builder.add(new InitializableField(term,
                                         HiveColumns.VERBATIM_COL_PREFIX + term.simpleName().toLowerCase(),
                                         // no escape needed due to prefix
                                         HiveDataTypes.TYPE_STRING));
    }
    return builder.build();
  }

  /**
   * @return the select fields for the interpreted table for an AVRO-format download
   */
  static Map<String, InitializableField> selectAvroInterpretedFields() {
    // The sorted map is only for easier debugging during development.
    return selectInterpretedFields(AVRO).stream()
      .collect(Collectors.toMap(i -> i.getTerm().simpleName(), Function.identity(), (o1, o2) -> o1, TreeMap::new));
  }

  /**
   * @return the select fields for the interpreted table in the DwCA download
   */
  static List<InitializableField> selectInterpretedFields(Initializers initializers) {
    return selectDownloadFields(DownloadTerms.DOWNLOAD_INTERPRETED_TERMS, initializers);
  }

  /**
   * @return the select fields for the table in the simple download
   */
  static List<InitializableField> selectSimpleDownloadFields() {
    return selectGroupedDownloadFields(DownloadTerms.SIMPLE_DOWNLOAD_TERMS, TEXT);
  }

  /**
   * @return the select fields for the internal fields for an AVRO-format download
   */
  static Map<String, InitializableField> selectAvroInternalFields() {
    // The sorted map is only for easier debugging during development.
    return selectDownloadFields(DownloadTerms.INTERNAL_DOWNLOAD_TERMS, AVRO).stream()
      .collect(Collectors.toMap(i -> i.getTerm().simpleName(), Function.identity(), (o1, o2) -> o1, TreeMap::new));
  }

  /**
   * @return the select fields for the interpreted table in the DwCA download
   */
  private static List<InitializableField> selectDownloadFields(Set<Term> terms, Initializers initializers) {
    ImmutableList.Builder<InitializableField> builder = ImmutableList.builder();
    // always add the GBIF ID
    builder.add(new InitializableField(GbifTerm.gbifID,
                                       HiveColumns.columnFor(GbifTerm.gbifID),
                                       HiveDataTypes.typeForTerm(GbifTerm.gbifID, true)));

    for (Term term : terms) {
      if (GbifTerm.gbifID == term) {
        continue; // for safety, we code defensively as it may be added
      }
      if (initializers != RAW && TermUtils.isInterpretedDate(term)) {
        builder.add(new InitializableField(term, toISO8601Initializer(term), HiveDataTypes.TYPE_STRING));
      } else if (initializers == TEXT && HiveColumnsUtils.isHiveArray(term)){
        builder.add(new InitializableField(term, String.format(JOIN_ARRAY_FMT, HiveColumns.columnFor(term)), HiveDataTypes.TYPE_STRING));
      } else {
        builder.add(new InitializableField(term, HiveColumns.columnFor(term), HiveDataTypes.TYPE_STRING));
      }
    }
    return builder.build();
  }

  /**
   * @return the select fields for the (mostly) interpreted table in the simple download
   */
  private static List<InitializableField> selectGroupedDownloadFields(Set<Pair<DownloadTerms.Group, Term>> termPairs, Initializers initializers) {
    ImmutableList.Builder<InitializableField> builder = ImmutableList.builder();
    // always add the GBIF ID
    builder.add(new InitializableField(GbifTerm.gbifID,
      HiveColumns.columnFor(GbifTerm.gbifID),
      HiveDataTypes.typeForTerm(GbifTerm.gbifID, true)));

    for (Pair<DownloadTerms.Group, Term> termPair : termPairs) {
      Term term = termPair.getRight();
      if (GbifTerm.gbifID == term) {
        continue; // for safety, we code defensively as it may be added
      }

      if (termPair.getLeft().equals(DownloadTerms.Group.VERBATIM)) {
        builder.add(new InitializableField(term, toVerbatimInitializer(termPair), HiveDataTypes.TYPE_STRING));
      } else if (initializers != RAW && TermUtils.isInterpretedDate(term)) {
        builder.add(new InitializableField(term, toISO8601Initializer(term), HiveDataTypes.TYPE_STRING));
      } else if (initializers == TEXT && HiveColumnsUtils.isHiveArray(term)){
        builder.add(new InitializableField(term, String.format(JOIN_ARRAY_FMT,HiveColumns.columnFor(term)), HiveDataTypes.TYPE_STRING));
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
   * Transforms the term into the "verbatim"-prefixed expression.
   */
  private static String toVerbatimInitializer(Pair<DownloadTerms.Group, Term> termPair) {
    return HiveColumns.VERBATIM_COL_PREFIX + termPair.getRight().simpleName().toLowerCase() + " AS " + DownloadTerms.simpleName(termPair).toLowerCase();
  }

  /**
   * Hidden constructor.
   */
  private Queries() {
    //empty constructor
  }
}
