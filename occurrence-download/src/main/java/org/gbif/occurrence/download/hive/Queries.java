package org.gbif.occurrence.download.hive;

import org.apache.commons.lang3.tuple.Pair;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Utilities related to the actual queries executed at runtime.
 * The queries relate closely to the data definitions (obviously) and this class provides the bridge between the
 * definitions and the queries.
 *
 * Subclasses define the queries needed for generating TSV, Avro etc.
 */
abstract class Queries {

  /**
   * @return the gbifID term as an InitializableField
   */
  private InitializableField selectGbifId() {
    return new InitializableField(
      GbifTerm.gbifID,
      HiveColumns.columnFor(GbifTerm.gbifID),
      HiveDataTypes.typeForTerm(GbifTerm.gbifID, false)
    );
  }

  /**
   * @return the select fields for the verbatim download fields.
   */
  Map<String, InitializableField> selectVerbatimFields() {
    Map<String, InitializableField> result = new LinkedHashMap<>();

    // always add the GBIF ID
    result.put(GbifTerm.gbifID.simpleName(), selectGbifId());

    for (Term term : DownloadTerms.DOWNLOAD_VERBATIM_TERMS) {
      if (GbifTerm.gbifID == term) {
        continue; // for safety, we code defensively as it may be added
      }

      result.put("v_" + term.simpleName(), new InitializableField(
        term,
        toVerbatimHiveInitializer(term),
        toVerbatimHiveDataType(term)
      ));
    }

    return result;
  }

  /**
   * @return The Hive data type for this verbatim term: STRING<STRING>
   */
  protected String toVerbatimHiveDataType(Term term) {
    return HiveDataTypes.TYPE_STRING;
  }

  /**
   * @return The Hive data type for this term, e.g. STRING, ARRAY<STRING>
   */
  abstract String toHiveDataType(Term term);

  /**
   * @return The Hive initializer for verbatim fields, e.g. v_eventdate
   */
  abstract String toVerbatimHiveInitializer(Term term);


  /**
   * @param useInitializers whether to convert dates, arrays etc to strings
   * @return the select fields for the interpreted download fields
   */
  Map<String, InitializableField> selectInterpretedFields(boolean useInitializers) {
    return selectDownloadFields(DownloadTerms.DOWNLOAD_INTERPRETED_TERMS, useInitializers);
  }

  /**
   * @param useInitializers whether to convert dates, arrays etc to strings
   * @return the select fields for the internal download fields
   */
  Map<String, InitializableField> selectInternalFields(boolean useInitializers) {
    return selectDownloadFields(DownloadTerms.INTERNAL_DOWNLOAD_TERMS, useInitializers);
  }

  /**
   * @param useInitializers whether to convert dates, arrays etc to strings
   * @return the select fields for the interpreted download fields
   */
  Map<String, InitializableField> selectDownloadFields(Set<Term> terms, boolean useInitializers) {
    Map<String, InitializableField> result = new LinkedHashMap<>();

    // always add the GBIF ID
    result.put(GbifTerm.gbifID.simpleName(), selectGbifId());

    for (Term term : terms) {
      if (GbifTerm.gbifID == term) {
        continue; // for safety, we code defensively as it may be added
      }

      if (term.equals(GbifTerm.verbatimScientificName)) {
        result.put(term.simpleName(), new InitializableField(
          term,
          toVerbatimHiveInitializer(DwcTerm.scientificName),
          toVerbatimHiveDataType(DwcTerm.scientificName)
        ));
      } else {
        result.put(term.simpleName(), new InitializableField(
          term,
          useInitializers ? toInterpretedHiveInitializer(term) : toHiveInitializer(term),
          toHiveDataType(term)
        ));
      }
    }

    return result;
  }

  /**
   * @return The Hive initializer for interpreted fields, e.g. eventdate
   */
  abstract String toHiveInitializer(Term term);

  /**
   * @return The initializer for an interpreted field, e.g. genus, toISO8601(eventdate) AS eventdate
   */
  abstract String toInterpretedHiveInitializer(Term term);

  /**
   * @param useInitializers whether to convert dates, arrays etc to strings
   * @return the select fields for the simple download fields
   */
  Map<String, InitializableField> selectSimpleDownloadFields(boolean useInitializers) {
    return selectGroupedDownloadFields(DownloadTerms.SIMPLE_DOWNLOAD_TERMS, useInitializers);
  }

  /**
   * @param useInitializers whether to convert dates, arrays etc to strings
   * @return the select fields for the simple-with-verbatim download fields
   */
  Map<String, InitializableField> selectSimpleWithVerbatimDownloadFields(boolean useInitializers) {
    return selectGroupedDownloadFields(DownloadTerms.SIMPLE_WITH_VERBATIM_DOWNLOAD_TERMS, useInitializers);
  }

  /**
   * @param useInitializers whether to convert dates, arrays etc to strings
   * @return the select fields for the (mostly) interpreted table in the simple download
   */
  Map<String, InitializableField> selectGroupedDownloadFields(Set<Pair<DownloadTerms.Group, Term>> termPairs, boolean useInitializers) {
    Map<String, InitializableField> result = new LinkedHashMap<>();

    // always add the GBIF ID
    result.put(GbifTerm.gbifID.simpleName(), selectGbifId());

    for (Pair<DownloadTerms.Group, Term> termPair : termPairs) {
      Term term = termPair.getRight();
      if (GbifTerm.gbifID == term) {
        continue; // for safety, we code defensively as it may be added
      }

      if (termPair.getLeft().equals(DownloadTerms.Group.VERBATIM)) {
        result.put("v_" + term.simpleName(), new InitializableField(
          term,
          toVerbatimHiveInitializer(term),
          toVerbatimHiveDataType(term)
        ));
      } else {
        result.put(term.simpleName(), new InitializableField(
          term,
          useInitializers ? toInterpretedHiveInitializer(term) : toHiveInitializer(term),
          toHiveDataType(term)
        ));
      }
    }

    return result;
  }
}
