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
package org.gbif.occurrence.download.hive;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.ImmutableMap;

/**
 * Utilities related to the actual queries executed at runtime.
 * The queries relate closely to the data definitions (obviously) and this class provides the bridge between the
 * definitions and the queries.
 *
 * Subclasses define the queries needed for generating TSV, Avro etc.
 */
public abstract class Queries {

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
  public Map<String, InitializableField> selectVerbatimFields() {
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
  public Map<String, InitializableField> selectInterpretedFields(boolean useInitializers) {
    return selectDownloadFields(DownloadTerms.DOWNLOAD_INTERPRETED_TERMS, useInitializers);
  }

  /**
   * @param useInitializers whether to convert dates, arrays etc to strings
   * @return the select fields for the interpreted multimedia extension fields
   */
  public Map<String, InitializableField> selectMultimediaFields(boolean useInitializers) {
    return selectDownloadFields(DownloadTerms.DOWNLOAD_MULTIMEDIA_TERMS, useInitializers);
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
   * @return the select fields for the internal search fields
   */
  public Map<String, InitializableField> selectInternalSearchFields(boolean useInitializers) {
    return selectDownloadFields(DownloadTerms.INTERNAL_SEARCH_TERMS, useInitializers);
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
   * Used for complex types like Structs which have nested elements.
   */
  protected static String toNestedHiveInitializer(Term term, String fieldName) {
    return HiveColumns.columnFor(term) + "." +  fieldName;
  }

  /**
   * Used for complex types like Structs which have nested elements.
   */
  public static String toVocabularyConceptHiveInitializer(Term term) {
    return toNestedHiveInitializer(term, "concept");
  }

  /**
   * @param useInitializers whether to convert dates, arrays etc to strings
   * @return the select fields for the simple download fields
   */
  public Map<String, InitializableField> selectSimpleDownloadFields(boolean useInitializers) {
    return selectGroupedDownloadFields(DownloadTerms.SIMPLE_DOWNLOAD_TERMS, useInitializers);
  }

  /**
   * @param useInitializers whether to convert dates, arrays etc to strings
   * @return the select fields for the simple-with-verbatim download fields
   */
  Map<String, InitializableField> selectSimpleWithVerbatimDownloadFields(boolean useInitializers) {
    return selectGroupedDownloadFields(DownloadTerms.SIMPLE_WITH_VERBATIM_DOWNLOAD_TERMS, useInitializers);
  }

  public Map<String, InitializableField> simpleWithVerbatimAvroQueryFields(boolean useInitializers) {
    Map<String, InitializableField> simpleFields = selectSimpleWithVerbatimDownloadFields(useInitializers);
    Map<String, InitializableField> verbatimFields = new TreeMap<>(selectVerbatimFields());

    // Omit any verbatim fields present in the simple download.
    for (String field : simpleFields.keySet()) {
      verbatimFields.remove(field);
    }

    return ImmutableMap.<String, InitializableField>builder().putAll(simpleFields).putAll(verbatimFields).build();
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
