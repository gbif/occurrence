/*
 * Copyright 2012 Global Biodiversity Information Facility (GBIF)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.download.service;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.predicate.CompoundPredicate;
import org.gbif.api.model.occurrence.predicate.ConjunctionPredicate;
import org.gbif.api.model.occurrence.predicate.DisjunctionPredicate;
import org.gbif.api.model.occurrence.predicate.EqualsPredicate;
import org.gbif.api.model.occurrence.predicate.GreaterThanOrEqualsPredicate;
import org.gbif.api.model.occurrence.predicate.GreaterThanPredicate;
import org.gbif.api.model.occurrence.predicate.InPredicate;
import org.gbif.api.model.occurrence.predicate.LessThanOrEqualsPredicate;
import org.gbif.api.model.occurrence.predicate.LessThanPredicate;
import org.gbif.api.model.occurrence.predicate.LikePredicate;
import org.gbif.api.model.occurrence.predicate.NotPredicate;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.api.model.occurrence.predicate.SimplePredicate;
import org.gbif.api.model.occurrence.predicate.WithinPredicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.util.IsoDateParsingUtils;
import org.gbif.api.util.VocabularyUtils;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.occurrence.common.download.HiveFieldUtil;
import org.gbif.occurrence.common.constants.FieldName;
import org.gbif.occurrence.common.converter.BasisOfRecordConverter;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class builds a WHERE clause for a Hive query from a {@link org.gbif.api.model.occurrence.predicate.Predicate}
 * object.
 * </p>
 * This is not thread-safe but one instance can be reused. It is package-local and should usually be accessed through
 * {@link DownloadRequestServiceImpl}. All {@code visit} methods have to be public for the
 * {@link Class#getMethod(String, Class[])} call to work. This is the primary reason for this class being package-local.
 * </p>
 * The only entry point into this class is the {@code getHiveQuery} method!
 */
// TODO: We should check somewhere for the length of the string to avoid possible attacks/oom situations (OCC-35)
class HiveQueryVisitor {

  private static final Logger LOG = LoggerFactory.getLogger(HiveQueryVisitor.class);

  private static final String CONJUNCTION_OPERATOR = " AND ";
  private static final String DISJUNCTION_OPERATOR = " OR ";
  private static final String EQUALS_OPERATOR = " = ";
  private static final String GREATER_THAN_OPERATOR = " > ";
  private static final String GREATER_THAN_EQUALS_OPERATOR = " >= ";
  private static final String LESS_THAN_OPERATOR = " < ";
  private static final String LESS_THAN_EQUALS_OPERATOR = " <= ";
  private static final String NOT_OPERATOR = "NOT ";
  private static final String LIKE_OPERATOR = " LIKE ";
  private static final String IS_NULL_OPERATOR = " IS NULL";
  private static final String IS_NOT_NULL_OPERATOR = " IS NOT NULL";
  private static final CharMatcher APOSTROPHE_MATCHER = CharMatcher.is('\'');
  // where query to execute a select all
  private static final String ALL_QUERY = "true";


  private static final List<FieldName> NUB_KEY_COLUMNS = ImmutableList.of(
    FieldName.I_NUB_ID,
    FieldName.I_KINGDOM_ID,
    FieldName.I_PHYLUM_ID,
    FieldName.I_CLASS_ID,
    FieldName.I_ORDER_ID,
    FieldName.I_FAMILY_ID,
    FieldName.I_GENUS_ID,
    FieldName.I_SPECIES_ID);

  private static final BasisOfRecordConverter BOR_CONVERTER = new BasisOfRecordConverter();

  // parameter that map directly to Hive, boundingBox, coordinate and taxonKey are treated special !
  private static final Map<OccurrenceSearchParameter, FieldName> PARAM_TO_FIELD = ImmutableMap
    .<OccurrenceSearchParameter, FieldName>builder()
    .put(OccurrenceSearchParameter.DATASET_KEY, FieldName.DATASET_KEY)
    .put(OccurrenceSearchParameter.YEAR, FieldName.I_YEAR)
    .put(OccurrenceSearchParameter.MONTH, FieldName.I_MONTH)
    .put(OccurrenceSearchParameter.LATITUDE, FieldName.I_LATITUDE)
    .put(OccurrenceSearchParameter.LONGITUDE, FieldName.I_LONGITUDE)
    .put(OccurrenceSearchParameter.ALTITUDE, FieldName.I_ALTITUDE)
    .put(OccurrenceSearchParameter.DEPTH, FieldName.I_DEPTH)
    .put(OccurrenceSearchParameter.INSTITUTION_CODE, FieldName.INSTITUTION_CODE)
    .put(OccurrenceSearchParameter.COLLECTION_CODE, FieldName.COLLECTION_CODE)
    .put(OccurrenceSearchParameter.CATALOG_NUMBER, FieldName.CATALOG_NUMBER)
    .put(OccurrenceSearchParameter.COLLECTOR_NAME, FieldName.COLLECTOR_NAME)
    .put(OccurrenceSearchParameter.SCIENTIFIC_NAME, FieldName.I_SCIENTIFIC_NAME)
    // the following need some value transformation
    .put(OccurrenceSearchParameter.DATE, FieldName.I_OCCURRENCE_DATE)
    .put(OccurrenceSearchParameter.MODIFIED, FieldName.MODIFIED)
    .put(OccurrenceSearchParameter.BASIS_OF_RECORD, FieldName.I_BASIS_OF_RECORD)
    .put(OccurrenceSearchParameter.COUNTRY, FieldName.I_ISO_COUNTRY_CODE)
    .put(OccurrenceSearchParameter.PUBLISHING_COUNTRY, FieldName.PUBLISHING_COUNTRY)
    .put(OccurrenceSearchParameter.SPATIAL_ISSUES, FieldName.I_GEOSPATIAL_ISSUE)
    .build();

  // IS_GEOREFERENCED_CHECK, IS_NOT_GEOREFERENCED_CHECK, SPATIAL_ISSUES_CHECK and NO_SPATIAL_ISSUES_CHECK are
  // precalculated since they are the same for all the queries.
  private static final String IS_GEOREFERENCED_CHECK = toHiveField(OccurrenceSearchParameter.LATITUDE)
    + IS_NOT_NULL_OPERATOR + CONJUNCTION_OPERATOR + toHiveField(OccurrenceSearchParameter.LONGITUDE)
    + IS_NOT_NULL_OPERATOR;

  private static final String IS_NOT_GEOREFERENCED_CHECK = '(' + toHiveField(OccurrenceSearchParameter.LATITUDE)
    + IS_NULL_OPERATOR + DISJUNCTION_OPERATOR + toHiveField(OccurrenceSearchParameter.LONGITUDE)
    + IS_NULL_OPERATOR + ')';

  private static final String SPATIAL_ISSUES_CHECK = toHiveField(OccurrenceSearchParameter.SPATIAL_ISSUES)
    + GREATER_THAN_OPERATOR + '0';
  private static final String NO_SPATIAL_ISSUES_CHECK = '(' + toHiveField(OccurrenceSearchParameter.SPATIAL_ISSUES)
    + IS_NULL_OPERATOR + DISJUNCTION_OPERATOR + toHiveField(OccurrenceSearchParameter.SPATIAL_ISSUES) + EQUALS_OPERATOR
    + "0)";

  private StringBuilder builder;

  private static String toHiveField(OccurrenceSearchParameter param) {
    if (PARAM_TO_FIELD.containsKey(param)) {
      return HiveFieldUtil.getHiveField(PARAM_TO_FIELD.get(param));
    }
    // QueryBuildingException requires an underlying exception
    throw new IllegalArgumentException("Search parameter " + param + " is not mapped to Hive");
  }

  /**
   * Translates a valid {@link Download} object and translates it into a
   * strings that can be used as the <em>WHERE</em> clause for a Hive download.
   *
   * @param download to translate
   * @return WHERE clause
   */
  public String getHiveQuery(Predicate predicate) throws QueryBuildingException {
    String hiveQuery = ALL_QUERY;
    if (predicate != null) { // null predicate means a SELECT ALL
      builder = new StringBuilder();
      visit(predicate);
      hiveQuery = builder.toString();
    }

    // Set to null to prevent old StringBuilders hanging around in case this class is reused somewhere else
    builder = null;
    return hiveQuery;
  }

  public void visit(ConjunctionPredicate predicate) throws QueryBuildingException {
    visitCompoundPredicate(predicate, CONJUNCTION_OPERATOR);
  }

  public void visit(DisjunctionPredicate predicate) throws QueryBuildingException {
    visitCompoundPredicate(predicate, DISJUNCTION_OPERATOR);
  }

  /**
   * Supports all parameters incl taxonKey expansion for higher taxa.
   *
   * @param predicate
   */
  public void visit(EqualsPredicate predicate) throws QueryBuildingException {
    if (OccurrenceSearchParameter.TAXON_KEY == predicate.getKey()) {
      appendTaxonKeyFilter(predicate.getValue());

    } else {
      visitSimplePredicate(predicate, EQUALS_OPERATOR);
    }
  }

  public void visit(GreaterThanOrEqualsPredicate predicate) throws QueryBuildingException {
    visitSimplePredicate(predicate, GREATER_THAN_EQUALS_OPERATOR);
  }

  public void visit(GreaterThanPredicate predicate) throws QueryBuildingException {
    visitSimplePredicate(predicate, GREATER_THAN_OPERATOR);
  }

  public void visit(InPredicate predicate) throws QueryBuildingException {
    builder.append('(');
    Iterator<String> iterator = predicate.getValues().iterator();
    while (iterator.hasNext()) {
      String value = iterator.next();
      builder.append('(');
      builder.append(toHiveField(predicate.getKey()));
      builder.append(EQUALS_OPERATOR);
      builder.append(toHiveValue(predicate.getKey(), value));
      builder.append(')');
      if (iterator.hasNext()) {
        builder.append(DISJUNCTION_OPERATOR);
      }
    }
    builder.append(')');
  }

  public void visit(LessThanOrEqualsPredicate predicate) throws QueryBuildingException {
    visitSimplePredicate(predicate, LESS_THAN_EQUALS_OPERATOR);
  }

  public void visit(LessThanPredicate predicate) throws QueryBuildingException {
    visitSimplePredicate(predicate, LESS_THAN_OPERATOR);
  }

  // TODO: This probably won't work without a bit more intelligence
  public void visit(LikePredicate predicate) throws QueryBuildingException {
    visitSimplePredicate(predicate, LIKE_OPERATOR);
  }

  public void visit(NotPredicate predicate) throws QueryBuildingException {
    builder.append(NOT_OPERATOR);
    visit(predicate.getPredicate());
  }

  public void visit(WithinPredicate within) {
    // the geometry must be valid - it was validated in the predicates constructor
    builder.append("contains(\"");
    builder.append(within.getGeometry());
    builder.append("\", ");
    builder.append(HiveFieldUtil.getHiveField(FieldName.I_LATITUDE));
    builder.append(", ");
    builder.append(HiveFieldUtil.getHiveField(FieldName.I_LONGITUDE));
    builder.append(')');
  }

  /**
   * Builds a list of predicates joined by 'op' statements.
   * The final statement will look like this:
   *
   * <pre>
   * ((predicate) op (predicate) ... op (predicate))
   * </pre>
   */
  public void visitCompoundPredicate(CompoundPredicate predicate, String op) throws QueryBuildingException {
    builder.append('(');
    Iterator<Predicate> iterator = predicate.getPredicates().iterator();
    while (iterator.hasNext()) {
      Predicate subPredicate = iterator.next();
      builder.append('(');
      visit(subPredicate);
      builder.append(')');
      if (iterator.hasNext()) {
        builder.append(op);
      }
    }
    builder.append(')');
  }


  public void visitSimplePredicate(SimplePredicate predicate, String op) throws QueryBuildingException {
    if (predicate.getKey() == OccurrenceSearchParameter.SPATIAL_ISSUES) {
      appendSpatialIssuePredicate(predicate.getValue());
    } else if (predicate.getKey() == OccurrenceSearchParameter.GEOREFERENCED) {
      appendGeoreferencedPredicate(predicate.getValue());
    } else {
      builder.append(toHiveField(predicate.getKey()));
      builder.append(op);
      builder.append(toHiveValue(predicate.getKey(), predicate.getValue()));
    }
  }

  /**
   * OccurrenceSearchParameter.GEOREFERENCED is managed specially.
   * The search parameter is a boolean value but in hive must be converted into NULL comparison of the latitude and
   * longitude columns.
   * Be aware that the operator is ignored and it's handled as expression that checks if the record has a spatial issue.
   */
  private void appendGeoreferencedPredicate(String value) {
    if (Boolean.parseBoolean(value)) {
      builder.append(IS_GEOREFERENCED_CHECK);
    } else {
      builder.append(IS_NOT_GEOREFERENCED_CHECK);
    }
  }

  /**
   * OccurrenceSearchParameter.SPATIAL_ISSUES is managed specially. The search parameter is a boolean value but in hive
   * is an integer.
   * Be aware that the operator is ignored and it's handled as expression that checks if the record has a spatial issue.
   */
  private void appendSpatialIssuePredicate(String value) {
    if (Boolean.parseBoolean(value)) {
      builder.append(SPATIAL_ISSUES_CHECK);
    } else {
      builder.append(NO_SPATIAL_ISSUES_CHECK);
    }
  }

  /**
   * Searches any of the nub keys in hbase of any rank.
   *
   * @param taxonKey
   */
  private void appendTaxonKeyFilter(String taxonKey) {
    builder.append('(');
    boolean first = true;
    for (FieldName col : NUB_KEY_COLUMNS) {
      if (!first) {
        builder.append(DISJUNCTION_OPERATOR);
      }
      builder.append(HiveFieldUtil.getHiveField(col));
      builder.append(EQUALS_OPERATOR);
      builder.append(taxonKey);
      first = false;
    }
    builder.append(')');
  }

  /**
   * Converts a value to the form expected by Hive/Hbase based on the OccurrenceSearchParameter.
   * Most values pass by unaltered. Quotes are added for values that need to be quoted, escaping any existing quotes.
   *
   * @param param the type of parameter defining the expected type
   * @param value the original query value
   * @return the converted value expected by HBase
   */
  private String toHiveValue(OccurrenceSearchParameter param, String value) throws QueryBuildingException {
    if (param == OccurrenceSearchParameter.BASIS_OF_RECORD) {
      // special int
      BasisOfRecord bor = (BasisOfRecord) VocabularyUtils.lookupEnum(value, BasisOfRecord.class);
      return bor == null ? "" : BOR_CONVERTER.fromEnum(bor).toString();
    } else if (param == OccurrenceSearchParameter.COUNTRY) {
      // upper case 2 letter iso code
      return '\'' + value.toUpperCase() + '\'';
    }

    if (Date.class.isAssignableFrom(param.type())) {
      // use longs for timestamps expressed as ISO dates
      Date d = IsoDateParsingUtils.parseDate(value);
      return String.valueOf(d.getTime());

    } else if (Number.class.isAssignableFrom(param.type())) {
      // dont quote numbers
      return value;

    } else {
      // quote value, escape existing quotes
      return '\'' + APOSTROPHE_MATCHER.replaceFrom(value, "\\\'") + '\'';
    }
  }

  private void visit(Object object) throws QueryBuildingException {
    Method method = null;
    try {
      method = getClass().getMethod("visit", new Class[] {object.getClass()});
    } catch (NoSuchMethodException e) {
      LOG
        .warn(
          "Visit method could not be found. That means a Predicate has been passed in that is unknown to this "
            + "class",
          e);
      throw new IllegalArgumentException("Unknown Predicate", e);
    }
    try {
      method.invoke(this, object);
    } catch (IllegalAccessException e) {
      LOG.error("This should never happen as all our methods are public and missing methods should have been caught "
        + "before. Probably a programming error", e);
      throw new RuntimeException("Programming error", e);
    } catch (InvocationTargetException e) {
      LOG.info("Exception thrown while building the Hive Download", e);
      throw new QueryBuildingException(e);
    }
  }
}
