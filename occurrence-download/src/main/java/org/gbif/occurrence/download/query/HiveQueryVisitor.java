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
package org.gbif.occurrence.download.query;

import org.gbif.api.model.occurrence.predicate.CompoundPredicate;
import org.gbif.api.model.occurrence.predicate.ConjunctionPredicate;
import org.gbif.api.model.occurrence.predicate.DisjunctionPredicate;
import org.gbif.api.model.occurrence.predicate.EqualsPredicate;
import org.gbif.api.model.occurrence.predicate.GreaterThanOrEqualsPredicate;
import org.gbif.api.model.occurrence.predicate.GreaterThanPredicate;
import org.gbif.api.model.occurrence.predicate.InPredicate;
import org.gbif.api.model.occurrence.predicate.IsNotNullPredicate;
import org.gbif.api.model.occurrence.predicate.LessThanOrEqualsPredicate;
import org.gbif.api.model.occurrence.predicate.LessThanPredicate;
import org.gbif.api.model.occurrence.predicate.LikePredicate;
import org.gbif.api.model.occurrence.predicate.NotPredicate;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.api.model.occurrence.predicate.SimplePredicate;
import org.gbif.api.model.occurrence.predicate.WithinPredicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.util.IsoDateParsingUtils;
import org.gbif.api.util.IsoDateParsingUtils.IsoDateFormat;
import org.gbif.api.util.SearchTypeValidator;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.HiveColumnsUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class builds a WHERE clause for a Hive query from a {@link org.gbif.api.model.occurrence.predicate.Predicate}
 * object.
 * </p>
 * This is not thread-safe but one instance can be reused. It is package-local and should usually be accessed through
 * {@link DownloadRequestServiceImpl}. All {@code visit} methods have to be public for the
 * {@link Class#getMethod(String, Class[])} call to work. This is the primary reason for this class being
 * package-local.
 * </p>
 * The only entry point into this class is the {@code getHiveQuery} method!
 */
// TODO: We should check somewhere for the length of the string to avoid possible attacks/oom situations (OCC-35)
public class HiveQueryVisitor {

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
  private static final String IS_NOT_NULL_OPERATOR = " IS NOT NULL ";
  private static final String IS_NOT_NULL_ARRAY_OPERATOR = "(%1$s IS NOT NULL AND size(%1$s) > 0)";
  private static final CharMatcher APOSTROPHE_MATCHER = CharMatcher.is('\'');
  // where query to execute a select all
  private static final String ALL_QUERY = "true";

  private static final String MEDIATYPE_CONTAINS_FMT =
    "array_contains(" + HiveColumnsUtils.getHiveColumn(GbifTerm.mediaType) + ",'%s')";
  private static final String ISSUE_CONTAINS_FMT =
    "array_contains(" + HiveColumnsUtils.getHiveColumn(GbifTerm.issue) + ",'%s')";

  private static final String HIVE_ARRAY_PRE = "ARRAY";

  private static final List<GbifTerm> NUB_KEYS = ImmutableList.of(GbifTerm.taxonKey,
                                                                  GbifTerm.kingdomKey,
                                                                  GbifTerm.phylumKey,
                                                                  GbifTerm.classKey,
                                                                  GbifTerm.orderKey,
                                                                  GbifTerm.familyKey,
                                                                  GbifTerm.genusKey,
                                                                  GbifTerm.subgenusKey,
                                                                  GbifTerm.speciesKey);

  // parameter that map directly to Hive, boundingBox, coordinate and taxonKey are treated special !
  private static final Map<OccurrenceSearchParameter, ? extends Term> PARAM_TO_TERM =
    ImmutableMap.<OccurrenceSearchParameter, Term>builder()
      .put(OccurrenceSearchParameter.DATASET_KEY, GbifTerm.datasetKey)
      .put(OccurrenceSearchParameter.YEAR, DwcTerm.year)
      .put(OccurrenceSearchParameter.MONTH, DwcTerm.month)
      .put(OccurrenceSearchParameter.DECIMAL_LATITUDE, DwcTerm.decimalLatitude)
      .put(OccurrenceSearchParameter.DECIMAL_LONGITUDE, DwcTerm.decimalLongitude)
      .put(OccurrenceSearchParameter.ELEVATION, GbifTerm.elevation)
      .put(OccurrenceSearchParameter.DEPTH, GbifTerm.depth)
      .put(OccurrenceSearchParameter.INSTITUTION_CODE, DwcTerm.institutionCode)
      .put(OccurrenceSearchParameter.COLLECTION_CODE, DwcTerm.collectionCode)
      .put(OccurrenceSearchParameter.CATALOG_NUMBER, DwcTerm.catalogNumber)
      .put(OccurrenceSearchParameter.SCIENTIFIC_NAME, DwcTerm.scientificName)
      .put(OccurrenceSearchParameter.OCCURRENCE_ID,
           DwcTerm.occurrenceID).put(OccurrenceSearchParameter.ESTABLISHMENT_MEANS, DwcTerm.establishmentMeans)
      // the following need some value transformation
      .put(OccurrenceSearchParameter.EVENT_DATE, DwcTerm.eventDate)
      .put(OccurrenceSearchParameter.LAST_INTERPRETED, GbifTerm.lastInterpreted)
      .put(OccurrenceSearchParameter.BASIS_OF_RECORD, DwcTerm.basisOfRecord)
      .put(OccurrenceSearchParameter.COUNTRY, DwcTerm.countryCode)
      .put(OccurrenceSearchParameter.CONTINENT, DwcTerm.continent)
      .put(OccurrenceSearchParameter.PUBLISHING_COUNTRY, GbifTerm.publishingCountry)
      .put(OccurrenceSearchParameter.RECORDED_BY, DwcTerm.recordedBy)
      .put(OccurrenceSearchParameter.RECORD_NUMBER, DwcTerm.recordNumber)
      .put(OccurrenceSearchParameter.TYPE_STATUS, DwcTerm.typeStatus)
      .put(OccurrenceSearchParameter.HAS_COORDINATE, GbifTerm.hasCoordinate)
      .put(OccurrenceSearchParameter.HAS_GEOSPATIAL_ISSUE, GbifTerm.hasGeospatialIssues)
      .put(OccurrenceSearchParameter.MEDIA_TYPE, GbifTerm.mediaType)
      .put(OccurrenceSearchParameter.ISSUE, GbifTerm.issue)
      .build();

  private StringBuilder builder;

  private static String toHiveField(OccurrenceSearchParameter param) {
    if (PARAM_TO_TERM.containsKey(param)) {
      return HiveColumnsUtils.getHiveColumn(PARAM_TO_TERM.get(param));
    }
    // QueryBuildingException requires an underlying exception
    throw new IllegalArgumentException("Search parameter " + param + " is not mapped to Hive");
  }

  /**
   * Converts a value to the form expected by Hive/Hbase based on the OccurrenceSearchParameter.
   * Most values pass by unaltered. Quotes are added for values that need to be quoted, escaping any existing quotes.
   *
   * @param param the type of parameter defining the expected type
   * @param value the original query value
   *
   * @return the converted value expected by HBase
   */
  private static String toHiveValue(OccurrenceSearchParameter param, String value) {
    if (Enum.class.isAssignableFrom(param.type())) {
      // all enum parameters are uppercase
      return '\'' + value.toUpperCase() + '\'';
    }

    if (Date.class.isAssignableFrom(param.type())) {
      // use longs for timestamps expressed as ISO dates
      Date d = IsoDateParsingUtils.parseDate(value);
      return String.valueOf(d.getTime());

    } else if (Number.class.isAssignableFrom(param.type()) || Boolean.class.isAssignableFrom(param.type())) {
      // dont quote numbers
      return value;

    } else {
      // quote value, escape existing quotes
      return '\'' + APOSTROPHE_MATCHER.replaceFrom(value, "\\\'") + '\'';
    }
  }

  /**
   * Translates a valid {@link org.gbif.api.model.occurrence.Download} object and translates it into a
   * strings that can be used as the <em>WHERE</em> clause for a Hive download.
   *
   * @param predicate to translate
   *
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
   */
  public void visit(EqualsPredicate predicate) throws QueryBuildingException {
    if (OccurrenceSearchParameter.TAXON_KEY == predicate.getKey()) {
      appendTaxonKeyFilter(predicate.getValue());
    } else if (OccurrenceSearchParameter.MEDIA_TYPE == predicate.getKey()) {
      builder.append(String.format(MEDIATYPE_CONTAINS_FMT, predicate.getValue().toUpperCase()));
    } else if (OccurrenceSearchParameter.ISSUE == predicate.getKey()) {
      builder.append(String.format(ISSUE_CONTAINS_FMT, predicate.getValue().toUpperCase()));
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

  public void visit(IsNotNullPredicate predicate) throws QueryBuildingException {
    if (isHiveArray(predicate.getParameter())) {
      builder.append(String.format(IS_NOT_NULL_ARRAY_OPERATOR, toHiveField(predicate.getParameter())));
    } else {
      builder.append(toHiveField(predicate.getParameter()));
      builder.append(IS_NOT_NULL_OPERATOR);
    }
  }

  public void visit(WithinPredicate within) {
    // the geometry must be valid - it was validated in the predicates constructor
    builder.append("contains(\"");
    builder.append(within.getGeometry());
    builder.append("\", ");
    builder.append(HiveColumnsUtils.getHiveColumn(DwcTerm.decimalLatitude));
    builder.append(", ");
    builder.append(HiveColumnsUtils.getHiveColumn(DwcTerm.decimalLongitude));
    builder.append(')');
  }

  /**
   * Builds a list of predicates joined by 'op' statements.
   * The final statement will look like this:
   * <p/>
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
    if (OccurrenceSearchParameter.ISSUE == predicate.getKey()) {
      // ignore - there's no way to actually request this in the interface, nor is it indexed in solr
    } else {
      if (Date.class.isAssignableFrom(predicate.getKey().type())) {
        if (SearchTypeValidator.isRange(predicate.getValue())) {
          visit(toDateRangePredicate(IsoDateParsingUtils.parseDateRange(predicate.getValue()), predicate.getKey()));
          return;
        } else {
          IsoDateFormat isoDateFormat = IsoDateParsingUtils.getFirstDateFormatMatch(predicate.getValue());
          if (IsoDateFormat.FULL != isoDateFormat) {
            visit(toDatePredicateQuery(predicate.getKey(), predicate.getValue(), isoDateFormat));
            return;
          }
        }

      }
      builder.append(toHiveField(predicate.getKey()));
      builder.append(op);
      builder.append(toHiveValue(predicate.getKey(), predicate.getValue()));

    }
  }

  /**
   * Determines if the type of a parameter it'a a Hive array.
   */
  private boolean isHiveArray(OccurrenceSearchParameter parameter) {
    return HiveColumnsUtils.getHiveType(PARAM_TO_TERM.get(parameter)).startsWith(HIVE_ARRAY_PRE);
  }

  /**
   * Searches any of the nub keys in HBase of any rank.
   *
   * @param taxonKey to append as filter
   */
  private void appendTaxonKeyFilter(String taxonKey) {
    builder.append('(');
    boolean first = true;
    for (Term term : NUB_KEYS) {
      if (!first) {
        builder.append(DISJUNCTION_OPERATOR);
      }
      builder.append(HiveColumnsUtils.getHiveColumn(term));
      builder.append(EQUALS_OPERATOR);
      builder.append(taxonKey);
      first = false;
    }
    builder.append(')');
  }

  /**
   * Converts a Date query into conjunction predicate.
   * If the value has the forms 'yyyy'/'yyyy-MM' it's translated to:
   * field >= firstDateOfYear/Month(value) and field <= lastDateOfYear/Month(value).
   * If the value is a range it is translated to: field >= range.lower AND field <= range.upper.
   */
  private CompoundPredicate toDatePredicateQuery(
    OccurrenceSearchParameter key,
    String value,
    IsoDateFormat dateFormat
  ) {
    final Date lowerDate = IsoDateParsingUtils.parseDate(value);
    return toDateRangePredicate(Range.closed(lowerDate, IsoDateParsingUtils.toLastDayOf(lowerDate, dateFormat)), key);
  }

  /**
   * Converts date range into a conjunction predicate with the form: field >= range.lower AND field <= range.upper.
   */
  private CompoundPredicate toDateRangePredicate(Range<Date> range, OccurrenceSearchParameter key) {
    ImmutableList<Predicate> predicates = new ImmutableList.Builder<Predicate>().add(new GreaterThanOrEqualsPredicate(
      key,
      IsoDateFormat.FULL.getDateFormat().format(range.lowerEndpoint().getTime())))
      .add(new LessThanOrEqualsPredicate(key, IsoDateFormat.FULL.getDateFormat().format(range.upperEndpoint())))
      .build();
    return new ConjunctionPredicate(predicates);
  }

  private void visit(Object object) throws QueryBuildingException {
    Method method = null;
    try {
      method = getClass().getMethod("visit", new Class[] {object.getClass()});
    } catch (NoSuchMethodException e) {
      LOG.warn(
        "Visit method could not be found. That means a Predicate has been passed in that is unknown to this class",
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
