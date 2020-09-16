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

import com.google.common.base.Joiner;
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
import org.gbif.api.util.Range;
import org.gbif.api.util.SearchTypeValidator;
import org.gbif.api.util.VocabularyUtils;
import org.gbif.api.vocabulary.MediaType;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GadmTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.HiveColumnsUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import com.google.common.base.CharMatcher;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.gbif.occurrence.search.es.OccurrenceEsField;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.spatial4j.context.jts.DatelineRule;
import org.locationtech.spatial4j.context.jts.JtsSpatialContextFactory;
import org.locationtech.spatial4j.io.WKTReader;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class builds a WHERE clause for a Hive query from a {@link org.gbif.api.model.occurrence.predicate.Predicate}
 * object.
 * </p>
 * This is not thread-safe but one instance can be reused. It is package-local and should usually be accessed through
 * {@link org.gbif.api.service.occurrence.DownloadRequestService}. All {@code visit} methods have to be public for the
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
  private static final String IN_OPERATOR = " IN";
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

  private static final Function<Term, String> ARRAY_FN = t ->
    "array_contains(" + HiveColumnsUtils.getHiveColumn(t) + ",'%s')";

  private static final String HIVE_ARRAY_PRE = "ARRAY";

  private static final List<GbifTerm> NUB_KEYS = ImmutableList.of(
    GbifTerm.taxonKey,
    GbifTerm.acceptedTaxonKey,
    GbifTerm.kingdomKey,
    GbifTerm.phylumKey,
    GbifTerm.classKey,
    GbifTerm.orderKey,
    GbifTerm.familyKey,
    GbifTerm.genusKey,
    GbifTerm.subgenusKey,
    GbifTerm.speciesKey
  );

  private static final List<GadmTerm> GADM_GIDS = ImmutableList.of(
    GadmTerm.level0Gid, GadmTerm.level1Gid, GadmTerm.level2Gid, GadmTerm.level3Gid
  );

  // parameters that map directly to Hive.
  // boundingBox, coordinate, taxonKey and gadmGid are treated specially!
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
      .put(OccurrenceSearchParameter.OCCURRENCE_ID, DwcTerm.occurrenceID)
      .put(OccurrenceSearchParameter.ESTABLISHMENT_MEANS, DwcTerm.establishmentMeans)
      // the following need some value transformation
      .put(OccurrenceSearchParameter.EVENT_DATE, DwcTerm.eventDate)
      .put(OccurrenceSearchParameter.LAST_INTERPRETED, GbifTerm.lastInterpreted)
      .put(OccurrenceSearchParameter.BASIS_OF_RECORD, DwcTerm.basisOfRecord)
      .put(OccurrenceSearchParameter.COUNTRY, DwcTerm.countryCode)
      .put(OccurrenceSearchParameter.CONTINENT, DwcTerm.continent)
      .put(OccurrenceSearchParameter.PUBLISHING_COUNTRY, GbifTerm.publishingCountry)
      .put(OccurrenceSearchParameter.RECORDED_BY, DwcTerm.recordedBy)
      .put(OccurrenceSearchParameter.IDENTIFIED_BY, DwcTerm.identifiedBy)
      .put(OccurrenceSearchParameter.RECORD_NUMBER, DwcTerm.recordNumber)
      .put(OccurrenceSearchParameter.TYPE_STATUS, DwcTerm.typeStatus)
      .put(OccurrenceSearchParameter.HAS_COORDINATE, GbifTerm.hasCoordinate)
      .put(OccurrenceSearchParameter.HAS_GEOSPATIAL_ISSUE, GbifTerm.hasGeospatialIssues)
      .put(OccurrenceSearchParameter.MEDIA_TYPE, GbifTerm.mediaType)
      .put(OccurrenceSearchParameter.ISSUE, GbifTerm.issue)
      .put(OccurrenceSearchParameter.KINGDOM_KEY, GbifTerm.kingdomKey)
      .put(OccurrenceSearchParameter.PHYLUM_KEY, GbifTerm.phylumKey)
      .put(OccurrenceSearchParameter.CLASS_KEY, GbifTerm.classKey)
      .put(OccurrenceSearchParameter.ORDER_KEY, GbifTerm.orderKey)
      .put(OccurrenceSearchParameter.FAMILY_KEY, GbifTerm.familyKey)
      .put(OccurrenceSearchParameter.GENUS_KEY, GbifTerm.genusKey)
      .put(OccurrenceSearchParameter.SUBGENUS_KEY, GbifTerm.subgenusKey)
      .put(OccurrenceSearchParameter.SPECIES_KEY, GbifTerm.speciesKey)
      .put(OccurrenceSearchParameter.ACCEPTED_TAXON_KEY, GbifTerm.acceptedTaxonKey)
      .put(OccurrenceSearchParameter.TAXONOMIC_STATUS, DwcTerm.taxonomicStatus)
      .put(OccurrenceSearchParameter.REPATRIATED, GbifTerm.repatriated)
      .put(OccurrenceSearchParameter.ORGANISM_ID, DwcTerm.organismID)
      .put(OccurrenceSearchParameter.LOCALITY, DwcTerm.locality)
      .put(OccurrenceSearchParameter.COORDINATE_UNCERTAINTY_IN_METERS, DwcTerm.coordinateUncertaintyInMeters)
      .put(OccurrenceSearchParameter.STATE_PROVINCE, DwcTerm.stateProvince)
      .put(OccurrenceSearchParameter.WATER_BODY, DwcTerm.waterBody)
      .put(OccurrenceSearchParameter.GADM_LEVEL_0_GID, GadmTerm.level0Gid)
      .put(OccurrenceSearchParameter.GADM_LEVEL_1_GID, GadmTerm.level1Gid)
      .put(OccurrenceSearchParameter.GADM_LEVEL_2_GID, GadmTerm.level2Gid)
      .put(OccurrenceSearchParameter.GADM_LEVEL_3_GID, GadmTerm.level3Gid)
      .put(OccurrenceSearchParameter.PROTOCOL, GbifTerm.protocol)
      .put(OccurrenceSearchParameter.LICENSE, DcTerm.license)
      .put(OccurrenceSearchParameter.PUBLISHING_ORG, GbifInternalTerm.publishingOrgKey)
      .put(OccurrenceSearchParameter.CRAWL_ID, GbifInternalTerm.crawlId)
      .put(OccurrenceSearchParameter.INSTALLATION_KEY, GbifInternalTerm.installationKey)
      .put(OccurrenceSearchParameter.NETWORK_KEY, GbifInternalTerm.networkKey)
      .put(OccurrenceSearchParameter.EVENT_ID, DwcTerm.eventID)
      .put(OccurrenceSearchParameter.PARENT_EVENT_ID, DwcTerm.parentEventID)
      .put(OccurrenceSearchParameter.SAMPLING_PROTOCOL, DwcTerm.samplingProtocol)
      .put(OccurrenceSearchParameter.PROJECT_ID, GbifInternalTerm.projectId)
      .put(OccurrenceSearchParameter.PROGRAMME, GbifInternalTerm.programmeAcronym)
      .put(OccurrenceSearchParameter.VERBATIM_SCIENTIFIC_NAME, GbifTerm.verbatimScientificName)
      .put(OccurrenceSearchParameter.TAXON_ID, DwcTerm.taxonID)
      .put(OccurrenceSearchParameter.SAMPLE_SIZE_UNIT, DwcTerm.sampleSizeUnit)
      .put(OccurrenceSearchParameter.SAMPLE_SIZE_VALUE, DwcTerm.sampleSizeValue)
      .put(OccurrenceSearchParameter.ORGANISM_QUANTITY, DwcTerm.organismQuantity)
      .put(OccurrenceSearchParameter.ORGANISM_QUANTITY_TYPE, DwcTerm.organismQuantityType)
      .put(OccurrenceSearchParameter.RELATIVE_ORGANISM_QUANTITY, GbifTerm.relativeOrganismQuantity)
      .put(OccurrenceSearchParameter.COLLECTION_KEY, GbifInternalTerm.collectionKey)
      .put(OccurrenceSearchParameter.INSTITUTION_KEY, GbifInternalTerm.institutionKey)
      .put(OccurrenceSearchParameter.RECORDED_BY_ID, GbifTerm.recordedByID)
      .put(OccurrenceSearchParameter.IDENTIFIED_BY_ID, GbifTerm.identifiedByID)
      .put(OccurrenceSearchParameter.OCCURRENCE_STATUS, DwcTerm.occurrenceStatus)
      .put(OccurrenceSearchParameter.HOSTING_ORGANIZATION_KEY, GbifInternalTerm.hostingOrganizationKey)
      .build();

  private final Joiner commaJoiner = Joiner.on(", ").skipNulls();

  private StringBuilder builder;

  /**
   * Transforms the value to the Hive statement lower(val).
   */
  private static String toHiveLower(String val) {
    return "lower(" + val + ")";
  }

  private static String toHiveField(OccurrenceSearchParameter param, boolean matchCase) {
    if (PARAM_TO_TERM.containsKey(param)) {
      String hiveCol = HiveColumnsUtils.getHiveColumn(PARAM_TO_TERM.get(param));
      if (String.class.isAssignableFrom(param.type()) && OccurrenceSearchParameter.GEOMETRY != param && !matchCase) {
        return toHiveLower(hiveCol);
      }
      return hiveCol;
    }
    // QueryBuildingException requires an underlying exception
    throw new IllegalArgumentException("Search parameter " + param + " is not mapped to Hive");
  }

  /**
   * Converts a value to the form expected by Hive based on the OccurrenceSearchParameter.
   * Most values pass by unaltered. Quotes are added for values that need to be quoted, escaping any existing quotes.
   *
   * @param param the type of parameter defining the expected type
   * @param value the original query value
   *
   * @return the converted value expected by Hive
   */
  private static String toHiveValue(OccurrenceSearchParameter param, String value, boolean matchCase) {
    if (Enum.class.isAssignableFrom(param.type())) {
      // all enum parameters are uppercase
      return '\'' + value.toUpperCase() + '\'';
    }

    if (Date.class.isAssignableFrom(param.type())) {
      // use longs for timestamps expressed as ISO dates
      Date d = IsoDateParsingUtils.parseDate(value);
      return String.valueOf(d.getTime());

    } else if (Number.class.isAssignableFrom(param.type()) || Boolean.class.isAssignableFrom(param.type())) {
      // do not quote numbers
      return value;

    } else {
      // quote value, escape existing quotes
      String strVal =  '\'' + APOSTROPHE_MATCHER.replaceFrom(value, "\\\'") + '\'';
      if (String.class.isAssignableFrom(param.type()) && OccurrenceSearchParameter.GEOMETRY != param && !matchCase) {
        return toHiveLower(strVal);
      }
      return strVal;
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
    // See if this disjunction can be simplified into an IN predicate, which is much faster.
    // We could overcomplicate this:
    //   A=1 OR A=2 OR B=3 OR B=4 OR C>5 → A IN(1,2) OR B IN (3,4) OR C>5
    // but that's a very unusual query for us, so we just check for
    // - EqualsPredicates everywhere
    // - on the same search parameter.

    boolean useIn = true;
    List<String> values = new ArrayList<>();
    OccurrenceSearchParameter parameter = null;

    for (Predicate subPredicate : predicate.getPredicates()) {
      if (subPredicate instanceof EqualsPredicate) {
        EqualsPredicate equalsSubPredicate = (EqualsPredicate) subPredicate;
        if (parameter == null) {
          parameter = equalsSubPredicate.getKey();
        } else if (parameter != equalsSubPredicate.getKey()) {
          useIn = false;
          break;
        }
        values.add(equalsSubPredicate.getValue());
      } else {
        useIn = false;
        break;
      }
    }

    if (useIn) {
      visit(new InPredicate(parameter, values, false));
    } else {
      visitCompoundPredicate(predicate, DISJUNCTION_OPERATOR);
    }
  }

  /**
   * Supports all parameters incl taxonKey expansion for higher taxa.
   */
  public void visit(EqualsPredicate predicate) throws QueryBuildingException {
    if (OccurrenceSearchParameter.TAXON_KEY == predicate.getKey()) {
      appendTaxonKeyFilter(predicate.getValue());
    } else if (OccurrenceSearchParameter.GADM_GID == predicate.getKey()) {
      appendGadmGidFilter(predicate.getValue());
    } else if (OccurrenceSearchParameter.MEDIA_TYPE == predicate.getKey()) {
      Optional.ofNullable(VocabularyUtils.lookupEnum(predicate.getValue(), MediaType.class))
        .ifPresent(mediaType -> builder.append(String.format(ARRAY_FN.apply(GbifTerm.mediaType), mediaType.name())));
    } else if (OccurrenceSearchParameter.ISSUE == predicate.getKey()) {
      builder.append(String.format(ARRAY_FN.apply(GbifTerm.issue), predicate.getValue().toUpperCase()));
    } else if (OccurrenceSearchParameter.NETWORK_KEY == predicate.getKey()) {
      builder.append(String.format(ARRAY_FN.apply(GbifInternalTerm.networkKey), predicate.getValue()));
    } else if (OccurrenceSearchParameter.IDENTIFIED_BY_ID == predicate.getKey()) {
      builder.append(String.format(ARRAY_FN.apply(GbifTerm.identifiedByID), predicate.getValue()));
    } else if (OccurrenceSearchParameter.RECORDED_BY_ID == predicate.getKey()) {
      builder.append(String.format(ARRAY_FN.apply(GbifTerm.recordedByID), predicate.getValue()));
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

  /*
   * For large disjunctions, IN predicates are around 3× faster than a perfectly balanced tree of
   * OR predicates, and around 2× faster than a fairly flat OR query.
   *
   * With Hive 1.3.0, balancing OR queries should be internal to Hive:
   *   https://jira.apache.org/jira/browse/HIVE-11398
   * but it is probably still better to use an IN, which uses a hash table lookup internally:
   *   https://jira.apache.org/jira/browse/HIVE-11415#comment-14651085
   */
  public void visit(InPredicate predicate) throws QueryBuildingException {

    boolean isMatchCase = Optional.ofNullable(predicate.isMatchCase()).orElse(Boolean.FALSE);

    if (isHiveArray(predicate.getKey())) {
      // Array values must be converted to ORs.
      builder.append('(');
      Iterator<String> iterator = predicate.getValues().iterator();
      while (iterator.hasNext()) {
        // Use the equals predicate to get the behaviour for array.
        visit(new EqualsPredicate(predicate.getKey(), iterator.next(), isMatchCase));
        if (iterator.hasNext()) {
          builder.append(DISJUNCTION_OPERATOR);
        }
      }
      builder.append(')');

    } else if (OccurrenceSearchParameter.TAXON_KEY == predicate.getKey()) {
      // Taxon keys must be expanded into a disjunction of in predicates
      appendTaxonKeyFilter(predicate.getValues());

    } else if (OccurrenceSearchParameter.GADM_GID == predicate.getKey()) {
      // GADM GIDs must be expanded into a disjunction of in predicates
      appendGadmGidFilter(predicate.getValues());

    } else {
      builder.append('(');
      builder.append(toHiveField(predicate.getKey(), isMatchCase));
      builder.append(IN_OPERATOR);
      builder.append('(');
      Iterator<String> iterator = predicate.getValues().iterator();
      while (iterator.hasNext()) {
        builder.append(toHiveValue(predicate.getKey(), iterator.next(), isMatchCase));
        if (iterator.hasNext()) {
          builder.append(", ");
        }
      }
      builder.append("))");
    }
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
      builder.append(String.format(IS_NOT_NULL_ARRAY_OPERATOR, toHiveField(predicate.getParameter(), false)));
    } else {
      builder.append(toHiveField(predicate.getParameter(), false));
      builder.append(IS_NOT_NULL_OPERATOR);
    }
  }

  public void visit(WithinPredicate within) throws QueryBuildingException {
    JtsSpatialContextFactory spatialContextFactory = new JtsSpatialContextFactory();
    spatialContextFactory.normWrapLongitude = true;
    spatialContextFactory.srid = 4326;
    spatialContextFactory.datelineRule = DatelineRule.ccwRect;

    WKTReader reader = new WKTReader(spatialContextFactory.newSpatialContext(), spatialContextFactory);

    try {
      // the geometry must be valid - it was validated in the predicates constructor
      Shape geometry = reader.parse(within.getGeometry());

      builder.append('(');
      String withinGeometry;

      // Add an additional filter to a bounding box around any shapes that aren't quadrilaterals, to speed up the query.
      if (geometry instanceof JtsGeometry && ((JtsGeometry) geometry).getGeom().getNumPoints() != 5) {
        // Use the Spatial4J-fixed geometry; this is split into a multipolygon if it crosses the antimeridian.
        withinGeometry = ((JtsGeometry) geometry).getGeom().toText();

        Rectangle bounds = geometry.getBoundingBox();
        boundingBox(bounds);
        builder.append(CONJUNCTION_OPERATOR);
      } else {
        withinGeometry = within.getGeometry();
      }
      builder.append("contains(\"");
      builder.append(withinGeometry);
      builder.append("\", ");
      builder.append(HiveColumnsUtils.getHiveColumn(DwcTerm.decimalLatitude));
      builder.append(", ");
      builder.append(HiveColumnsUtils.getHiveColumn(DwcTerm.decimalLongitude));
      // Without the "= TRUE", the expression may evaluate to TRUE or FALSE for all records, depending
      // on the data format (ORC, Avro, Parquet, text) of the table (!).
      // We could not reproduce the issue on our test cluster, so it seems safest to include this.
      builder.append(") = TRUE");

      builder.append(')');
    } catch (Exception e) {
      throw new QueryBuildingException(e);
    }
  }

  /**
   * Given a bounding box, generates greater than / lesser than queries using decimalLatitude and
   * decimalLongitude to form a bounding box.
   */
  private void boundingBox(Rectangle bounds) {
    builder.append('(');

    // Latitude is easy:
    builder.append(HiveColumnsUtils.getHiveColumn(DwcTerm.decimalLatitude));
    builder.append(GREATER_THAN_EQUALS_OPERATOR);
    builder.append(bounds.getMinY());
    builder.append(CONJUNCTION_OPERATOR);
    builder.append(HiveColumnsUtils.getHiveColumn(DwcTerm.decimalLatitude));
    builder.append(LESS_THAN_EQUALS_OPERATOR);
    builder.append(bounds.getMaxY());

    builder.append(CONJUNCTION_OPERATOR);

    // Longitude must take account of crossing the antimeridian:
    if (bounds.getMinX() < bounds.getMaxX()) {
      builder.append(HiveColumnsUtils.getHiveColumn(DwcTerm.decimalLongitude));
      builder.append(GREATER_THAN_EQUALS_OPERATOR);
      builder.append(bounds.getMinX());
      builder.append(CONJUNCTION_OPERATOR);
      builder.append(HiveColumnsUtils.getHiveColumn(DwcTerm.decimalLongitude));
      builder.append(LESS_THAN_EQUALS_OPERATOR);
      builder.append(bounds.getMaxX());
    } else {
      builder.append('(');
      builder.append(HiveColumnsUtils.getHiveColumn(DwcTerm.decimalLongitude));
      builder.append(GREATER_THAN_EQUALS_OPERATOR);
      builder.append(bounds.getMinX());
      builder.append(DISJUNCTION_OPERATOR);
      builder.append(HiveColumnsUtils.getHiveColumn(DwcTerm.decimalLongitude));
      builder.append(LESS_THAN_EQUALS_OPERATOR);
      builder.append(bounds.getMaxX());
      builder.append(')');
    }

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
    builder.append(toHiveField(predicate.getKey(), predicate.isMatchCase()));
    builder.append(op);
    builder.append(toHiveValue(predicate.getKey(), predicate.getValue(), predicate.isMatchCase()));
  }

  /**
   * Determines if the type of a parameter is a Hive array.
   */
  private static boolean isHiveArray(OccurrenceSearchParameter parameter) {
    return HiveColumnsUtils.getHiveType(PARAM_TO_TERM.get(parameter)).startsWith(HIVE_ARRAY_PRE);
  }

  /**
   * Searches any of the NUB keys in Hive of any rank.
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
   * Searches every level of GADM GID in Hive.
   *
   * @param gadmGid to append as filter
   */
  private void appendGadmGidFilter(String gadmGid) {
    builder.append('(');
    boolean first = true;
    for (Term term : GADM_GIDS) {
      if (!first) {
        builder.append(DISJUNCTION_OPERATOR);
      }
      builder.append(HiveColumnsUtils.getHiveColumn(term));
      builder.append(EQUALS_OPERATOR);
      // Hardcoded GADM_LEVEL_0_GID since the type of all these parameters is the same.
      // Using .toUpperCase() is safe, GIDs must be ASCII anyway.
      builder.append(toHiveValue(OccurrenceSearchParameter.GADM_LEVEL_0_GID, gadmGid.toUpperCase(), true));
      builder.append(gadmGid);
      first = false;
    }
    builder.append(')');
  }

  /**
   * Searches any of the NUB keys in Hive of any rank, for multiple keys.
   *
   * @param taxonKeys to append as filter
   */
  private void appendTaxonKeyFilter(Collection<String> taxonKeys) {
    builder.append('(');
    boolean first = true;
    for (Term term : NUB_KEYS) {
      if (!first) {
        builder.append(DISJUNCTION_OPERATOR);
      }
      builder.append(HiveColumnsUtils.getHiveColumn(term));
      builder.append(IN_OPERATOR);
      builder.append('(');
      builder.append(commaJoiner.join(taxonKeys));
      builder.append(')');
      first = false;
    }
    builder.append(')');
  }

  /**
   * Searches every level of GADM GID in Hive for multiple keys.
   *
   * @param gadmGids to append as filter
   */
  private void appendGadmGidFilter(Collection<String> gadmGids) {
    builder.append('(');
    boolean first = true;
    for (Term term : GADM_GIDS) {
      if (!first) {
        builder.append(DISJUNCTION_OPERATOR);
      }
      builder.append(HiveColumnsUtils.getHiveColumn(term));
      builder.append(IN_OPERATOR);
      builder.append('(');
      Iterator<String> iterator = gadmGids.iterator();
      while (iterator.hasNext()) {
        // Hardcoded GADM_LEVEL_0_GID since the type of all these parameters is the same.
        // Using .toUpperCase() is safe, GIDs must be ASCII anyway.
        builder.append(toHiveValue(OccurrenceSearchParameter.GADM_LEVEL_0_GID, iterator.next().toUpperCase(), true));
        if (iterator.hasNext()) {
          builder.append(", ");
        }
      }
      builder.append(")");
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
  private static CompoundPredicate toDatePredicateQuery(OccurrenceSearchParameter key, String value,
                                                 IsoDateFormat dateFormat) {
    Date lowerDate = IsoDateParsingUtils.parseDate(value);
    return toDateRangePredicate(Range.closed(lowerDate, IsoDateParsingUtils.toLastDayOf(lowerDate, dateFormat)), key);
  }

  /**
   * Converts date range into a conjunction predicate with the form: field >= range.lower AND field <= range.upper.
   */
  private static CompoundPredicate toDateRangePredicate(Range<Date> range, OccurrenceSearchParameter key) {
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
      LOG.warn("Visit method could not be found. That means a unknown Predicate has been passed", e);
      throw new IllegalArgumentException("Unknown Predicate", e);
    }
    try {
      method.invoke(this, object);
    } catch (IllegalAccessException e) {
      LOG.error("This error shouldn't occur if all visit methods are public. Probably a programming error", e);
      Throwables.propagate(e);
    } catch (InvocationTargetException e) {
      LOG.info("Exception thrown while building the query", e);
      throw new QueryBuildingException(e);
    }
  }
}
