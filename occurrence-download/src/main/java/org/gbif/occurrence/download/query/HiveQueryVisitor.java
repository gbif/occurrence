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
package org.gbif.occurrence.download.query;

import org.gbif.api.model.occurrence.predicate.*;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.util.IsoDateParsingUtils;
import org.gbif.api.util.Range;
import org.gbif.api.util.SearchTypeValidator;
import org.gbif.api.util.VocabularyUtils;
import org.gbif.api.vocabulary.MediaType;
import org.gbif.api.vocabulary.TypeStatus;
import org.gbif.dwc.terms.*;
import org.gbif.occurrence.common.HiveColumnsUtils;
import org.gbif.occurrence.common.TermUtils;
import org.gbif.occurrence.search.es.query.QueryBuildingException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.spatial4j.context.jts.DatelineRule;
import org.locationtech.spatial4j.context.jts.JtsSpatialContextFactory;
import org.locationtech.spatial4j.io.WKTReader;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.impl.RectangleImpl;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;
import org.opengis.feature.type.GeometryType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.api.util.IsoDateParsingUtils.ISO_DATE_FORMATTER;

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
  private static final String IS_NULL_OPERATOR = " IS NULL ";
  private static final String IS_NOT_NULL_ARRAY_OPERATOR = "(%1$s IS NOT NULL AND size(%1$s) > 0)";
  private static final CharMatcher APOSTROPHE_MATCHER = CharMatcher.is('\'');
  // where query to execute a select all
  private static final String ALL_QUERY = "true";

  private static final Function<Term, String> ARRAY_FN = t ->
    "stringArrayContains(" + HiveColumnsUtils.getHiveQueryColumn(t) + ",'%s',%b)";

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
      .put(OccurrenceSearchParameter.DEGREE_OF_ESTABLISHMENT, DwcTerm.degreeOfEstablishment)
      .put(OccurrenceSearchParameter.PATHWAY, DwcTerm.pathway)
      // the following need some value transformation
      .put(OccurrenceSearchParameter.EVENT_DATE, DwcTerm.eventDate)
      .put(OccurrenceSearchParameter.MODIFIED, DcTerm.modified)
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
      .put(OccurrenceSearchParameter.HOSTING_ORGANIZATION_KEY, GbifInternalTerm.hostingOrganizationKey)
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
      .put(OccurrenceSearchParameter.RECORDED_BY_ID, DwcTerm.recordedByID)
      .put(OccurrenceSearchParameter.IDENTIFIED_BY_ID, DwcTerm.identifiedByID)
      .put(OccurrenceSearchParameter.OCCURRENCE_STATUS, DwcTerm.occurrenceStatus)
      .put(OccurrenceSearchParameter.LIFE_STAGE, DwcTerm.lifeStage)
      .put(OccurrenceSearchParameter.IS_IN_CLUSTER, GbifInternalTerm.isInCluster)
      .put(OccurrenceSearchParameter.DWCA_EXTENSION, GbifInternalTerm.dwcaExtension)
      .put(OccurrenceSearchParameter.IUCN_RED_LIST_CATEGORY, IucnTerm.iucnRedListCategory)
      .put(OccurrenceSearchParameter.DATASET_ID, DwcTerm.datasetID)
      .put(OccurrenceSearchParameter.DATASET_NAME, DwcTerm.datasetName)
      .put(OccurrenceSearchParameter.OTHER_CATALOG_NUMBERS, DwcTerm.otherCatalogNumbers)
      .put(OccurrenceSearchParameter.PREPARATIONS, DwcTerm.preparations)
      .build();

  private static final Map<OccurrenceSearchParameter, Term> ARRAY_STRING_TERMS =
    ImmutableMap.<OccurrenceSearchParameter, Term>builder()
      .put(OccurrenceSearchParameter.NETWORK_KEY,GbifInternalTerm.networkKey)
      .put(OccurrenceSearchParameter.DWCA_EXTENSION,GbifInternalTerm.dwcaExtension)
      .put(OccurrenceSearchParameter.IDENTIFIED_BY_ID,DwcTerm.identifiedByID)
      .put(OccurrenceSearchParameter.RECORDED_BY_ID,DwcTerm.recordedByID)
      .put(OccurrenceSearchParameter.DATASET_ID,DwcTerm.datasetID)
      .put(OccurrenceSearchParameter.DATASET_NAME,DwcTerm.datasetName)
      .put(OccurrenceSearchParameter.OTHER_CATALOG_NUMBERS,DwcTerm.otherCatalogNumbers)
      .put(OccurrenceSearchParameter.RECORDED_BY,DwcTerm.recordedBy)
      .put(OccurrenceSearchParameter.IDENTIFIED_BY,DwcTerm.identifiedBy)
      .put(OccurrenceSearchParameter.PREPARATIONS,DwcTerm.preparations)
      .put(OccurrenceSearchParameter.SAMPLING_PROTOCOL,DwcTerm.samplingProtocol).build();

  private final Joiner commaJoiner = Joiner.on(", ").skipNulls();

  private StringBuilder builder;

  /**
   * Transforms the value to the Hive statement lower(val).
   */
  private static String toHiveLower(String val) {
    return "lower(" + val + ")";
  }

  private static String toHiveField(OccurrenceSearchParameter param, boolean matchCase) {
    return  Optional.ofNullable(term(param)).map( term -> {
              String hiveCol = HiveColumnsUtils.getHiveQueryColumn(term(param));
              if (String.class.isAssignableFrom(param.type()) && OccurrenceSearchParameter.GEOMETRY != param && !matchCase) {
                return toHiveLower(hiveCol);
              }
              return hiveCol;
             }).orElseThrow(() ->
             // QueryBuildingException requires an underlying exception
             new IllegalArgumentException("Search parameter " + param + " is not mapped to Hive"));
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
      LocalDate ld = IsoDateParsingUtils.parseDate(value);
      Instant i = ld.atStartOfDay(ZoneOffset.UTC).toInstant();
      return String.valueOf(i.toEpochMilli());

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
        .ifPresent(mediaType -> builder.append(String.format(ARRAY_FN.apply(GbifTerm.mediaType), mediaType.name(), true)));
    } else if (OccurrenceSearchParameter.TYPE_STATUS == predicate.getKey()) {
      Optional.ofNullable(VocabularyUtils.lookupEnum(predicate.getValue(), TypeStatus.class))
        .ifPresent(typeStatus -> builder.append(String.format(ARRAY_FN.apply(DwcTerm.typeStatus), typeStatus.name(), true)));
    } else if (OccurrenceSearchParameter.ISSUE == predicate.getKey()) {
      builder.append(String.format(ARRAY_FN.apply(GbifTerm.issue), predicate.getValue().toUpperCase(), true));
    } else if (ARRAY_STRING_TERMS.containsKey(predicate.getKey())) {
      builder.append(String.format(ARRAY_FN.apply(ARRAY_STRING_TERMS.get(predicate.getKey())), predicate.getValue(), predicate.isMatchCase()));
    } else if (TermUtils.isVocabulary(term(predicate.getKey()))) {
      builder.append(String.format(ARRAY_FN.apply(term(predicate.getKey())), predicate.getValue(), true));
    } else if (Date.class.isAssignableFrom(predicate.getKey().type())) {
      // Dates may contain a range even for an EqualsPredicate (e.g. "2000" or "2000-02")
      // The user's query value is inclusive, but the parsed dateRange is exclusive of the
      // upperBound to allow including the day itself.
      //
      // I.e. a predicate value 2000/2005-03 gives a dateRange [2000-01-01,2005-04-01)
      Range<LocalDate> dateRange = IsoDateParsingUtils.parseDateRange(predicate.getValue());

      if (dateRange.hasLowerBound() || dateRange.hasUpperBound()) {
        builder.append('(');
        if (dateRange.hasLowerBound()) {
          visitSimplePredicate(predicate, GREATER_THAN_EQUALS_OPERATOR, ISO_DATE_FORMATTER.format(dateRange.lowerEndpoint()));
          if (dateRange.hasUpperBound()) {
            builder.append(CONJUNCTION_OPERATOR);
          }
        }
        if (dateRange.hasUpperBound()) {
          visitSimplePredicate(predicate, LESS_THAN_OPERATOR, ISO_DATE_FORMATTER.format(dateRange.upperEndpoint()));
        }
        builder.append(')');
      }
    } else {
      visitSimplePredicate(predicate, EQUALS_OPERATOR);
    }
  }

  public void visit(GreaterThanOrEqualsPredicate predicate) throws QueryBuildingException {
    if (Date.class.isAssignableFrom(predicate.getKey().type())) {
      // Where the date is a range, consider the "OrEquals" to mean including the whole range.
      // "2000" includes all of 2000.
      Range<LocalDate> dateRange = IsoDateParsingUtils.parseDateRange(predicate.getValue());
      visitSimplePredicate(predicate, GREATER_THAN_EQUALS_OPERATOR, ISO_DATE_FORMATTER.format(dateRange.lowerEndpoint()));
    } else {
      visitSimplePredicate(predicate, GREATER_THAN_EQUALS_OPERATOR);
    }
  }

  public void visit(GreaterThanPredicate predicate) throws QueryBuildingException {
    if (Date.class.isAssignableFrom(predicate.getKey().type())) {
      // Where the date is a range, consider the lack of "OrEquals" to mean excluding the whole range.
      // "2000" excludes all of 2000, so the earliest date is 2001-01-01.
      Range<LocalDate> dateRange = IsoDateParsingUtils.parseDateRange(predicate.getValue());
      visitSimplePredicate(predicate, GREATER_THAN_EQUALS_OPERATOR, ISO_DATE_FORMATTER.format(dateRange.upperEndpoint()));
    } else {
      visitSimplePredicate(predicate, GREATER_THAN_OPERATOR);
    }
  }

  public void visit(LessThanOrEqualsPredicate predicate) throws QueryBuildingException {
    if (Date.class.isAssignableFrom(predicate.getKey().type())) {
      // Where the date is a range, consider the "OrEquals" to mean including the whole range.
      // "2000" includes all of 2000, so the latest date is 2001-01-01 (not inclusive).
      Range<LocalDate> dateRange = IsoDateParsingUtils.parseDateRange(predicate.getValue());
      visitSimplePredicate(predicate, LESS_THAN_OPERATOR, ISO_DATE_FORMATTER.format(dateRange.upperEndpoint()));
    } else {
      visitSimplePredicate(predicate, LESS_THAN_EQUALS_OPERATOR);
    }
  }

  public void visit(LessThanPredicate predicate) throws QueryBuildingException {
    if (Date.class.isAssignableFrom(predicate.getKey().type())) {
      // Where the date is a range, consider the lack of "OrEquals" to mean excluding the whole range.
      // "2000" excludes all of 2000, so the latest date is 2000-01-01 (not inclusive).
      Range<LocalDate> dateRange = IsoDateParsingUtils.parseDateRange(predicate.getValue());
      visitSimplePredicate(predicate, LESS_THAN_OPERATOR, ISO_DATE_FORMATTER.format(dateRange.lowerEndpoint()));
    } else {
      visitSimplePredicate(predicate, LESS_THAN_OPERATOR);
    }
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

  public void visit(LikePredicate predicate) throws QueryBuildingException {
    // Replace % → \% and _ → \_
    // Then replace * → % and ? → _
    LikePredicate likePredicate = new LikePredicate(
      predicate.getKey(),
      predicate.getValue()
        .replace("%", "\\%")
        .replace("_", "\\_")
        .replace('*', '%')
        .replace('?', '_'),
      predicate.isMatchCase());

    visitSimplePredicate(likePredicate, LIKE_OPERATOR);
  }

  public void visit(NotPredicate predicate) throws QueryBuildingException {
    builder.append(NOT_OPERATOR);
    visit(predicate.getPredicate());
  }

  public void visit(IsNotNullPredicate predicate) throws QueryBuildingException {
    if (isHiveArray(predicate.getParameter())) {
      builder.append(String.format(IS_NOT_NULL_ARRAY_OPERATOR, toHiveField(predicate.getParameter(), true)));
    } else if (OccurrenceSearchParameter.TAXON_KEY == predicate.getParameter()) {
      appendTaxonKeyUnary(IS_NOT_NULL_OPERATOR);
    } else {
      // matchCase: Avoid adding an unnecessary "lower()" when just testing for null.
      builder.append(toHiveField(predicate.getParameter(), true));
      builder.append(IS_NOT_NULL_OPERATOR);
    }
  }

  public void visit(IsNullPredicate predicate) throws QueryBuildingException {
    if (OccurrenceSearchParameter.TAXON_KEY == predicate.getParameter()) {
      appendTaxonKeyUnary(IS_NULL_OPERATOR);
    } else {
      // matchCase: Avoid adding an unnecessary "lower()" when just testing for null.
      builder.append(toHiveField(predicate.getParameter(), true));
      builder.append(IS_NULL_OPERATOR);
    }
  }

  /**
   * Searches any of the NUB keys in Hive of any rank.
   *
   * @param unaryOperator to append as filter
   */
  private void appendTaxonKeyUnary(String unaryOperator) {
    builder.append('(');
    builder.append(NUB_KEYS.stream()
                     .map(term -> HiveColumnsUtils.getHiveQueryColumn(term) + unaryOperator)
                     .collect(Collectors.joining(CONJUNCTION_OPERATOR)));
    builder.append(')');
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
        Geometry g = ((JtsGeometry) geometry).getGeom();
        // Use the Spatial4J-fixed geometry; this is split into a multipolygon if it crosses the antimeridian.
        withinGeometry = g.toText();

        Rectangle bounds = geometry.getBoundingBox();
        boundingBox(bounds);
        builder.append(CONJUNCTION_OPERATOR);

        // A tool (R?) can generate hundreds of tiny areas spread across the globe, all in a single multipolygon.
        // Add bounding boxes for these too.
        // Example: https://www.gbif.org/occurrence/download/0187894-210914110416597
        if (g instanceof MultiPolygon && g.getNumGeometries() > 2) {
          builder.append("((");
          for (int i = 0; i < g.getNumGeometries(); i++) {
            if (i > 0) {
              // Too many clauses exceeds Hive's query parsing stack.
              if (i % 500 == 0) {
                builder.append(')');
                builder.append(DISJUNCTION_OPERATOR);
                builder.append('(');
              } else {
                builder.append(DISJUNCTION_OPERATOR);
              }
            }
            Geometry gi = g.getGeometryN(i);
            Envelope env = gi.getEnvelopeInternal();
            boundingBox(new RectangleImpl(env.getMinX(), env.getMaxX(), env.getMinY(), env.getMaxY(), geometry.getContext()));
          }
          builder.append("))");
          builder.append(CONJUNCTION_OPERATOR);
        }
      } else {
        withinGeometry = within.getGeometry();
      }
      builder.append("contains(\"");
      builder.append(withinGeometry);
      builder.append("\", ");
      builder.append(HiveColumnsUtils.getHiveQueryColumn(DwcTerm.decimalLatitude));
      builder.append(", ");
      builder.append(HiveColumnsUtils.getHiveQueryColumn(DwcTerm.decimalLongitude));
      // Without the "= TRUE", the expression may evaluate to TRUE or FALSE for all records, depending
      // on the data format (ORC, Avro, Parquet, text) of the table (!).
      // We could not reproduce the issue on our test cluster, so it seems safest to include this.
      builder.append(") = TRUE");

      builder.append(')');
    } catch (Exception e) {
      throw new QueryBuildingException(e);
    }
  }

  public void visit(GeoDistancePredicate geoDistance) throws QueryBuildingException {
    builder.append("(geoDistance(");
    builder.append(geoDistance.getGeoDistance().toGeoDistanceString());
    builder.append(", ");
    builder.append(HiveColumnsUtils.getHiveQueryColumn(DwcTerm.decimalLatitude));
    builder.append(", ");
    builder.append(HiveColumnsUtils.getHiveQueryColumn(DwcTerm.decimalLongitude));
    builder.append(") = TRUE)");
  }

  /**
   * Given a bounding box, generates greater than / lesser than queries using decimalLatitude and
   * decimalLongitude to form a bounding box.
   */
  private void boundingBox(Rectangle bounds) {
    builder.append('(');

    // Latitude is easy:
    builder.append(HiveColumnsUtils.getHiveQueryColumn(DwcTerm.decimalLatitude));
    builder.append(GREATER_THAN_EQUALS_OPERATOR);
    builder.append(bounds.getMinY());
    builder.append(CONJUNCTION_OPERATOR);
    builder.append(HiveColumnsUtils.getHiveQueryColumn(DwcTerm.decimalLatitude));
    builder.append(LESS_THAN_EQUALS_OPERATOR);
    builder.append(bounds.getMaxY());

    builder.append(CONJUNCTION_OPERATOR);

    // Longitude must take account of crossing the antimeridian:
    builder.append('(');
    builder.append(HiveColumnsUtils.getHiveQueryColumn(DwcTerm.decimalLongitude));
    builder.append(GREATER_THAN_EQUALS_OPERATOR);
    builder.append(bounds.getMinX());
    if (bounds.getMinX() < bounds.getMaxX()) {
      builder.append(CONJUNCTION_OPERATOR);
    } else {
      builder.append(DISJUNCTION_OPERATOR);
    }
    builder.append(HiveColumnsUtils.getHiveQueryColumn(DwcTerm.decimalLongitude));
    builder.append(LESS_THAN_EQUALS_OPERATOR);
    builder.append(bounds.getMaxX());
    builder.append(')');

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
    if (Number.class.isAssignableFrom(predicate.getKey().type())) {
      if (SearchTypeValidator.isRange(predicate.getValue())) {
        visit(toNumberRangePredicate(SearchTypeValidator.parseDecimalRange(predicate.getValue()), predicate.getKey()));
        return;
      }
    }
    builder.append(toHiveField(predicate.getKey(), predicate.isMatchCase()));
    builder.append(op);
    builder.append(toHiveValue(predicate.getKey(), predicate.getValue(), predicate.isMatchCase()));
  }

  public void visitSimplePredicate(SimplePredicate predicate, String op, String value) {
    builder.append(toHiveField(predicate.getKey(), predicate.isMatchCase()));
    builder.append(op);
    builder.append(toHiveValue(predicate.getKey(), value, predicate.isMatchCase()));
  }

  /**
   * Determines if the parameter type is a Hive array.
   */
  private static boolean isHiveArray(OccurrenceSearchParameter parameter) {
    return HiveColumnsUtils.getHiveType(term(parameter)).startsWith(HIVE_ARRAY_PRE);
  }

  /** Term associated to a search parameter */
  public static Term term(OccurrenceSearchParameter parameter) {
    return PARAM_TO_TERM.get(parameter);
  }

  /**
   * Searches any of the NUB keys in Hive of any rank.
   *
   * @param taxonKey to append as filter
   */
  private void appendTaxonKeyFilter(String taxonKey) {
    builder.append('(');
    builder.append(NUB_KEYS.stream()
                     .map(term -> HiveColumnsUtils.getHiveQueryColumn(term) + EQUALS_OPERATOR + taxonKey)
                     .collect(Collectors.joining(DISJUNCTION_OPERATOR)));
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
      builder.append(HiveColumnsUtils.getHiveQueryColumn(term));
      builder.append(EQUALS_OPERATOR);
      // Hardcoded GADM_LEVEL_0_GID since the type of all these parameters is the same.
      // Using .toUpperCase() is safe, GIDs must be ASCII anyway.
      builder.append(toHiveValue(OccurrenceSearchParameter.GADM_LEVEL_0_GID, gadmGid.toUpperCase(), true));
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
      builder.append(HiveColumnsUtils.getHiveQueryColumn(term));
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
      builder.append(HiveColumnsUtils.getHiveQueryColumn(term));
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
   * Converts decimal range into a predicate with the form: field >= range.lower AND field <= range.upper.
   */
  private static Predicate toNumberRangePredicate(Range<Double> range, OccurrenceSearchParameter key) {
    if (!range.hasLowerBound()) {
      return new LessThanOrEqualsPredicate(key, String.valueOf(range.upperEndpoint().doubleValue()));
    }
    if (!range.hasUpperBound()) {
      return new GreaterThanOrEqualsPredicate(key, String.valueOf(range.lowerEndpoint().doubleValue()));
    }

    ImmutableList<Predicate> predicates = new ImmutableList.Builder<Predicate>()
      .add(new GreaterThanOrEqualsPredicate(key, String.valueOf(range.lowerEndpoint().doubleValue())))
      .add(new LessThanOrEqualsPredicate(key, String.valueOf(range.upperEndpoint().doubleValue())))
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
