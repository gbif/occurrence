package org.gbif.occurrence.search.es;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.http.Header;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HTTP;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Country;

import java.text.DateFormat;
import java.text.ParseException;
import java.time.LocalDate;
import java.util.Date;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public class EsQueryUtils {

  private EsQueryUtils() {}

  // ES fields for queries
  public static final String SIZE = "size";
  public static final String FROM = "from";
  public static final String TO = "to";
  public static final String QUERY = "query";
  public static final String BOOL = "bool";
  public static final String MUST = "must";
  public static final String MATCH = "match";
  public static final String TERM = "term";
  public static final String TERMS = "terms";
  public static final String FILTER = "filter";
  public static final String RANGE = "range";
  public static final String GTE = "gte";
  public static final String LTE = "lte";
  public static final String VALUE = "value";

  // Aggs
  public static final String FIELD = "field";
  public static final String AGGREGATIONS = "aggregations";
  public static final String AGGS = "aggs";
  public static final String PRECISION = "precision";
  public static final String GEOHASH_GRID = "geohash_grid";
  public static final String GEO_BOUNDS = "geo_bounds";
  public static final String GEO_BOUNDING_BOX = "geo_bounding_box";

  // geo_shape
  static final String GEO_SHAPE = "geo_shape";
  static final String COORDINATES = "coordinates";
  static final String TYPE = "type";
  static final String SHAPE = "shape";
  static final String RELATION = "relation";
  static final String WITHIN = "within";

  static final String RANGE_SEPARATOR = ",";

  static final BiFunction<String, DateFormat, Date> DATE_FORMAT =
      (dateAsString, format) -> {
        if (Strings.isNullOrEmpty(dateAsString)) {
          return null;
        }

        try {
          return format.parse(dateAsString);
        } catch (ParseException e) {
          throw new IllegalStateException(e.getMessage(), e);
        }
      };

  // functions
  public static final Function<String, String> SEARCH_ENDPOINT = index -> "/" + index + "/_search";
  public static final Supplier<Header[]> HEADERS =
      () ->
          new Header[] {
            new BasicHeader(HTTP.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString())
          };

  static final ImmutableMap<OccurrenceSearchParameter, OccurrenceEsField> QUERY_FIELD_MAPPING =
      ImmutableMap.<OccurrenceSearchParameter, OccurrenceEsField>builder()
          .put(OccurrenceSearchParameter.DECIMAL_LATITUDE, OccurrenceEsField.LATITUDE)
          .put(OccurrenceSearchParameter.DECIMAL_LONGITUDE, OccurrenceEsField.LONGITUDE)
          .put(OccurrenceSearchParameter.YEAR, OccurrenceEsField.YEAR)
          .put(OccurrenceSearchParameter.MONTH, OccurrenceEsField.MONTH)
          .put(OccurrenceSearchParameter.CATALOG_NUMBER, OccurrenceEsField.CATALOG_NUMBER)
          .put(OccurrenceSearchParameter.RECORDED_BY, OccurrenceEsField.RECORDED_BY)
          .put(OccurrenceSearchParameter.RECORD_NUMBER, OccurrenceEsField.RECORD_NUMBER)
          .put(OccurrenceSearchParameter.COLLECTION_CODE, OccurrenceEsField.COLLECTION_CODE)
          .put(OccurrenceSearchParameter.INSTITUTION_CODE, OccurrenceEsField.INSTITUTION_CODE)
          .put(OccurrenceSearchParameter.DEPTH, OccurrenceEsField.DEPTH)
          .put(OccurrenceSearchParameter.ELEVATION, OccurrenceEsField.ELEVATION)
          .put(OccurrenceSearchParameter.BASIS_OF_RECORD, OccurrenceEsField.BASIS_OF_RECORD)
          .put(OccurrenceSearchParameter.DATASET_KEY, OccurrenceEsField.DATASET_KEY)
          .put(OccurrenceSearchParameter.HAS_GEOSPATIAL_ISSUE, OccurrenceEsField.SPATIAL_ISSUES)
          .put(OccurrenceSearchParameter.HAS_COORDINATE, OccurrenceEsField.HAS_COORDINATE)
          .put(OccurrenceSearchParameter.EVENT_DATE, OccurrenceEsField.EVENT_DATE)
          .put(OccurrenceSearchParameter.LAST_INTERPRETED, OccurrenceEsField.LAST_INTERPRETED)
          .put(OccurrenceSearchParameter.COUNTRY, OccurrenceEsField.COUNTRY_CODE)
          .put(OccurrenceSearchParameter.PUBLISHING_COUNTRY, OccurrenceEsField.PUBLISHING_COUNTRY)
          .put(OccurrenceSearchParameter.CONTINENT, OccurrenceEsField.CONTINENT)
          .put(OccurrenceSearchParameter.TAXON_KEY, OccurrenceEsField.TAXA_KEY)
          .put(OccurrenceSearchParameter.KINGDOM_KEY, OccurrenceEsField.KINGDOM_KEY)
          .put(OccurrenceSearchParameter.PHYLUM_KEY, OccurrenceEsField.PHYLUM_KEY)
          .put(OccurrenceSearchParameter.CLASS_KEY, OccurrenceEsField.CLASS_KEY)
          .put(OccurrenceSearchParameter.ORDER_KEY, OccurrenceEsField.ORDER_KEY)
          .put(OccurrenceSearchParameter.FAMILY_KEY, OccurrenceEsField.FAMILY_KEY)
          .put(OccurrenceSearchParameter.GENUS_KEY, OccurrenceEsField.GENUS_KEY)
          .put(OccurrenceSearchParameter.SUBGENUS_KEY, OccurrenceEsField.SUBGENUS_KEY)
          .put(OccurrenceSearchParameter.SPECIES_KEY, OccurrenceEsField.SPECIES_KEY)
          .put(OccurrenceSearchParameter.SCIENTIFIC_NAME, OccurrenceEsField.SCIENTIFIC_NAME)
          .put(OccurrenceSearchParameter.TYPE_STATUS, OccurrenceEsField.TYPE_STATUS)
          .put(OccurrenceSearchParameter.MEDIA_TYPE, OccurrenceEsField.MEDIA_TYPE)
          .put(OccurrenceSearchParameter.ISSUE, OccurrenceEsField.ISSUE)
          .put(OccurrenceSearchParameter.OCCURRENCE_ID, OccurrenceEsField.OCCURRENCE_ID)
          .put(OccurrenceSearchParameter.ESTABLISHMENT_MEANS, OccurrenceEsField.ESTABLISHMENT_MEANS)
          .put(OccurrenceSearchParameter.REPATRIATED, OccurrenceEsField.REPATRIATED)
          .put(OccurrenceSearchParameter.LOCALITY, OccurrenceEsField.LOCALITY)
          .put(OccurrenceSearchParameter.STATE_PROVINCE, OccurrenceEsField.STATE_PROVINCE)
          .put(OccurrenceSearchParameter.WATER_BODY, OccurrenceEsField.WATER_BODY)
          .put(OccurrenceSearchParameter.LICENSE, OccurrenceEsField.LICENSE)
          .put(OccurrenceSearchParameter.PROTOCOL, OccurrenceEsField.PROTOCOL)
          .put(OccurrenceSearchParameter.ORGANISM_ID, OccurrenceEsField.ORGANISM_ID)
          .put(
              OccurrenceSearchParameter.PUBLISHING_ORG,
              OccurrenceEsField.PUBLISHING_ORGANIZATION_KEY)
          .put(OccurrenceSearchParameter.CRAWL_ID, OccurrenceEsField.CRAWL_ID)
          .put(OccurrenceSearchParameter.INSTALLATION_KEY, OccurrenceEsField.INSTALLATION_KEY)
          .put(OccurrenceSearchParameter.NETWORK_KEY, OccurrenceEsField.NETWORK_KEY)
          .put(OccurrenceSearchParameter.EVENT_ID, OccurrenceEsField.EVENT_ID)
          .put(OccurrenceSearchParameter.PARENT_EVENT_ID, OccurrenceEsField.PARENT_EVENT_ID)
          .put(OccurrenceSearchParameter.SAMPLING_PROTOCOL, OccurrenceEsField.SAMPLING_PROTOCOL)
          .build();

  static final ImmutableMap<OccurrenceEsField, Integer> CARDINALITY_FIELD_MAPPING =
      ImmutableMap.<OccurrenceEsField, Integer>builder()
          .put(OccurrenceEsField.MONTH, 12)
          .put(OccurrenceEsField.DAY, 31)
          .put(OccurrenceEsField.YEAR, LocalDate.now().getYear() - 1000)
          .put(OccurrenceEsField.COUNTRY_CODE, Country.values().length)
          .put(OccurrenceEsField.BASIS_OF_RECORD, BasisOfRecord.values().length)
          .build();
}
