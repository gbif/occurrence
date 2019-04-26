package org.gbif.occurrence.search.es;

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.vocabulary.*;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.http.entity.ContentType;
import org.apache.http.protocol.HTTP;
import org.elasticsearch.client.RequestOptions;

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
  public static final String SHOULD = "should";
  public static final String RANGE = "range";
  public static final String GTE = "gte";
  public static final String LTE = "lte";
  public static final String VALUE = "value";
  public static final String POST_FILTER = "post_filter";
  public static final String SUGGEST = "suggest";

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

  private static final DateTimeFormatter FORMATTER =
      DateTimeFormatter.ofPattern(
              "[yyyy-MM-dd'T'HH:mm:ss.SSS XXX][yyyy-MM-dd'T'HH:mm:ss.SSSXXX][yyyy-MM-dd'T'HH:mm:ss.SSS]"
                  + "[yyyy-MM-dd'T'HH:mm:ss][yyyy-MM-dd'T'HH:mm:ss XXX][yyyy-MM-dd'T'HH:mm:ssXXX][yyyy-MM-dd'T'HH:mm:ss]"
                  + "[yyyy-MM-dd'T'HH:mm][yyyy-MM-dd][yyyy-MM][yyyy]")
          .withZone(ZoneId.of("UTC"));

  static final Function<String, Date> STRING_TO_DATE =
      dateAsString -> {
        if (Strings.isNullOrEmpty(dateAsString)) {
          return null;
        }

        // parse string
        TemporalAccessor temporalAccessor = FORMATTER.parse(dateAsString);

        // create local date time
        LocalDateTime localDateTime =
            LocalDateTime.now().withYear(temporalAccessor.get(ChronoField.YEAR));

        if (temporalAccessor.isSupported(ChronoField.MONTH_OF_YEAR)) {
          localDateTime = localDateTime.withMonth(temporalAccessor.get(ChronoField.MONTH_OF_YEAR));
        }
        if (temporalAccessor.isSupported(ChronoField.DAY_OF_MONTH)) {
          localDateTime =
              localDateTime.withDayOfMonth(temporalAccessor.get(ChronoField.DAY_OF_MONTH));
        }
        if (temporalAccessor.isSupported(ChronoField.HOUR_OF_DAY)) {
          localDateTime = localDateTime.withHour(temporalAccessor.get(ChronoField.HOUR_OF_DAY));
        }
        if (temporalAccessor.isSupported(ChronoField.MINUTE_OF_HOUR)) {
          localDateTime =
              localDateTime.withMinute(temporalAccessor.get(ChronoField.MINUTE_OF_HOUR));
        }
        if (temporalAccessor.isSupported(ChronoField.SECOND_OF_MINUTE)) {
          localDateTime =
              localDateTime.withSecond(temporalAccessor.get(ChronoField.SECOND_OF_MINUTE));
        }
        if (temporalAccessor.isSupported(ChronoField.NANO_OF_SECOND)) {
          localDateTime = localDateTime.withNano(temporalAccessor.get(ChronoField.NANO_OF_SECOND));
        }

        return Date.from(localDateTime.toInstant(ZoneOffset.UTC));
      };

  // functions
  public static final Supplier<RequestOptions> HEADERS =
      () -> {
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.addHeader(HTTP.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString());
        return builder.build();
      };

  public static final ImmutableMap<OccurrenceSearchParameter, OccurrenceEsField> SEARCH_TO_ES_MAPPING =
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
          .put(
              OccurrenceSearchParameter.HAS_GEOSPATIAL_ISSUE,
              OccurrenceEsField.HAS_GEOSPATIAL_ISSUES)
          .put(OccurrenceSearchParameter.HAS_COORDINATE, OccurrenceEsField.HAS_COORDINATE)
          .put(OccurrenceSearchParameter.EVENT_DATE, OccurrenceEsField.EVENT_DATE)
          .put(OccurrenceSearchParameter.LAST_INTERPRETED, OccurrenceEsField.LAST_INTERPRETED)
          .put(OccurrenceSearchParameter.COUNTRY, OccurrenceEsField.COUNTRY_CODE)
          .put(OccurrenceSearchParameter.PUBLISHING_COUNTRY, OccurrenceEsField.PUBLISHING_COUNTRY)
          .put(OccurrenceSearchParameter.CONTINENT, OccurrenceEsField.CONTINENT)
          .put(OccurrenceSearchParameter.TAXON_KEY, OccurrenceEsField.TAXON_KEY)
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

  static final Map<OccurrenceEsField, Integer> CARDINALITIES =
      ImmutableMap.<OccurrenceEsField, Integer>builder()
          .put(OccurrenceEsField.BASIS_OF_RECORD, BasisOfRecord.values().length)
          .put(OccurrenceEsField.COUNTRY_CODE, Country.values().length)
          .put(OccurrenceEsField.PUBLISHING_COUNTRY, Country.values().length)
          .put(OccurrenceEsField.CONTINENT, Continent.values().length)
          .put(OccurrenceEsField.ESTABLISHMENT_MEANS, EstablishmentMeans.values().length)
          .put(OccurrenceEsField.ISSUE, OccurrenceIssue.values().length)
          .put(OccurrenceEsField.LICENSE, License.values().length)
          .put(OccurrenceEsField.MEDIA_TYPE, MediaType.values().length)
          .put(OccurrenceEsField.TYPE_STATUS, TypeStatus.values().length)
          .put(OccurrenceEsField.MONTH, 12)
          .build();

  static final Map<String, OccurrenceSearchParameter> ES_TO_SEARCH_MAPPING =
      new HashMap<>(SEARCH_TO_ES_MAPPING.size());

  static {
    for (Map.Entry<OccurrenceSearchParameter, OccurrenceEsField> paramField :
        SEARCH_TO_ES_MAPPING.entrySet()) {
      ES_TO_SEARCH_MAPPING.put(paramField.getValue().getFieldName(), paramField.getKey());
    }
  }
}
