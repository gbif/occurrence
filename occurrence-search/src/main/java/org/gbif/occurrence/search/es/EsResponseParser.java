package org.gbif.occurrence.search.es;

import org.gbif.api.model.common.Identifier;
import org.gbif.api.model.common.paging.Pageable;
import org.gbif.api.model.common.search.Facet;
import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.OccurrenceRelation;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.vocabulary.*;

import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;

import static org.gbif.occurrence.search.es.EsQueryUtils.ES_TO_SEARCH_MAPPING;
import static org.gbif.occurrence.search.es.EsQueryUtils.SEARCH_TO_ES_MAPPING;
import static org.gbif.occurrence.search.es.EsQueryUtils.STRING_TO_DATE;
import static org.gbif.occurrence.search.es.OccurrenceEsField.*;

public class EsResponseParser {

  private static final Pattern NESTED_PATTERN = Pattern.compile("^\\w+(\\.\\w+)+$");
  private static final Predicate<String> IS_NESTED = s -> NESTED_PATTERN.matcher(s).find();
  private static final DateFormat DATE_FORMAT =
      new SimpleDateFormat("yyyy-MM-dd"); // Quoted "Z" to indicate UTC, no timezone offset

  static {
    DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  private EsResponseParser() {}

  /**
   * Builds a SearchResponse instance using the current builder state.
   *
   * @return a new instance of a SearchResponse.
   */
  static SearchResponse<Occurrence, OccurrenceSearchParameter> buildResponse(
      org.elasticsearch.action.search.SearchResponse esResponse, OccurrenceSearchRequest request) {

    SearchResponse<Occurrence, OccurrenceSearchParameter> response = new SearchResponse<>(request);
    response.setCount(esResponse.getHits().getTotalHits());
    parseHits(esResponse).ifPresent(response::setResults);
    parseFacets(esResponse, request).ifPresent(response::setFacets);

    return response;
  }

  static List<String> buildSuggestResponse(
      org.elasticsearch.action.search.SearchResponse esResponse,
      OccurrenceSearchParameter parameter) {

    String fieldName = SEARCH_TO_ES_MAPPING.get(parameter).getFieldName();

    return esResponse.getSuggest().getSuggestion(fieldName + "-suggest").getEntries().stream()
        .flatMap(e -> ((CompletionSuggestion.Entry) e).getOptions().stream())
        .map(o -> ((CompletionSuggestion.Entry.Option) o).getHit())
        .map(hit -> hit.getSourceAsMap().get(fieldName))
        .filter(Objects::nonNull)
        .map(String::valueOf)
        .collect(Collectors.toList());
  }

  private static Optional<List<Facet<OccurrenceSearchParameter>>> parseFacets(
      org.elasticsearch.action.search.SearchResponse esResponse, OccurrenceSearchRequest request) {
    if (esResponse.getAggregations() == null) {
      return Optional.empty();
    }

    return Optional.of(
        esResponse.getAggregations().asList().stream()
            .map(
                aggs -> {
                  // get buckets
                  List<? extends Terms.Bucket> buckets = null;
                  if (aggs instanceof Terms) {
                    buckets = ((Terms) aggs).getBuckets();
                  } else if (aggs instanceof Filter) {
                    buckets =
                        ((Filter) aggs)
                            .getAggregations().asList().stream()
                                .flatMap(agg -> ((Terms) agg).getBuckets().stream())
                                .collect(Collectors.toList());
                  } else {
                    throw new IllegalArgumentException(
                        aggs.getClass() + " aggregation not supported");
                  }

                  // get facet of the agg
                  OccurrenceSearchParameter facet = ES_TO_SEARCH_MAPPING.get(aggs.getName());

                  // check for paging in facets
                  Pageable facetPage = request.getFacetPage(facet);

                  List<Facet.Count> counts =
                      buckets.stream()
                          .skip((int) facetPage.getOffset())
                          .limit(facetPage.getOffset() + facetPage.getLimit())
                          .map(b -> new Facet.Count(b.getKeyAsString(), b.getDocCount()))
                          .collect(Collectors.toList());

                  return new Facet<>(facet, counts);
                })
            .collect(Collectors.toList()));
  }

  private static Optional<List<Occurrence>> parseHits(
      org.elasticsearch.action.search.SearchResponse esResponse) {
    if (esResponse.getHits() == null
        || esResponse.getHits().getHits() == null
        || esResponse.getHits().getHits().length == 0) {
      return Optional.empty();
    }

    List<Occurrence> occurrences = new ArrayList<>();
    esResponse
        .getHits()
        .forEach(
            hit -> {
              // create occurrence
              Occurrence occ = new Occurrence();
              occurrences.add(occ);

              // set fields
              setOccurrenceFields(hit, occ);
              setLocationFields(hit, occ);
              setTemporalFields(hit, occ);
              setCrawlingFields(hit, occ);
              setDatasetFields(hit, occ);
              setTaxonFields(hit, occ);

              // issues
              getListValue(hit, ISSUE)
                  .ifPresent(
                      v -> {
                        Set<OccurrenceIssue> issues = new HashSet<>();
                        v.forEach(issue -> issues.add(OccurrenceIssue.valueOf(issue)));
                        occ.setIssues(issues);
                      });

              // multimedia extension
              // TODO: multimedia. see if it's indexed and how
              //              getStringValue(hit, MEDIA_TYPE)
              //                  .ifPresent(
              //                      mediaType -> {
              //                        MediaObject mediaObject = new MediaObject();
              //                        // media type has to be compared ignoring the case
              //                        Arrays.stream(MediaType.values())
              //                            .filter(v -> v.name().equalsIgnoreCase(mediaType))
              //                            .findFirst()
              //                            .ifPresent(mediaObject::setType);
              //                        occ.setMedia(Collections.singletonList(mediaObject));
              //                      });
            });
    return Optional.of(occurrences);
  }

  private static void setOccurrenceFields(SearchHit hit, Occurrence occ) {
    getValue(hit, GBIF_ID, Integer::valueOf).ifPresent(occ::setKey);
    getValue(hit, BASIS_OF_RECORD, BasisOfRecord::valueOf).ifPresent(occ::setBasisOfRecord);
    getValue(hit, ESTABLISHMENT_MEANS, EstablishmentMeans::valueOf)
        .ifPresent(occ::setEstablishmentMeans);
    getValue(hit, LIFE_STAGE, LifeStage::valueOf).ifPresent(occ::setLifeStage);
    getDateValue(hit, MODIFIED).ifPresent(occ::setModified);
    getValue(hit, REFERENCES, URI::create).ifPresent(occ::setReferences);
    getValue(hit, SEX, Sex::valueOf).ifPresent(occ::setSex);
    getValue(hit, TYPE_STATUS, TypeStatus::valueOf).ifPresent(occ::setTypeStatus);
    getStringValue(hit, TYPIFIED_NAME).ifPresent(occ::setTypifiedName);
    getValue(hit, INDIVIDUAL_COUNT, Integer::valueOf).ifPresent(occ::setIndividualCount);
    // FIXME: should we have a list of identifiers in the schema?
    getStringValue(hit, IDENTIFIER)
        .ifPresent(
            v -> {
              Identifier identifier = new Identifier();
              identifier.setIdentifier(v);
              occ.setIdentifiers(Collections.singletonList(identifier));
            });

    // FIXME: should we have a list in the schema and all the info of the enum?
    getStringValue(hit, RELATION)
        .ifPresent(
            v -> {
              OccurrenceRelation occRelation = new OccurrenceRelation();
              occRelation.setId(v);
              occ.setRelations(Collections.singletonList(occRelation));
            });
  }

  private static void setTemporalFields(SearchHit hit, Occurrence occ) {
    getDateValue(hit, DATE_IDENTIFIED).ifPresent(occ::setDateIdentified);
    getValue(hit, DAY, Integer::valueOf).ifPresent(occ::setDay);
    getValue(hit, MONTH, Integer::valueOf).ifPresent(occ::setMonth);
    getValue(hit, YEAR, Integer::valueOf).ifPresent(occ::setYear);
    getDateValue(hit, EVENT_DATE).ifPresent(occ::setEventDate);
  }

  private static void setLocationFields(SearchHit hit, Occurrence occ) {
    getValue(hit, CONTINENT, Continent::valueOf).ifPresent(occ::setContinent);
    getStringValue(hit, STATE_PROVINCE).ifPresent(occ::setStateProvince);
    getValue(hit, COUNTRY_CODE, Country::fromIsoCode).ifPresent(occ::setCountry);
    getDoubleValue(hit, COORDINATE_ACCURACY).ifPresent(occ::setCoordinateAccuracy);
    getDoubleValue(hit, COORDINATE_PRECISION).ifPresent(occ::setCoordinatePrecision);
    getDoubleValue(hit, COORDINATE_UNCERTAINTY_METERS)
        .ifPresent(occ::setCoordinateUncertaintyInMeters);
    getDoubleValue(hit, LATITUDE).ifPresent(occ::setDecimalLatitude);
    getDoubleValue(hit, LONGITUDE).ifPresent(occ::setDecimalLongitude);
    getDoubleValue(hit, DEPTH).ifPresent(occ::setDepth);
    getDoubleValue(hit, DEPTH_ACCURACY).ifPresent(occ::setDepthAccuracy);
    getDoubleValue(hit, ELEVATION).ifPresent(occ::setElevation);
    getDoubleValue(hit, ELEVATION_ACCURACY).ifPresent(occ::setElevationAccuracy);
    getStringValue(hit, WATER_BODY).ifPresent(occ::setWaterBody);
  }

  private static void setTaxonFields(SearchHit hit, Occurrence occ) {
    getIntValue(hit, KINGDOM_KEY).ifPresent(occ::setKingdomKey);
    getStringValue(hit, KINGDOM).ifPresent(occ::setKingdom);
    getIntValue(hit, PHYLUM_KEY).ifPresent(occ::setPhylumKey);
    getStringValue(hit, PHYLUM).ifPresent(occ::setPhylum);
    getIntValue(hit, CLASS_KEY).ifPresent(occ::setClassKey);
    getStringValue(hit, CLASS).ifPresent(occ::setClazz);
    getIntValue(hit, ORDER_KEY).ifPresent(occ::setOrderKey);
    getStringValue(hit, ORDER).ifPresent(occ::setOrder);
    getIntValue(hit, FAMILY_KEY).ifPresent(occ::setFamilyKey);
    getStringValue(hit, FAMILY).ifPresent(occ::setFamily);
    getIntValue(hit, GENUS_KEY).ifPresent(occ::setGenusKey);
    getStringValue(hit, GENUS).ifPresent(occ::setGenus);
    getIntValue(hit, SUBGENUS_KEY).ifPresent(occ::setSubgenusKey);
    getStringValue(hit, SUBGENUS).ifPresent(occ::setSubgenus);
    getIntValue(hit, SPECIES_KEY).ifPresent(occ::setSpeciesKey);
    getStringValue(hit, SPECIES).ifPresent(occ::setSpecies);
    getStringValue(hit, SCIENTIFIC_NAME).ifPresent(occ::setScientificName);
    getStringValue(hit, SPECIFIC_EPITHET).ifPresent(occ::setSpecificEpithet);
    getStringValue(hit, INFRA_SPECIFIC_EPITHET).ifPresent(occ::setInfraspecificEpithet);
    getStringValue(hit, GENERIC_NAME).ifPresent(occ::setGenericName);
    getStringValue(hit, TAXON_RANK).ifPresent(v -> occ.setTaxonRank(Rank.valueOf(v)));
    getIntValue(hit, TAXON_KEY).ifPresent(occ::setTaxonKey);
    getIntValue(hit, ACCEPTED_TAXON_KEY).ifPresent(occ::setAcceptedTaxonKey);
    getStringValue(hit, ACCEPTED_SCIENTIFIC_NAME).ifPresent(occ::setScientificName);
    getValue(hit, TAXONOMIC_STATUS, TaxonomicStatus::valueOf).ifPresent(occ::setTaxonomicStatus);
  }

  private static void setDatasetFields(SearchHit hit, Occurrence occ) {
    getValue(hit, PUBLISHING_COUNTRY, v -> Country.fromIsoCode(v.toUpperCase()))
        .ifPresent(occ::setPublishingCountry);
    getValue(hit, DATASET_KEY, UUID::fromString).ifPresent(occ::setDatasetKey);
    getValue(hit, INSTALLATION_KEY, UUID::fromString).ifPresent(occ::setInstallationKey);
    getValue(hit, PUBLISHING_ORGANIZATION_KEY, UUID::fromString)
        .ifPresent(occ::setPublishingOrgKey);
    getValue(hit, LICENSE, v -> License.fromLicenseUrl(v).orNull()).ifPresent(occ::setLicense);
    // TODO
    //    getValue(hit, PROTOCOL, EndpointType::fromString).ifPresent(occ::setProtocol);

    getListValue(hit, NETWORK_KEY)
        .ifPresent(
            v -> {
              List<UUID> networkKeys = new ArrayList<>();
              v.forEach(s -> networkKeys.add(UUID.fromString(s)));
              occ.setNetworkKeys(networkKeys);
            });
  }

  private static void setCrawlingFields(SearchHit hit, Occurrence occ) {
    getValue(hit, CRAWL_ID, Integer::valueOf).ifPresent(occ::setCrawlId);
    getDateValue(hit, LAST_INTERPRETED).ifPresent(occ::setLastInterpreted);
    getDateValue(hit, LAST_PARSED).ifPresent(occ::setLastParsed);
    getDateValue(hit, LAST_CRAWLED).ifPresent(occ::setLastParsed);
  }

  private static Optional<String> getStringValue(SearchHit hit, OccurrenceEsField esField) {
    return getValue(hit, esField, Function.identity());
  }

  private static Optional<Integer> getIntValue(SearchHit hit, OccurrenceEsField esField) {
    return getValue(hit, esField, Integer::valueOf);
  }

  private static Optional<Double> getDoubleValue(SearchHit hit, OccurrenceEsField esField) {
    return getValue(hit, esField, Double::valueOf);
  }

  private static Optional<Date> getDateValue(SearchHit hit, OccurrenceEsField esField) {
    return getValue(hit, esField, v -> STRING_TO_DATE.apply(v, DATE_FORMAT));
  }

  private static Optional<List<String>> getListValue(SearchHit hit, OccurrenceEsField esField) {
    return Optional.ofNullable(hit.getSourceAsMap().get(esField.getFieldName()))
        .map(v -> (List<String>) v)
        .filter(v -> !v.isEmpty());
  }

  private static Optional<Map<String, Object>> getMapValue(
      SearchHit hit, OccurrenceEsField esField) {
    return Optional.ofNullable(hit.getSourceAsMap().get(esField.getFieldName()))
        .map(v -> (Map<String, Object>) v)
        .filter(v -> !v.isEmpty());
  }

  private static <T> Optional<T> getValue(
      SearchHit hit, OccurrenceEsField esField, Function<String, T> mapper) {
    String fieldName = esField.getFieldName();
    Map<String, Object> fields = hit.getSourceAsMap();
    if (IS_NESTED.test(esField.getFieldName())) {
      // take all paths till the field name
      String[] paths = esField.getFieldName().split("\\.");
      for (int i = 0; i < paths.length - 1; i++) {
        fields = (Map<String, Object>) fields.get(paths[i]);
      }
      fieldName = paths[paths.length - 1];
    }

    return Optional.ofNullable(fields.get(fieldName))
        .map(String::valueOf)
        .filter(v -> !v.isEmpty())
        .map(mapper);
  }
}
