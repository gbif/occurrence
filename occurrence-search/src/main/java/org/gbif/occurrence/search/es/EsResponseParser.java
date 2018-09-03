package org.gbif.occurrence.search.es;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.gbif.api.model.common.Identifier;
import org.gbif.api.model.common.MediaObject;
import org.gbif.api.model.common.paging.Pageable;
import org.gbif.api.model.common.search.Facet;
import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.OccurrenceRelation;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.vocabulary.*;

import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.gbif.occurrence.search.es.EsQueryUtils.DATE_FORMAT;
import static org.gbif.occurrence.search.es.EsQueryUtils.ES_TO_SEARCH_MAPPING;
import static org.gbif.occurrence.search.es.OccurrenceEsField.*;
import static org.gbif.occurrence.search.es.OccurrenceEsField.NETWORK_KEY;

public class EsResponseParser {

  private EsResponseParser() {}

  /**
   * Builds a SearchResponse instance using the current builder state.
   *
   * @return a new instance of a SearchResponse.
   */
  static SearchResponse<Occurrence, OccurrenceSearchParameter> buildResponse(
      org.elasticsearch.action.search.SearchResponse esResponse, Pageable request) {

    SearchResponse<Occurrence, OccurrenceSearchParameter> response = new SearchResponse<>(request);
    response.setCount(esResponse.getHits().getTotalHits());
    response.setResults(parseHits(esResponse));
    response.setFacets(parseFacets(esResponse));

    return response;
  }

  private static List<Facet<OccurrenceSearchParameter>> parseFacets(
      org.elasticsearch.action.search.SearchResponse esResponse) {
    return esResponse
        .getAggregations()
        .asList()
        .stream()
        .map(
            aggs -> {
              List<? extends Terms.Bucket> buckets = ((Terms) aggs).getBuckets();

              // set counts
              List<Facet.Count> counts = new ArrayList<>(buckets.size());
              buckets.forEach(
                  bucket ->
                      counts.add(new Facet.Count(bucket.getKeyAsString(), bucket.getDocCount())));

              // build facet
              Facet<OccurrenceSearchParameter> facet =
                  new Facet<>(ES_TO_SEARCH_MAPPING.get(aggs.getName()));
              facet.setCounts(counts);

              return facet;
            })
        .collect(Collectors.toList());
  }

  private static List<Occurrence> parseHits(
      org.elasticsearch.action.search.SearchResponse esResponse) {
    final DateFormat dateFormat =
        new SimpleDateFormat("yyyy-MM-dd"); // Quoted "Z" to indicate UTC, no timezone offset

    SearchHits hits = esResponse.getHits();

    List<Occurrence> occurrences = new ArrayList<>();
    hits.forEach(
        hit -> {
          // create occurrence
          Occurrence occ = new Occurrence();
          occurrences.add(occ);

          // fill out fields
          getValue(hit, BASIS_OF_RECORD, BasisOfRecord::valueOf).ifPresent(occ::setBasisOfRecord);
          getValue(hit, CONTINENT).ifPresent(v -> occ.setContinent(Continent.valueOf(v)));
          getValue(hit, COORDINATE_ACCURACY, Double::valueOf).ifPresent(occ::setCoordinateAccuracy);
          getValue(hit, COORDINATE_PRECISION, Double::valueOf)
              .ifPresent(occ::setCoordinatePrecision);
          getValue(hit, COORDINATE_UNCERTAINTY_METERS, Double::valueOf)
              .ifPresent(occ::setCoordinateUncertaintyInMeters);
          getValue(hit, COUNTRY, v -> Country.valueOf(v.toUpperCase())).ifPresent(occ::setCountry);
          // TODO: date identified shouldnt be a timestamp
          getValue(hit, DATE_IDENTIFIED, v -> new Date(Long.valueOf(v)))
              .ifPresent(occ::setDateIdentified);
          getValue(hit, DAY, Integer::valueOf).ifPresent(occ::setDay);
          getValue(hit, MONTH, Integer::valueOf).ifPresent(occ::setMonth);
          getValue(hit, YEAR, Integer::valueOf).ifPresent(occ::setYear);
          getValue(hit, LATITUDE, Double::valueOf).ifPresent(occ::setDecimalLatitude);
          getValue(hit, LONGITUDE, Double::valueOf).ifPresent(occ::setDecimalLongitude);
          getValue(hit, DEPTH, Double::valueOf).ifPresent(occ::setDepth);
          getValue(hit, DEPTH_ACCURACY, Double::valueOf).ifPresent(occ::setDepthAccuracy);
          getValue(hit, ELEVATION, Double::valueOf).ifPresent(occ::setElevation);
          getValue(hit, ELEVATION_ACCURACY, Double::valueOf).ifPresent(occ::setElevationAccuracy);
          getValue(hit, ESTABLISHMENT_MEANS, EstablishmentMeans::valueOf)
              .ifPresent(occ::setEstablishmentMeans);

          // FIXME: should we have a list of identifiers in the schema?
          getValue(hit, IDENTIFIER)
              .ifPresent(
                  v -> {
                    Identifier identifier = new Identifier();
                    identifier.setIdentifier(v);
                    occ.setIdentifiers(Collections.singletonList(identifier));
                  });

          getValue(hit, INDIVIDUAL_COUNT, Integer::valueOf).ifPresent(occ::setIndividualCount);
          getValue(hit, LICENSE, License::valueOf).ifPresent(occ::setLicense);
          getValue(hit, ELEVATION, Double::valueOf).ifPresent(occ::setElevation);

          // TODO: facts??

          // issues
          getValueList(hit, ISSUE)
              .ifPresent(
                  v -> {
                    Set<OccurrenceIssue> issues = new HashSet<>();
                    v.forEach(issue -> issues.add(OccurrenceIssue.valueOf(issue)));
                    occ.setIssues(issues);
                  });

          // FIXME: should we have a list in the schema and all the info of the enum?
          getValue(hit, RELATION)
              .ifPresent(
                  v -> {
                    OccurrenceRelation occRelation = new OccurrenceRelation();
                    occRelation.setId(v);
                    occ.setRelations(Collections.singletonList(occRelation));
                  });

          getValue(hit, LAST_INTERPRETED, v -> DATE_FORMAT.apply(v, dateFormat))
              .ifPresent(occ::setLastInterpreted);
          getValue(hit, LAST_CRAWLED, v -> DATE_FORMAT.apply(v, dateFormat))
              .ifPresent(occ::setLastCrawled);
          getValue(hit, LAST_PARSED, v -> DATE_FORMAT.apply(v, dateFormat))
              .ifPresent(occ::setLastParsed);

          getValue(hit, EVENT_DATE, v -> DATE_FORMAT.apply(v, dateFormat))
              .ifPresent(occ::setEventDate);
          getValue(hit, LIFE_STAGE, LifeStage::valueOf).ifPresent(occ::setLifeStage);
          // TODO: modified shouldnt be a timestamp
          getValue(hit, MODIFIED, v -> new Date(Long.valueOf(v))).ifPresent(occ::setModified);
          getValue(hit, REFERENCES, URI::create).ifPresent(occ::setReferences);
          getValue(hit, SEX, Sex::valueOf).ifPresent(occ::setSex);
          getValue(hit, STATE_PROVINCE).ifPresent(occ::setStateProvince);
          getValue(hit, TYPE_STATUS, TypeStatus::valueOf).ifPresent(occ::setTypeStatus);
          getValue(hit, WATER_BODY).ifPresent(occ::setWaterBody);
          getValue(hit, TYPIFIED_NAME).ifPresent(occ::setTypifiedName);

          // taxon
          getValue(hit, KINGDOM_KEY, Integer::valueOf).ifPresent(occ::setKingdomKey);
          getValue(hit, KINGDOM).ifPresent(occ::setKingdom);
          getValue(hit, PHYLUM_KEY, Integer::valueOf).ifPresent(occ::setPhylumKey);
          getValue(hit, PHYLUM).ifPresent(occ::setPhylum);
          getValue(hit, CLASS_KEY, Integer::valueOf).ifPresent(occ::setClassKey);
          getValue(hit, CLASS).ifPresent(occ::setClazz);
          getValue(hit, ORDER_KEY, Integer::valueOf).ifPresent(occ::setOrderKey);
          getValue(hit, ORDER).ifPresent(occ::setOrder);
          getValue(hit, FAMILY_KEY, Integer::valueOf).ifPresent(occ::setFamilyKey);
          getValue(hit, FAMILY).ifPresent(occ::setFamily);
          getValue(hit, GENUS_KEY, Integer::valueOf).ifPresent(occ::setGenusKey);
          getValue(hit, GENUS).ifPresent(occ::setGenus);
          getValue(hit, SUBGENUS_KEY, Integer::valueOf).ifPresent(occ::setSubgenusKey);
          getValue(hit, SUBGENUS).ifPresent(occ::setSubgenus);
          getValue(hit, SPECIES_KEY, Integer::valueOf).ifPresent(occ::setSpeciesKey);
          getValue(hit, SPECIES).ifPresent(occ::setSpecies);
          getValue(hit, SCIENTIFIC_NAME).ifPresent(occ::setScientificName);
          getValue(hit, SPECIFIC_EPITHET).ifPresent(occ::setSpecificEpithet);
          getValue(hit, INFRA_SPECIFIC_EPITHET).ifPresent(occ::setInfraspecificEpithet);
          getValue(hit, GENERIC_NAME).ifPresent(occ::setGenericName);
          getValue(hit, TAXON_RANK).ifPresent(v -> occ.setTaxonRank(Rank.valueOf(v)));
          getValue(hit, TAXON_KEY, Integer::valueOf).ifPresent(occ::setTaxonKey);

          // multimedia
          // TODO: change when we have the full info about multimedia
          getValue(hit, MEDIA_TYPE)
              .ifPresent(
                  mediaType -> {
                    MediaObject mediaObject = new MediaObject();
                    mediaObject.setType(MediaType.valueOf(mediaType));
                    occ.setMedia(Collections.singletonList(mediaObject));
                  });

          getValue(hit, PUBLISHING_COUNTRY, v -> Country.fromIsoCode(v.toUpperCase()))
              .ifPresent(occ::setPublishingCountry);
          getValue(hit, DATASET_KEY, UUID::fromString).ifPresent(occ::setDatasetKey);
          getValue(hit, INSTALLATION_KEY, UUID::fromString).ifPresent(occ::setInstallationKey);
          getValue(hit, PUBLISHING_ORGANIZATION_KEY, UUID::fromString)
              .ifPresent(occ::setPublishingOrgKey);
          getValue(hit, CRAWL_ID, Integer::valueOf).ifPresent(occ::setCrawlId);
          getValue(hit, PROTOCOL, EndpointType::fromString).ifPresent(occ::setProtocol);

          // FIXME: shouldn't networkkey be a list in the schema?
          getValue(hit, NETWORK_KEY)
              .ifPresent(v -> occ.setNetworkKeys(Collections.singletonList(UUID.fromString(v))));
        });
    return occurrences;
  }

  private static Optional<String> getValue(SearchHit hit, OccurrenceEsField esField) {
    return getValue(hit, esField, Function.identity());
  }

  private static <T> Optional<T> getValue(
      SearchHit hit, OccurrenceEsField esField, Function<String, T> mapper) {
    return Optional.ofNullable(hit.getSourceAsMap().get(esField.getFieldName()))
        .map(String::valueOf)
        .filter(v -> !v.isEmpty())
        .map(mapper);
  }

  private static Optional<List<String>> getValueList(SearchHit hit, OccurrenceEsField esField) {
    return Optional.ofNullable(hit.getSourceAsMap().get(esField.getFieldName()))
        .map(v -> (List<String>) v)
        .filter(v -> !v.isEmpty());
  }
}
