package org.gbif.occurrence.search.es;

import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.http.HttpEntity;
import org.apache.solr.client.solrj.SolrQuery;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.api.model.checklistbank.NameUsageMatch.MatchType;
import org.gbif.api.model.common.Identifier;
import org.gbif.api.model.common.MediaObject;
import org.gbif.api.model.common.paging.Pageable;
import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.OccurrenceRelation;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.service.checklistbank.NameUsageMatchingService;
import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.gbif.api.vocabulary.*;
import org.gbif.common.search.SearchException;
import org.gbif.common.search.solr.QueryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import java.io.IOException;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;

import static org.gbif.common.search.solr.SolrConstants.DEFAULT_FILTER_QUERY;
import static org.gbif.occurrence.search.es.EsQueryUtils.*;
import static org.gbif.occurrence.search.es.OccurrenceEsField.*;
import static org.gbif.occurrence.search.es.OccurrenceEsField.RELATION;

/**
 * Occurrence search service. Executes {@link OccurrenceSearchRequest} by transforming the request
 * into {@link SolrQuery}.
 */
public class OccurrenceSearchEsImpl implements OccurrenceSearchService {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceSearchEsImpl.class);

  private static final ObjectReader JSON_READER = new ObjectMapper().reader(Map.class);

  private final NameUsageMatchingService nameUsageMatchingService;
  private final RestHighLevelClient esClient;
  private final String esIndex;
  private final boolean facetsEnabled;
  private final int maxOffset;
  private final int maxLimit;

  @Inject
  public OccurrenceSearchEsImpl(
      RestHighLevelClient esClient,
      NameUsageMatchingService nameUsageMatchingService,
      @Named("max.offset") int maxOffset,
      @Named("max.limit") int maxLimit,
      @Named("facets.enable") boolean facetsEnable,
      @Named("es.index") String esIndex) {
    facetsEnabled = facetsEnable;
    this.maxOffset = maxOffset;
    this.maxLimit = maxLimit;
    this.esIndex = esIndex;
    // create ES client
    this.esClient = esClient;
    this.nameUsageMatchingService = nameUsageMatchingService;
  }

  @Override
  public SearchResponse<Occurrence, OccurrenceSearchParameter> search(
      @Nullable OccurrenceSearchRequest request) {
    if (request == null) {
      return new SearchResponse<>();
    }

    if (hasReplaceableScientificNames(request)) {
      SearchRequest esRequest =
          EsSearchRequestBuilder.buildSearchRequest(
              request, facetsEnabled, maxOffset, maxLimit, esIndex);
      org.elasticsearch.action.search.SearchResponse response = null;
      try {
        response = esClient.search(esRequest, HEADERS.get());
      } catch (IOException e) {
        LOG.error("Error executing the search operation", e);
        throw new SearchException(e);
      }

      return buildResponse(response, request);
    }
    return new SearchResponse<>(request);
  }

  /**
   * Builds a SearchResponse instance using the current builder state.
   *
   * @return a new instance of a SearchResponse.
   */
  private SearchResponse<Occurrence, OccurrenceSearchParameter> buildResponse(
      org.elasticsearch.action.search.SearchResponse esResponse, Pageable request) {

    SearchHits hits = esResponse.getHits();

    final DateFormat dateFormat =
        new SimpleDateFormat("yyyy-MM-dd"); // Quoted "Z" to indicate UTC, no timezone offset

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

    // create response
    SearchResponse<Occurrence, OccurrenceSearchParameter> response = new SearchResponse<>(request);
    response.setCount(hits.totalHits);
    response.setResults(occurrences);

    return response;
  }

  private Optional<String> getValue(SearchHit hit, OccurrenceEsField esField) {
    return getValue(hit, esField, Function.identity());
  }

  private <T> Optional<T> getValue(
      SearchHit hit, OccurrenceEsField esField, Function<String, T> mapper) {
    return Optional.ofNullable(hit.getSourceAsMap().get(esField.getFieldName()))
        .map(String::valueOf)
        .filter(v -> !v.isEmpty())
        .map(mapper);
  }

  private Optional<List<String>> getValueList(SearchHit hit, OccurrenceEsField esField) {
    return Optional.ofNullable(hit.getSourceAsMap().get(esField.getFieldName()))
        .map(v -> (List<String>) v)
        .filter(v -> !v.isEmpty());
  }

  @Override
  public List<String> suggestCatalogNumbers(String prefix, @Nullable Integer limit) {
    return suggestTermByField(prefix, OccurrenceSearchParameter.CATALOG_NUMBER, limit);
  }

  @Override
  public List<String> suggestCollectionCodes(String prefix, @Nullable Integer limit) {
    return suggestTermByField(prefix, OccurrenceSearchParameter.COLLECTION_CODE, limit);
  }

  @Override
  public List<String> suggestRecordedBy(String prefix, @Nullable Integer limit) {
    return suggestTermByField(prefix, OccurrenceSearchParameter.RECORDED_BY, limit);
  }

  @Override
  public List<String> suggestInstitutionCodes(String prefix, @Nullable Integer limit) {
    return suggestTermByField(prefix, OccurrenceSearchParameter.INSTITUTION_CODE, limit);
  }

  @Override
  public List<String> suggestRecordNumbers(String prefix, @Nullable Integer limit) {
    return suggestTermByField(prefix, OccurrenceSearchParameter.RECORD_NUMBER, limit);
  }

  @Override
  public List<String> suggestOccurrenceIds(String prefix, @Nullable Integer limit) {
    return suggestTermByField(prefix, OccurrenceSearchParameter.OCCURRENCE_ID, limit);
  }

  @Override
  public List<String> suggestOrganismIds(@Min(1L) String prefix, @Nullable Integer limit) {
    return suggestTermByField(prefix, OccurrenceSearchParameter.ORGANISM_ID, limit);
  }

  @Override
  public List<String> suggestLocalities(@Min(1L) String prefix, @Nullable Integer limit) {
    return suggestTermByField(prefix, OccurrenceSearchParameter.LOCALITY, limit);
  }

  @Override
  public List<String> suggestWaterBodies(@Min(1L) String prefix, @Nullable Integer limit) {
    return suggestTermByField(prefix, OccurrenceSearchParameter.WATER_BODY, limit);
  }

  @Override
  public List<String> suggestStateProvinces(@Min(1L) String prefix, @Nullable Integer limit) {
    return suggestTermByField(prefix, OccurrenceSearchParameter.STATE_PROVINCE, limit);
  }

  /**
   * Searches a indexed terms of a field that matched against the prefix parameter.
   *
   * @param prefix search term
   * @param parameter mapped field to be searched
   * @param limit of maximum matches
   * @return a list of elements that matched against the prefix
   */
  public List<String> suggestTermByField(
      String prefix, OccurrenceSearchParameter parameter, Integer limit) {
    // try {
    //  String solrField = QUERY_FIELD_MAPPING.get(parameter).getFieldName();
    //  SolrQuery solrQuery = buildTermQuery(parseTermsQueryValue(prefix).toLowerCase(), solrField,
    //                                       Objects.firstNonNull(limit, DEFAULT_SUGGEST_LIMIT));
    //  final QueryResponse queryResponse = solrClient.query(solrQuery);
    //  final TermsResponse termsResponse = queryResponse.getTermsResponse();
    //  return
    // termsResponse.getTerms(solrField).stream().map(Term::getTerm).collect(Collectors.toList());
    // } catch (SolrServerException | IOException e) {
    //  LOG.error("Error executing/building the request", e);
    //  throw new SearchException(e);
    // }

    return null;
  }

  /** Escapes a query value and transform it into a phrase query if necessary. */
  private static String parseTermsQueryValue(final String q) {
    // return default query for empty queries
    String qValue = Strings.nullToEmpty(q).trim();
    if (Strings.isNullOrEmpty(qValue)) {
      return DEFAULT_FILTER_QUERY;
    }
    // If default query was sent, must not be escaped
    if (!qValue.equals(DEFAULT_FILTER_QUERY)) {
      qValue = QueryUtils.clearConsecutiveBlanks(qValue);
      qValue = QueryUtils.escapeQuery(qValue);
    }

    return qValue;
  }

  /**
   * Tries to get the corresponding name usage keys from the scientific_name parameter values.
   *
   * @return true: if the request doesn't contain any scientific_name parameter or if any scientific
   *     name was found false: if none scientific name was found
   */
  private boolean hasReplaceableScientificNames(OccurrenceSearchRequest request) {
    boolean hasValidReplaces = true;
    if (request.getParameters().containsKey(OccurrenceSearchParameter.SCIENTIFIC_NAME)) {
      hasValidReplaces = false;
      Collection<String> values =
          request.getParameters().get(OccurrenceSearchParameter.SCIENTIFIC_NAME);
      for (String value : values) {
        NameUsageMatch nameUsageMatch =
            nameUsageMatchingService.match(value, null, null, true, false);
        if (nameUsageMatch.getMatchType() == MatchType.EXACT) {
          hasValidReplaces = true;
          values.remove(value);
          request.addParameter(OccurrenceSearchParameter.TAXON_KEY, nameUsageMatch.getUsageKey());
        }
      }
    }
    return hasValidReplaces;
  }
}
