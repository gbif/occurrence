package org.gbif.occurrence.search.es;

import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.http.HttpEntity;
import org.apache.solr.client.solrj.SolrQuery;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
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

import static org.gbif.common.search.solr.SolrConstants.DEFAULT_FILTER_QUERY;
import static org.gbif.occurrence.search.es.EsQueryUtils.*;
import static org.gbif.occurrence.search.es.OccurrenceEsField.*;
import static org.gbif.occurrence.search.es.OccurrenceEsField.RELATION;

/**
 * Occurrence search service. Executes {@link OccurrenceSearchRequest} by transforming the request
 * into {@link SolrQuery}.
 */
public class OccurrenceSearchESImpl implements OccurrenceSearchService {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceSearchESImpl.class);

  private static final ObjectReader JSON_READER = new ObjectMapper().reader(Map.class);

  private final NameUsageMatchingService nameUsageMatchingService;
  private final RestClient esClient;
  private final String esIndex;

  @Inject
  public OccurrenceSearchESImpl(
      RestClient esClient,
      NameUsageMatchingService nameUsageMatchingService,
      @Named("max.offset") int maxOffset,
      @Named("max.limit") int maxLimit,
      @Named("facets.enable") boolean facetsEnable,
      @Named("es.index") String esIndex) {
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
      HttpEntity requestBody = EsSearchRequestBuilder.buildRequestBody(request);
      Response response = null;
      try {
        response =
            esClient.performRequest(
                "GET",
                SEARCH_ENDPOINT.apply(esIndex),
                Collections.emptyMap(),
                requestBody,
                HEADERS.get());
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
      Response esResponse, Pageable request) {

    JsonNode resp;
    try {
      resp = JSON_READER.readTree(esResponse.getEntity().getContent());
    } catch (IOException e) {
      LOG.error("Error reading ES response", e);
      throw new IllegalStateException(e.getMessage(), e);
    }

    // parse results
    final JsonNode hits = resp.path("hits");
    final DateFormat dateFormat =
        new SimpleDateFormat("yyyy-MM-dd"); // Quoted "Z" to indicate UTC, no timezone offset

    List<Occurrence> occurrences = new ArrayList<>();
    for (JsonNode record : hits.path("hits")) {
      final JsonNode source = record.path("_source");

      // create occurrence
      Occurrence occ = new Occurrence();
      occurrences.add(occ);

      // fill out fields
      getValueText(source, BASIS_OF_RECORD)
          .ifPresent(v -> occ.setBasisOfRecord(BasisOfRecord.valueOf(v)));
      getValueText(source, CONTINENT).ifPresent(v -> occ.setContinent(Continent.valueOf(v)));
      getValue(source, COORDINATE_ACCURACY).ifPresent(v -> occ.setCoordinateAccuracy(v.asDouble()));
      getValue(source, COORDINATE_PRECISION)
          .ifPresent(v -> occ.setCoordinatePrecision(v.asDouble()));
      getValue(source, COORDINATE_UNCERTAINTY_METERS)
          .ifPresent(v -> occ.setCoordinateUncertaintyInMeters(v.asDouble()));
      getValueText(source, COUNTRY)
          .ifPresent(v -> occ.setCountry(Country.valueOf(v.toUpperCase())));
      // TODO: date identified shouldnt be a timestamp
      getValue(source, DATE_IDENTIFIED).ifPresent(v -> occ.setDateIdentified(new Date(v.asLong())));
      getValue(source, DAY).ifPresent(v -> occ.setDay(v.asInt()));
      getValue(source, MONTH).ifPresent(v -> occ.setMonth(v.asInt()));
      getValue(source, YEAR).ifPresent(v -> occ.setYear(v.asInt()));
      getValue(source, LATITUDE).ifPresent(v -> occ.setDecimalLatitude(v.asDouble()));
      getValue(source, LONGITUDE).ifPresent(v -> occ.setDecimalLongitude(v.asDouble()));
      getValue(source, DEPTH).ifPresent(v -> occ.setDepth(v.asDouble()));
      getValue(source, DEPTH_ACCURACY).ifPresent(v -> occ.setDepthAccuracy(v.asDouble()));
      getValue(source, ELEVATION).ifPresent(v -> occ.setElevation(v.asDouble()));
      getValue(source, ELEVATION_ACCURACY).ifPresent(v -> occ.setElevationAccuracy(v.asDouble()));
      getValueText(source, ESTABLISHMENT_MEANS)
          .ifPresent(v -> occ.setEstablishmentMeans(EstablishmentMeans.valueOf(v)));

      // FIXME: should we have a list of identifiers in the schema?
      getValueText(source, IDENTIFIER)
          .ifPresent(
              v -> {
                Identifier identifier = new Identifier();
                identifier.setIdentifier(v);
                occ.setIdentifiers(Collections.singletonList(identifier));
              });

      getValue(source, INDIVIDUAL_COUNT).ifPresent(v -> occ.setIndividualCount(v.asInt()));
      getValueText(source, LICENSE).ifPresent(v -> occ.setLicense(License.valueOf(v)));
      getValue(source, ELEVATION).ifPresent(v -> occ.setElevation(v.asDouble()));

      // TODO: facts??

      // issues
      getValue(source, ISSUE)
          .ifPresent(
              v -> {
                Set<OccurrenceIssue> issues = new HashSet<>();
                v.forEach(issue -> issues.add(OccurrenceIssue.valueOf(issue.asText())));
                occ.setIssues(issues);
              });

      // FIXME: should we have a list in the schema and all the info of the enum?
      getValueText(source, RELATION)
          .ifPresent(
              v -> {
                OccurrenceRelation occRelation = new OccurrenceRelation();
                occRelation.setId(v);
                occ.setRelations(Collections.singletonList(occRelation));
              });

      getValueText(source, LAST_INTERPRETED)
          .ifPresent(v -> occ.setLastInterpreted(DATE_FORMAT.apply(v, dateFormat)));
      getValueText(source, LAST_CRAWLED)
          .ifPresent(v -> occ.setLastCrawled(DATE_FORMAT.apply(v, dateFormat)));
      getValueText(source, LAST_PARSED)
          .ifPresent(v -> occ.setLastParsed(DATE_FORMAT.apply(v, dateFormat)));

      getValueText(source, EVENT_DATE)
          .ifPresent(v -> occ.setEventDate(DATE_FORMAT.apply(v, dateFormat)));
      getValueText(source, LIFE_STAGE).ifPresent(v -> occ.setLifeStage(LifeStage.valueOf(v)));
      // TODO: modified shouldnt be a timestamp
      getValue(source, MODIFIED).ifPresent(v -> occ.setModified(new Date(v.asLong())));
      getValueText(source, REFERENCES).ifPresent(v -> occ.setReferences(URI.create(v)));
      getValueText(source, SEX).ifPresent(v -> occ.setSex(Sex.valueOf(v)));
      getValueText(source, STATE_PROVINCE).ifPresent(occ::setStateProvince);
      getValueText(source, TYPE_STATUS).ifPresent(v -> occ.setTypeStatus(TypeStatus.valueOf(v)));
      getValueText(source, WATER_BODY).ifPresent(occ::setWaterBody);
      getValueText(source, TYPIFIED_NAME).ifPresent(occ::setTypifiedName);

      // taxon
      getValue(source, KINGDOM_KEY).ifPresent(v -> occ.setKingdomKey(v.asInt()));
      getValueText(source, KINGDOM).ifPresent(occ::setKingdom);
      getValue(source, PHYLUM_KEY).ifPresent(v -> occ.setPhylumKey(v.asInt()));
      getValueText(source, PHYLUM).ifPresent(occ::setPhylum);
      getValue(source, CLASS_KEY).ifPresent(v -> occ.setClassKey(v.asInt()));
      getValueText(source, CLASS).ifPresent(occ::setClazz);
      getValue(source, ORDER_KEY).ifPresent(v -> occ.setOrderKey(v.asInt()));
      getValueText(source, ORDER).ifPresent(occ::setOrder);
      getValue(source, FAMILY_KEY).ifPresent(v -> occ.setFamilyKey(v.asInt()));
      getValueText(source, FAMILY).ifPresent(occ::setFamily);
      getValue(source, GENUS_KEY).ifPresent(v -> occ.setGenusKey(v.asInt()));
      getValueText(source, GENUS).ifPresent(occ::setGenus);
      getValue(source, SUBGENUS_KEY).ifPresent(v -> occ.setSubgenusKey(v.asInt()));
      getValueText(source, SUBGENUS).ifPresent(occ::setSubgenus);
      getValue(source, SPECIES_KEY).ifPresent(v -> occ.setSpeciesKey(v.asInt()));
      getValueText(source, SPECIES).ifPresent(occ::setSpecies);
      getValueText(source, SCIENTIFIC_NAME).ifPresent(occ::setScientificName);
      getValueText(source, SPECIFIC_EPITHET).ifPresent(occ::setSpecificEpithet);
      getValueText(source, INFRA_SPECIFIC_EPITHET).ifPresent(occ::setInfraspecificEpithet);
      getValueText(source, GENERIC_NAME).ifPresent(occ::setGenericName);
      getValueText(source, TAXON_RANK).ifPresent(v -> occ.setTaxonRank(Rank.valueOf(v)));
      getValue(source, TAXON_KEY).ifPresent(v -> occ.setTaxonKey(v.asInt()));

      // multimedia
      // TODO: change when we have the full info about multimedia
      getValueText(source, MEDIA_TYPE)
          .ifPresent(
              mediaType -> {
                MediaObject mediaObject = new MediaObject();
                mediaObject.setType(MediaType.valueOf(mediaType));
                occ.setMedia(Collections.singletonList(mediaObject));
              });

      getValueText(source, PUBLISHING_COUNTRY)
          .ifPresent(v -> occ.setPublishingCountry(Country.fromIsoCode(v.toUpperCase())));
      getValueText(source, DATASET_KEY).ifPresent(v -> occ.setDatasetKey(UUID.fromString(v)));
      getValueText(source, INSTALLATION_KEY)
          .ifPresent(v -> occ.setInstallationKey(UUID.fromString(v)));
      getValueText(source, PUBLISHING_ORGANIZATION_KEY)
          .ifPresent(v -> occ.setPublishingOrgKey(UUID.fromString(v)));
      getValue(source, CRAWL_ID).ifPresent(v -> occ.setCrawlId(v.asInt()));
      getValueText(source, PROTOCOL).ifPresent(v -> occ.setProtocol(EndpointType.fromString(v)));

      // FIXME: shouldn't networkkey be a list in the schema?
      getValueText(source, NETWORK_KEY)
          .ifPresent(v -> occ.setNetworkKeys(Collections.singletonList(UUID.fromString(v))));
    }

    // create response
    SearchResponse<Occurrence, OccurrenceSearchParameter> response = new SearchResponse<>(request);
    response.setCount(hits.get("total").asLong());
    response.setResults(occurrences);

    return response;
  }

  private Optional<JsonNode> getValue(JsonNode source, OccurrenceEsField esField) {
    return Optional.ofNullable(source.get(esField.getFieldName())).filter(v -> !v.isNull());
  }

  private Optional<String> getValueText(JsonNode source, OccurrenceEsField esField) {
    return Optional.ofNullable(source.get(esField.getFieldName()))
        .filter(v -> !v.isNull())
        .map(JsonNode::asText)
        .filter(str -> !str.isEmpty());
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
