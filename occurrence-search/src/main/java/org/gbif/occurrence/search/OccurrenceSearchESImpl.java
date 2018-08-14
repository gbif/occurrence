package org.gbif.occurrence.search;

import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HTTP;
import org.apache.solr.client.solrj.SolrQuery;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.api.model.checklistbank.NameUsageMatch.MatchType;
import org.gbif.api.model.common.MediaObject;
import org.gbif.api.model.common.paging.Pageable;
import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.service.checklistbank.NameUsageMatchingService;
import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.gbif.api.vocabulary.*;
import org.gbif.common.search.SearchException;
import org.gbif.common.search.solr.QueryUtils;
import org.gbif.occurrence.search.es.EsConfig;
import org.gbif.occurrence.search.es.OccurrenceEsField;
import org.gbif.occurrence.search.es.EsSearchRequestBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.print.attribute.standard.Media;
import javax.validation.constraints.Min;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.gbif.common.search.solr.SolrConstants.DEFAULT_FILTER_QUERY;
import static org.gbif.occurrence.search.es.OccurrenceEsField.*;

/**
 * Occurrence search service. Executes {@link OccurrenceSearchRequest} by transforming the request
 * into {@link SolrQuery}.
 */
public class OccurrenceSearchESImpl implements OccurrenceSearchService {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceSearchESImpl.class);

  private static final ObjectReader JSON_READER = new ObjectMapper().reader(Map.class);
  private static final DateFormat DATE_PATTERN =
      new SimpleDateFormat(
          "yyyy-MM-dd'T'HH:mm'Z'"); // Quoted "Z" to indicate UTC, no timezone offset
  private static final Function<JsonNode, Date> DATE_FORMAT =
      (jsonNode) ->
          Optional.ofNullable(jsonNode)
              .map(JsonNode::asText)
              .filter(dateAsString -> !Strings.isNullOrEmpty(dateAsString))
              .map(
                  dateAsString -> {
                    try {
                      return DATE_PATTERN.parse(dateAsString);
                    } catch (ParseException e) {
                      throw new IllegalStateException(e.getMessage(), e);
                    }
                  })
              .orElse(null);
  private static final String BLANK = " ";

  // functions
  private static final Function<String, String> SEARCH_ENDPOINT =
      (index) -> "/" + index + "/_search";
  private static final Supplier<Header[]> HEADERS =
      () ->
          new Header[] {
            new BasicHeader(HTTP.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString())
          };

  private final NameUsageMatchingService nameUsageMatchingService;
  private final RestClient esClient;
  private final EsConfig esConfig;

  @Inject
  public OccurrenceSearchESImpl(
      EsConfig esConfig,
      NameUsageMatchingService nameUsageMatchingService,
      @Named("max.offset") int maxOffset,
      @Named("max.limit") int maxLimit,
      @Named("facets.enable") boolean facetsEnable) {
    this.esConfig = esConfig;
    // create ES client
    esClient = RestClient.builder(createHosts(esConfig)).build();
    this.nameUsageMatchingService = nameUsageMatchingService;
  }

  private HttpHost[] createHosts(EsConfig esConfig) {
    HttpHost[] hosts = new HttpHost[esConfig.getHosts().length];
    int i = 0;
    for (String host : esConfig.getHosts()) {
      try {
        URL url = new URL(host);
        hosts[i] = new HttpHost(url.getHost(), url.getPort(), url.getProtocol());
        i++;
      } catch (MalformedURLException e) {
        throw new IllegalArgumentException(e.getMessage(), e);
      }
    }

    return hosts;
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
      throw new IllegalStateException(e.getMessage(), e);
    }

    // parse results
    final JsonNode hits = resp.path("hits");
    List<Occurrence> occurrences = new ArrayList<>();
    for (JsonNode record : hits.path("hits")) {
      final JsonNode source = record.path("_source");

      // create occurrence
      Occurrence occ = new Occurrence();
      occurrences.add(occ);

      // fill out fields
      occ.setBasisOfRecord(
          getValue(source, BASIS_OF_RECORD, (node -> BasisOfRecord.valueOf(node.asText()))));
      occ.setContinent(getValue(source, CONTINENT, (node -> Continent.valueOf(node.asText()))));
      // TODO: we dont have coordinateAccuracy
      occ.setCoordinatePrecision(getValue(source, COORDINATE_PRECISION, JsonNode::asDouble));
      occ.setCoordinateUncertaintyInMeters(
          getValue(source, COORDINATE_UNCERTAINTY_METERS, JsonNode::asDouble));
      occ.setCountry(
          getValue(source, COUNTRY, (node -> Country.valueOf(node.asText().toUpperCase()))));
      occ.setDateIdentified(DATE_FORMAT.apply(source.get(DATE_IDENTIFIED.getFieldName())));
      occ.setDay(getValue(source, DAY, JsonNode::asInt));
      occ.setMonth(getValue(source, MONTH, JsonNode::asInt));
      occ.setYear(getValue(source, YEAR, JsonNode::asInt));
      occ.setDecimalLatitude(getValue(source.path("location"), LATITUDE, JsonNode::asDouble));
      occ.setDecimalLongitude(getValue(source.path("location"), LONGITUDE, JsonNode::asDouble));
      // TODO: depth?? we only have minimum and maximum depth. Maybe take it from verbatim??

      // TODO: we dont have depthAccuracy
      // TODO: elevation from verbatim??
      // TODO: we dont have elevationAccuracy
      occ.setEstablishmentMeans(
          getValue(
              source, ESTABLISHMENT_MEANS, (node -> EstablishmentMeans.valueOf(node.asText()))));
      occ.setEventDate(DATE_FORMAT.apply(source.get(EVENT_DATE.getFieldName())));
      // TODO: what are facts??
      // TODO: identifiers??
      occ.setIndividualCount(getValue(source, INDIVIDUAL_COUNT, JsonNode::asInt));
      // TODO: issues missing in ES
      // TODO: we dont have lastInterpreted, lastCrawled and lastParsed
      // TODO: we dont have license
      occ.setLifeStage(getValue(source, LIFE_STAGE, (node -> LifeStage.valueOf(node.asText()))));
      occ.setModified(DATE_FORMAT.apply(source.get(MODIFIED.getFieldName())));
      occ.setReferences(getValue(source, REFERENCES, (node) -> URI.create(node.asText())));
      // TODO: what are relations??
      occ.setSex(getValue(source, SEX, (node -> Sex.valueOf(node.asText()))));

      // TODO: typifiedName
      // TODO: publishingCountry
      occ.setStateProvince(getValue(source, STATE_PROVINCE, JsonNode::asText));
      occ.setTypeStatus(getValue(source, TYPE_STATUS, (node -> TypeStatus.valueOf(node.asText()))));
      occ.setWaterBody(getValue(source, WATER_BODY, JsonNode::asText));

      // taxon
      setTaxonFields(occ, source);

      // multimedia
      occ.setMedia(parseMediaObjects(source));

      // metadata
      // TODO: add missing fields
      occ.setDatasetKey(getValue(source, DATASET_KEY, node -> UUID.fromString(node.asText())));
      occ.setInstallationKey(
          getValue(source, INSTALLATION_KEY, node -> UUID.fromString(node.asText())));
      occ.setPublishingOrgKey(
          getValue(source, PUBLISHING_ORGANIZATION_KEY, node -> UUID.fromString(node.asText())));
    }

    // create response
    SearchResponse<Occurrence, OccurrenceSearchParameter> response = new SearchResponse<>(request);
    response.setCount(hits.get("total").asLong());
    response.setResults(occurrences);

    return response;
  }

  private <T> T getValue(JsonNode source, OccurrenceEsField esField, Function<JsonNode, T> mapper) {
    return Optional.ofNullable(source.get(esField.getFieldName())).map(mapper).orElse(null);
  }

  private List<MediaObject> parseMediaObjects(JsonNode source) {
    List<MediaObject> mediaObjects = new ArrayList<>();

    source
        .path("multimediaItems")
        .forEach(
            multimedia -> {
              MediaObject mediaObject = new MediaObject();

              mediaObject.setFormat(multimedia.path("format").asText());
              mediaObject.setTitle(multimedia.path("title").asText());
              mediaObject.setDescription(multimedia.path("description").asText());
              mediaObject.setSource(multimedia.path("source").asText());
              mediaObject.setAudience(multimedia.path("audience").asText());
              mediaObject.setCreated(DATE_FORMAT.apply(multimedia.path("created")));
              mediaObject.setCreator(multimedia.path("creator").asText());
              mediaObject.setContributor(multimedia.path("contributor").asText());
              mediaObject.setPublisher(multimedia.path("publisher").asText());
              mediaObject.setLicense(multimedia.path("license").asText());
              mediaObject.setRightsHolder(multimedia.path("rightsHolder").asText());

              Optional.ofNullable(multimedia.path("type"))
                  .ifPresent(type -> mediaObject.setType(MediaType.valueOf(type.asText())));
              Optional.ofNullable(multimedia.path("identifier"))
                  .ifPresent(id -> mediaObject.setIdentifier(URI.create(id.asText())));
              Optional.ofNullable(multimedia.path("references"))
                  .ifPresent(ref -> mediaObject.setReferences(URI.create(ref.asText())));

              mediaObjects.add(mediaObject);
            });

    return mediaObjects;
  }

  private void setTaxonFields(Occurrence occ, JsonNode source) {
    // Taxon fields as map
    JsonNode classification = source.path("classification");
    Map<Rank, JsonNode> taxonFieldsMap = new EnumMap<>(Rank.class);
    classification.forEach(
        node -> taxonFieldsMap.put(Rank.valueOf(node.get("rank").asText()), node));

    Function<JsonNode, Integer> keyExtractor =
        json -> Optional.ofNullable(json).map(node -> node.get("key").asInt()).orElse(null);

    Function<JsonNode, String> nameExtractor =
        json -> Optional.ofNullable(json).map(node -> node.get("name").asText()).orElse(null);

    // class
    occ.setClassKey(keyExtractor.apply(taxonFieldsMap.get(Rank.CLASS)));
    occ.setClazz(nameExtractor.apply(taxonFieldsMap.get(Rank.CLASS)));
    // family
    occ.setFamilyKey(keyExtractor.apply(taxonFieldsMap.get(Rank.FAMILY)));
    occ.setFamily(nameExtractor.apply(taxonFieldsMap.get(Rank.FAMILY)));
    // genus
    occ.setGenusKey(keyExtractor.apply(taxonFieldsMap.get(Rank.GENUS)));
    occ.setGenus(nameExtractor.apply(taxonFieldsMap.get(Rank.GENUS)));
    // kingdom
    occ.setKingdomKey(keyExtractor.apply(taxonFieldsMap.get(Rank.KINGDOM)));
    occ.setKingdom(nameExtractor.apply(taxonFieldsMap.get(Rank.KINGDOM)));
    // order
    occ.setOrderKey(keyExtractor.apply(taxonFieldsMap.get(Rank.ORDER)));
    occ.setOrder(nameExtractor.apply(taxonFieldsMap.get(Rank.ORDER)));
    // phylum
    occ.setPhylumKey(keyExtractor.apply(taxonFieldsMap.get(Rank.PHYLUM)));
    occ.setPhylum(nameExtractor.apply(taxonFieldsMap.get(Rank.PHYLUM)));
    // species
    occ.setSpeciesKey(keyExtractor.apply(taxonFieldsMap.get(Rank.SPECIES)));
    occ.setSpecies(nameExtractor.apply(taxonFieldsMap.get(Rank.SPECIES)));
    // subgenus
    occ.setSubgenusKey(keyExtractor.apply(taxonFieldsMap.get(Rank.SUBGENUS)));
    occ.setSubgenus(nameExtractor.apply(taxonFieldsMap.get(Rank.SUBGENUS)));

    // usage
    final JsonNode usage = source.path("usage");
    if (usage != null) {
      occ.setScientificName(nameExtractor.apply(usage));
      occ.setTaxonKey(keyExtractor.apply(usage));
      Optional.ofNullable(usage.get("rank"))
          .ifPresent(rank -> occ.setTaxonRank(Rank.valueOf(rank.asText())));
    }

    // generic name is the same as genus
    occ.setGenericName(occ.getGenus());

    // specific epithet
    Optional.ofNullable(occ.getSpecies())
        .ifPresent(species -> occ.setSpecificEpithet(species.split(BLANK)[1]));

    // TODO: infraspecific epithet: it should be the rank after SPECIES and we take the 3rd name
  }

  @Override
  public SearchResponse<Occurrence, OccurrenceSearchParameter> search(
      @Nullable OccurrenceSearchRequest request) {

    if (hasReplaceableScientificNames(request)) {
      HttpEntity requestBody = EsSearchRequestBuilder.buildRequestBody(request);
      Response response = null;
      try {
        response =
            esClient.performRequest(
                "GET",
                SEARCH_ENDPOINT.apply(esConfig.getIndex()),
                Collections.emptyMap(),
                requestBody,
                HEADERS.get());
      } catch (IOException e) {
        LOG.error("Error executing the search operation", e);
        throw new SearchException(e);
      }

      return buildResponse(response, request);
    } else {
      return new SearchResponse<Occurrence, OccurrenceSearchParameter>(request);
    }
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
