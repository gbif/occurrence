package org.gbif.occurrence.search.es;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.api.model.checklistbank.NameUsageMatch.MatchType;
import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.service.checklistbank.NameUsageMatchingService;
import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.gbif.occurrence.search.SearchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.gbif.occurrence.search.es.EsQueryUtils.HEADERS;

/** Occurrence search service. */
public class OccurrenceSearchEsImpl implements OccurrenceSearchService {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceSearchEsImpl.class);

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

      return EsResponseParser.buildResponse(response, request);
    }
    return new SearchResponse<>(request);
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
    //  String solrField = SEARCH_TO_ES_MAPPING.get(parameter).getFieldName();
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
    //    // return default query for empty queries
    //    String qValue = Strings.nullToEmpty(q).trim();
    //    if (Strings.isNullOrEmpty(qValue)) {
    //      return DEFAULT_FILTER_QUERY;
    //    }
    //    // If default query was sent, must not be escaped
    //    if (!qValue.equals(DEFAULT_FILTER_QUERY)) {
    //      qValue = QueryUtils.clearConsecutiveBlanks(qValue);
    //      qValue = QueryUtils.escapeQuery(qValue);
    //    }
    //
    //    return qValue;
    return null;
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
