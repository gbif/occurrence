package org.gbif.occurrence.ws.resources;


import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.gbif.ws.util.ExtraMediaTypes;

import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.api.model.common.paging.PagingConstants.PARAM_LIMIT;
import static org.gbif.api.model.common.search.SearchConstants.QUERY_PARAM;
import static org.gbif.ws.paths.OccurrencePaths.CATALOG_NUMBER_PATH;
import static org.gbif.ws.paths.OccurrencePaths.COLLECTION_CODE_PATH;
import static org.gbif.ws.paths.OccurrencePaths.INSTITUTION_CODE_PATH;
import static org.gbif.ws.paths.OccurrencePaths.OCC_SEARCH_PATH;
import static org.gbif.ws.paths.OccurrencePaths.RECORDED_BY_PATH;
import static org.gbif.ws.paths.OccurrencePaths.RECORD_NUMBER_PATH;
import static org.gbif.ws.paths.OccurrencePaths.OCCURRENCE_ID_PATH;
import static org.gbif.ws.paths.OccurrencePaths.ORGANISM_ID_PATH;
import static org.gbif.ws.paths.OccurrencePaths.WATER_BODY_PATH;
import static org.gbif.ws.paths.OccurrencePaths.STATE_PROVINCE_PATH;
import static org.gbif.ws.paths.OccurrencePaths.LOCALITY_PATH;


/**
 * Occurrence resource.
 */
@Path(OCC_SEARCH_PATH)
@Produces({MediaType.APPLICATION_JSON, ExtraMediaTypes.APPLICATION_JAVASCRIPT})
public class OccurrenceSearchResource {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceSearchResource.class);
  private final OccurrenceSearchService searchService;

  @Inject
  public OccurrenceSearchResource(OccurrenceSearchService searchService) {
    this.searchService = searchService;
  }

  @GET
  public SearchResponse<Occurrence,OccurrenceSearchParameter> search(@Context OccurrenceSearchRequest request) {
    LOG.debug("Executing query, parameters {}, limit {}, offset {}", request.getParameters(), request.getLimit(),
              request.getOffset());
    return searchService.search(request);
  }

  @GET
  @Path(CATALOG_NUMBER_PATH)
  public List<String> suggestCatalogNumber(@QueryParam(QUERY_PARAM) String prefix, @QueryParam(PARAM_LIMIT) int limit) {
    LOG.debug("Executing catalog number suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestCatalogNumbers(prefix, limit);
  }

  @GET
  @Path(COLLECTION_CODE_PATH)
  public List<String> suggestCollectionCodes(@QueryParam(QUERY_PARAM) String prefix,
                                             @QueryParam(PARAM_LIMIT) int limit) {
    LOG.debug("Executing collection codes suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestCollectionCodes(prefix, limit);
  }

  @GET
  @Path(RECORDED_BY_PATH)
  public List<String> suggestRecordedBy(@QueryParam(QUERY_PARAM) String prefix, @QueryParam(PARAM_LIMIT) int limit) {
    LOG.debug("Executing recorded_by suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestRecordedBy(prefix, limit);
  }


  @GET
  @Path(RECORD_NUMBER_PATH)
  public List<String> suggestRecordNumbers(@QueryParam(QUERY_PARAM) String prefix, @QueryParam(PARAM_LIMIT) int limit) {
    LOG.debug("Executing record number suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestRecordNumbers(prefix, limit);
  }


  @GET
  @Path(INSTITUTION_CODE_PATH)
  public List<String> suggestInstitutionCodes(@QueryParam(QUERY_PARAM) String prefix,
                                              @QueryParam(PARAM_LIMIT) int limit) {
    LOG.debug("Executing institution codes suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestInstitutionCodes(prefix, limit);
  }

  @GET
  @Path(OCCURRENCE_ID_PATH)
  public List<String> suggestOccurrenceIds(@QueryParam(QUERY_PARAM) String prefix, @QueryParam(PARAM_LIMIT) int limit) {
    LOG.debug("Executing occurrenceId suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestOccurrenceIds(prefix, limit);
  }

  @GET
  @Path(ORGANISM_ID_PATH)
  public List<String> suggestOrganismIds(@QueryParam(QUERY_PARAM) String prefix, @QueryParam(PARAM_LIMIT) int limit) {
    LOG.debug("Executing organismId suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestOrganismIds(prefix, limit);
  }

  @GET
  @Path(LOCALITY_PATH)
  public List<String> suggestLocality(@QueryParam(QUERY_PARAM) String prefix, @QueryParam(PARAM_LIMIT) int limit) {
    LOG.debug("Executing locality suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestLocalities(prefix, limit);
  }

  @GET
  @Path(STATE_PROVINCE_PATH)
  public List<String> suggestStateProvince(@QueryParam(QUERY_PARAM) String prefix, @QueryParam(PARAM_LIMIT) int limit) {
    LOG.debug("Executing stateProvince suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestStateProvinces(prefix, limit);
  }

  @GET
  @Path(WATER_BODY_PATH)
  public List<String> suggestWaterBody(@QueryParam(QUERY_PARAM) String prefix, @QueryParam(PARAM_LIMIT) int limit) {
    LOG.debug("Executing waterBody suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestWaterBodies(prefix, limit);
  }

}
