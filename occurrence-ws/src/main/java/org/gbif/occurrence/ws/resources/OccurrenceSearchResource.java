package org.gbif.occurrence.ws.resources;


import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.occurrence.Occurrence;
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
import static org.gbif.ws.paths.OccurrencePaths.COLLECTOR_NAME_PATH;
import static org.gbif.ws.paths.OccurrencePaths.INSTITUTION_CODE_PATH;
import static org.gbif.ws.paths.OccurrencePaths.COLLECTION_CODE_PATH;
import static org.gbif.ws.paths.OccurrencePaths.OCC_SEARCH_PATH;


/**
 * Occurrence resource.
 */
@Path(OCC_SEARCH_PATH)
@Produces({MediaType.APPLICATION_JSON, ExtraMediaTypes.APPLICATION_JAVASCRIPT })
public class OccurrenceSearchResource {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceSearchResource.class);
  private final OccurrenceSearchService searchService;

  @Inject
  public OccurrenceSearchResource(OccurrenceSearchService searchService) {
    this.searchService = searchService;
  }

  @GET
  public PagingResponse<Occurrence> search(@Context OccurrenceSearchRequest request) {
    LOG.debug(
      "Exceuting query, parameters {}, limit {}, offset {}",
      new Object[] {request.getParameters(), request.getLimit(), request.getOffset()});
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
  public List<String>
    suggestCollectionCodes(@QueryParam(QUERY_PARAM) String prefix, @QueryParam(PARAM_LIMIT) int limit) {
    LOG.debug("Executing collection codes suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestCollectionCodes(prefix, limit);
  }

  @GET
  @Path(COLLECTOR_NAME_PATH)
  public List<String> suggestCollectorName(@QueryParam(QUERY_PARAM) String prefix, @QueryParam(PARAM_LIMIT) int limit) {
    LOG.debug("Executing collector name suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestCollectorNames(prefix, limit);
  }


  @GET
  @Path(INSTITUTION_CODE_PATH)
  public List<String>
    suggestInstitutionCodes(@QueryParam(QUERY_PARAM) String prefix, @QueryParam(PARAM_LIMIT) int limit) {
    LOG.debug("Executing institution codes suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestInstitutionCodes(prefix, limit);
  }
}
