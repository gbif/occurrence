package org.gbif.occurrence.ws.resources;

import static org.gbif.api.model.common.paging.PagingConstants.PARAM_LIMIT;
import static org.gbif.api.model.common.search.SearchConstants.QUERY_PARAM;
import static org.gbif.ws.paths.OccurrencePaths.CATALOG_NUMBER_PATH;
import static org.gbif.ws.paths.OccurrencePaths.COLLECTION_CODE_PATH;
import static org.gbif.ws.paths.OccurrencePaths.INSTITUTION_CODE_PATH;
import static org.gbif.ws.paths.OccurrencePaths.LOCALITY_PATH;
import static org.gbif.ws.paths.OccurrencePaths.OCCURRENCE_ID_PATH;
import static org.gbif.ws.paths.OccurrencePaths.OCC_SEARCH_PATH;
import static org.gbif.ws.paths.OccurrencePaths.ORGANISM_ID_PATH;
import static org.gbif.ws.paths.OccurrencePaths.RECORDED_BY_PATH;
import static org.gbif.ws.paths.OccurrencePaths.RECORD_NUMBER_PATH;
import static org.gbif.ws.paths.OccurrencePaths.STATE_PROVINCE_PATH;
import static org.gbif.ws.paths.OccurrencePaths.WATER_BODY_PATH;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.gbif.ws.util.ExtraMediaTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.inject.Inject;

/**
 * Occurrence resource.
 */
@Path(OCC_SEARCH_PATH)
@Produces({MediaType.APPLICATION_JSON, ExtraMediaTypes.APPLICATION_JAVASCRIPT})
public class OccurrenceSearchResource {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceSearchResource.class);

  private final OccurrenceSearchService searchService;

  @Inject
  public OccurrenceSearchResource(OccurrenceSearchService searchService, DownloadRequestService downloadRequestService) {
    this.searchService = searchService;
  }

  @GET
  public SearchResponse<Occurrence,OccurrenceSearchParameter> search(@Context OccurrenceSearchRequest request) {
    LOG.debug("Executing query, parameters {}, limit {}, offset {}", request.getParameters(), request.getLimit(),
              request.getOffset());
    return searchService.search(request);
  }

  /**
   * Remove after the portal is updated, e.g. during or after December 2018.
   *
   * Old location for a GET download, doesn't make sense to be within occurrence search.
   */
  @GET
  @Path("download")
  @Deprecated
  public Response download(@Context HttpServletRequest httpRequest,
                         @QueryParam("notification_address") String emails,
                         @QueryParam("format") String format,
                         @Context UriInfo uriInfo) {
    LOG.warn("Deprecated internal API used! (download)");
    return Response.status(Response.Status.TEMPORARY_REDIRECT).location(uriInfo.getBaseUri().resolve("occurrence/download/request?"+uriInfo.getRequestUri().getQuery())).build();
  }

  /**
   * Remove after the portal is updated, e.g. during or after December 2018.
   *
   * Old location for a GET download predicate request, doesn't make sense to be within occurrence search.
   */
  @GET
  @Path("predicate")
  @Deprecated
  public Response downloadPredicate(@Context HttpServletRequest httpRequest,
                         @QueryParam("notification_address") String emails,
                         @QueryParam("format") String format,
                         @Context UriInfo uriInfo) {
    LOG.warn("Deprecated internal API used! (predicate)");
    return Response.status(Response.Status.TEMPORARY_REDIRECT).location(uriInfo.getBaseUri().resolve("occurrence/download/request?"+uriInfo.getRequestUri().getQuery())).build();
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
