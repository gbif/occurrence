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

import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.view.RedirectView;

/**
 * Occurrence resource.
 */
@RestController
@RequestMapping(
  value = OCC_SEARCH_PATH,
  produces = {MediaType.APPLICATION_JSON_VALUE, "application/x-javascript"}
)
public class OccurrenceSearchResource {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceSearchResource.class);

  private static final String USER_ROLE = "USER";

  private final OccurrenceSearchService searchService;

  @Autowired
  public OccurrenceSearchResource(OccurrenceSearchService searchService, DownloadRequestService downloadRequestService) {
    this.searchService = searchService;
  }

  @GetMapping
  public SearchResponse<Occurrence,OccurrenceSearchParameter> search(OccurrenceSearchRequest request) {
     LOG.debug("Executing query, parameters {}, limit {}, offset {}", request.getParameters(), request.getLimit(),
              request.getOffset());
    return searchService.search(request);
  }

  /**
   * Remove after the portal is updated, e.g. during or after December 2018.RegistryMethodSecurityConfiguration
   *
   * Old location for a GET download, doesn't make sense to be within occurrence search.
   */
  @GetMapping("download")
  @Deprecated
  @Secured(USER_ROLE)
  public RedirectView download() {
    LOG.warn("Deprecated internal API used! (download)");
    RedirectView redirectView = new RedirectView();
    redirectView.setContextRelative(true);
    redirectView.setUrl("/occurrence/download/request");
    redirectView.setPropagateQueryParams(true);
    redirectView.setStatusCode(HttpStatus.TEMPORARY_REDIRECT);
    return redirectView;
  }

  /**
   * Remove after the portal is updated, e.g. during or after December 2018.
   *
   * Old location for a GET download predicate request, doesn't make sense to be within occurrence search.
   */
  @GetMapping("predicate")
  @Deprecated
  public RedirectView downloadPredicate() {
    LOG.warn("Deprecated internal API used! (predicate)");
    RedirectView redirectView = new RedirectView();
    redirectView.setContextRelative(true);
    redirectView.setUrl("/occurrence/download/request/predicate");
    redirectView.setPropagateQueryParams(true);
    redirectView.setStatusCode(HttpStatus.TEMPORARY_REDIRECT);
    return redirectView;
  }

  @GetMapping(CATALOG_NUMBER_PATH)
  @ResponseBody
  public List<String> suggestCatalogNumber(@RequestParam(QUERY_PARAM) String prefix, @RequestParam(PARAM_LIMIT) int limit) {
    LOG.debug("Executing catalog number suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestCatalogNumbers(prefix, limit);
  }

  @GetMapping(COLLECTION_CODE_PATH)
  @ResponseBody
  public List<String> suggestCollectionCodes(@RequestParam(QUERY_PARAM) String prefix,
                                             @RequestParam(PARAM_LIMIT) int limit) {
    LOG.debug("Executing collection codes suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestCollectionCodes(prefix, limit);
  }

  @GetMapping(RECORDED_BY_PATH)
  @ResponseBody
  public List<String> suggestRecordedBy(@RequestParam(QUERY_PARAM) String prefix, @RequestParam(PARAM_LIMIT) int limit) {
    LOG.debug("Executing recorded_by suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestRecordedBy(prefix, limit);
  }

  @GetMapping(RECORD_NUMBER_PATH)
  @ResponseBody
  public List<String> suggestRecordNumbers(@RequestParam(QUERY_PARAM) String prefix, @RequestParam(PARAM_LIMIT) int limit) {
    LOG.debug("Executing record number suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestRecordNumbers(prefix, limit);
  }

  @GetMapping(INSTITUTION_CODE_PATH)
  @ResponseBody
  public List<String> suggestInstitutionCodes(@RequestParam(QUERY_PARAM) String prefix,
                                              @RequestParam(PARAM_LIMIT) int limit) {
    LOG.debug("Executing institution codes suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestInstitutionCodes(prefix, limit);
  }

  @GetMapping(OCCURRENCE_ID_PATH)
  @ResponseBody
  public List<String> suggestOccurrenceIds(@RequestParam(QUERY_PARAM) String prefix, @RequestParam(PARAM_LIMIT) int limit) {
    LOG.debug("Executing occurrenceId suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestOccurrenceIds(prefix, limit);
  }

  @GetMapping(ORGANISM_ID_PATH)
  @ResponseBody
  public List<String> suggestOrganismIds(@RequestParam(QUERY_PARAM) String prefix, @RequestParam(PARAM_LIMIT) int limit) {
    LOG.debug("Executing organismId suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestOrganismIds(prefix, limit);
  }

  @GetMapping(LOCALITY_PATH)
  @ResponseBody
  public List<String> suggestLocality(@RequestParam(QUERY_PARAM) String prefix, @RequestParam(PARAM_LIMIT) int limit) {
    LOG.debug("Executing locality suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestLocalities(prefix, limit);
  }

  @GetMapping(STATE_PROVINCE_PATH)
  @ResponseBody
  public List<String> suggestStateProvince(@RequestParam(QUERY_PARAM) String prefix, @RequestParam(PARAM_LIMIT) int limit) {
    LOG.debug("Executing stateProvince suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestStateProvinces(prefix, limit);
  }

  @GetMapping(WATER_BODY_PATH)
  @ResponseBody
  public List<String> suggestWaterBody(@RequestParam(QUERY_PARAM) String prefix, @RequestParam(PARAM_LIMIT) int limit) {
    LOG.debug("Executing waterBody suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestWaterBodies(prefix, limit);
  }
}
