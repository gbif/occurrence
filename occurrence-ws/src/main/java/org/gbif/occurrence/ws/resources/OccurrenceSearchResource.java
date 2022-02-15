/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.ws.resources;

import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.search.OccurrencePredicateSearchRequest;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.gbif.api.util.VocabularyUtils;
import org.gbif.occurrence.search.SearchTermService;

import java.util.List;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.view.RedirectView;

import static org.gbif.api.model.common.paging.PagingConstants.PARAM_LIMIT;
import static org.gbif.api.model.common.search.SearchConstants.QUERY_PARAM;
import static org.gbif.ws.paths.OccurrencePaths.*;

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

  private final SearchTermService searchTermService;

  @Autowired
  public OccurrenceSearchResource(OccurrenceSearchService searchService, SearchTermService searchTermService) {
    this.searchService = searchService;
    this.searchTermService = searchTermService;
  }

  @PostMapping("predicate")
  public SearchResponse<Occurrence,OccurrenceSearchParameter> search(@NotNull @Valid @RequestBody OccurrencePredicateSearchRequest request) {
     LOG.debug("Executing query, predicate {}, limit {}, offset {}", request.getPredicate(), request.getLimit(),
              request.getOffset());
    return searchService.search(request);
  }

  @PostMapping
  public SearchResponse<Occurrence,OccurrenceSearchParameter> postSearch(@NotNull @Valid @RequestBody OccurrenceSearchRequest request) {
    LOG.debug("Executing query, parameters {}, limit {}, offset {}", request.getParameters(), request.getLimit(),
              request.getOffset());
    return searchService.search(request);
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

  @GetMapping(IDENTIFIED_BY_PATH)
  @ResponseBody
  public List<String> suggestIdentifiedBy(@RequestParam(QUERY_PARAM) String prefix, @RequestParam(PARAM_LIMIT) int limit) {
    LOG.debug("Executing recorded_by suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestIdentifiedBy(prefix, limit);
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


  @GetMapping(SAMPLING_PROTOCOL_PATH)
  @ResponseBody
  public List<String> suggestSamplingProtocol(@RequestParam(QUERY_PARAM) String prefix, @RequestParam(PARAM_LIMIT) int limit) {
    LOG.debug("Executing samplingProtocol suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestSamplingProtocol(prefix, limit);
  }

  @GetMapping(EVENT_ID_PATH)
  @ResponseBody
  public List<String> suggestEventId(@RequestParam(QUERY_PARAM) String prefix, @RequestParam(PARAM_LIMIT) int limit) {
    LOG.debug("Executing eventId suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestEventId(prefix, limit);
  }

  @GetMapping(PARENT_EVENT_ID_PATH)
  @ResponseBody
  public List<String> suggestParentEventId(@RequestParam(QUERY_PARAM) String prefix, @RequestParam(PARAM_LIMIT) int limit) {
    LOG.debug("Executing parentEventId suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestParentEventId(prefix, limit);
  }

  @GetMapping(DATASET_NAME_PATH)
  @ResponseBody
  public List<String> suggestDatasetName(@RequestParam(QUERY_PARAM) String prefix, @RequestParam(PARAM_LIMIT) int limit) {
    LOG.debug("Executing datasetName suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestDatasetName(prefix, limit);
  }

  @GetMapping(OTHER_CATALOG_NUMBERS_PATH)
  @ResponseBody
  public List<String> suggestOtherCatalogNumbers(@RequestParam(QUERY_PARAM) String prefix, @RequestParam(PARAM_LIMIT) int limit) {
    LOG.debug("Executing otherCatalogNumbers suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestOtherCatalogNumbers(prefix, limit);
  }

  @GetMapping("experimental/term/{term}")
  @ResponseBody
  public List<String> searchTerm(@PathVariable("term") String term, @RequestParam(QUERY_PARAM) String query, @RequestParam(PARAM_LIMIT) int limit) {
    LOG.debug("Executing term suggest/search, term {}, query {}, limit {}", term, query, limit);
    return
      VocabularyUtils.lookup(term, OccurrenceSearchParameter.class)
        .map(parameter -> searchTermService.searchFieldTerms(query, parameter, limit))
        .orElseThrow(() -> new IllegalArgumentException("Search not supported for term " +  term));
  }
}
