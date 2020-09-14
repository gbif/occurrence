package org.gbif.occurrence.ws.client;

import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.service.occurrence.OccurrenceSearchService;

import java.util.List;

import javax.annotation.Nullable;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import static org.gbif.api.model.common.paging.PagingConstants.PARAM_LIMIT;
import static org.gbif.api.model.common.search.SearchConstants.QUERY_PARAM;
import static org.gbif.ws.paths.OccurrencePaths.CATALOG_NUMBER_PATH;
import static org.gbif.ws.paths.OccurrencePaths.COLLECTION_CODE_PATH;
import static org.gbif.ws.paths.OccurrencePaths.EVENT_ID_PATH;
import static org.gbif.ws.paths.OccurrencePaths.IDENTIFIED_BY_PATH;
import static org.gbif.ws.paths.OccurrencePaths.INSTITUTION_CODE_PATH;
import static org.gbif.ws.paths.OccurrencePaths.OCCURRENCE_PATH;
import static org.gbif.ws.paths.OccurrencePaths.PARENT_EVENT_ID_PATH;
import static org.gbif.ws.paths.OccurrencePaths.RECORDED_BY_PATH;
import static org.gbif.ws.paths.OccurrencePaths.RECORD_NUMBER_PATH;
import static org.gbif.ws.paths.OccurrencePaths.OCCURRENCE_ID_PATH;
import static org.gbif.ws.paths.OccurrencePaths.ORGANISM_ID_PATH;
import static org.gbif.ws.paths.OccurrencePaths.LOCALITY_PATH;
import static org.gbif.ws.paths.OccurrencePaths.SAMPLING_PROTOCOL_PATH;
import static org.gbif.ws.paths.OccurrencePaths.WATER_BODY_PATH;
import static org.gbif.ws.paths.OccurrencePaths.STATE_PROVINCE_PATH;

/**
 * Ws client for {@link OccurrenceSearchService}.
 */
@RequestMapping(
  value = OCCURRENCE_PATH,
  produces = MediaType.APPLICATION_JSON_VALUE
)
public interface OccurrenceWsSearchClient extends  OccurrenceSearchService {

  String SEARCH_PATH ="search/";

  @RequestMapping(
    method = RequestMethod.GET,
    value = SEARCH_PATH
  )
  @ResponseBody
  @Override
  SearchResponse<Occurrence, OccurrenceSearchParameter> search(@RequestBody OccurrenceSearchRequest occurrenceSearchRequest);

  @RequestMapping(
    method = RequestMethod.GET,
    value = CATALOG_NUMBER_PATH
  )
  @ResponseBody
  @Override
  List<String> suggestCatalogNumbers(@RequestParam(QUERY_PARAM) String prefix, @RequestParam(PARAM_LIMIT) @Nullable Integer limit);


  @RequestMapping(
    method = RequestMethod.GET,
    value = SEARCH_PATH + COLLECTION_CODE_PATH
  )
  @ResponseBody
  @Override
  List<String> suggestCollectionCodes(@RequestParam(QUERY_PARAM) String prefix, @RequestParam(PARAM_LIMIT) @Nullable Integer limit);

  @RequestMapping(
    method = RequestMethod.GET,
    value = SEARCH_PATH + RECORDED_BY_PATH
  )
  @ResponseBody
  @Override
  List<String> suggestRecordedBy(@RequestParam(QUERY_PARAM) String prefix, @RequestParam(PARAM_LIMIT) @Nullable Integer limit);

  @RequestMapping(
    method = RequestMethod.GET,
    value = SEARCH_PATH + RECORD_NUMBER_PATH
  )
  @ResponseBody
  @Override
  List<String> suggestRecordNumbers(@RequestParam(QUERY_PARAM) String prefix, @RequestParam(PARAM_LIMIT) @Nullable Integer limit);

  @RequestMapping(
    method = RequestMethod.GET,
    value = SEARCH_PATH + INSTITUTION_CODE_PATH
  )
  @ResponseBody
  @Override
  List<String> suggestInstitutionCodes(@RequestParam(QUERY_PARAM) String prefix, @RequestParam(PARAM_LIMIT) @Nullable Integer limit);

  @RequestMapping(
    method = RequestMethod.GET,
    value = SEARCH_PATH + OCCURRENCE_ID_PATH
  )
  @ResponseBody
  @Override
  List<String> suggestOccurrenceIds(@RequestParam(QUERY_PARAM) String prefix, @RequestParam(PARAM_LIMIT) @Nullable Integer limit);

  @RequestMapping(
    method = RequestMethod.GET,
    value = SEARCH_PATH + ORGANISM_ID_PATH
  )
  @ResponseBody
  @Override
  List<String> suggestOrganismIds(@RequestParam(QUERY_PARAM) String prefix, @RequestParam(PARAM_LIMIT) @Nullable Integer limit);

  @RequestMapping(
    method = RequestMethod.GET,
    value = SEARCH_PATH + LOCALITY_PATH
  )
  @ResponseBody
  @Override
  List<String> suggestLocalities(@RequestParam(QUERY_PARAM) String prefix, @RequestParam(PARAM_LIMIT) @Nullable Integer limit);

  @RequestMapping(
    method = RequestMethod.GET,
    value = SEARCH_PATH + WATER_BODY_PATH
  )
  @ResponseBody
  @Override
  List<String> suggestWaterBodies(@RequestParam(QUERY_PARAM) String prefix, @RequestParam(PARAM_LIMIT) @Nullable Integer limit);

  @RequestMapping(
    method = RequestMethod.GET,
    value = SEARCH_PATH + STATE_PROVINCE_PATH
  )
  @ResponseBody
  @Override
  List<String> suggestStateProvinces(@RequestParam(QUERY_PARAM) String prefix, @RequestParam(PARAM_LIMIT) @Nullable Integer limit);

  @RequestMapping(
    method = RequestMethod.GET,
    value = SEARCH_PATH + IDENTIFIED_BY_PATH
  )
  @ResponseBody
  @Override
  List<String> suggestIdentifiedBy(@RequestParam(QUERY_PARAM) String prefix, @RequestParam(PARAM_LIMIT) @Nullable Integer limit);

  @RequestMapping(
    method = RequestMethod.GET,
    value = SEARCH_PATH + SAMPLING_PROTOCOL_PATH
  )
  @ResponseBody
  @Override
  List<String> suggestSamplingProtocol(@RequestParam(QUERY_PARAM) String prefix, @RequestParam(PARAM_LIMIT) @Nullable Integer limit);

  @RequestMapping(
    method = RequestMethod.GET,
    value = SEARCH_PATH + EVENT_ID_PATH
  )
  @ResponseBody
  @Override
  List<String> suggestEventId(@RequestParam(QUERY_PARAM) String prefix, @RequestParam(PARAM_LIMIT) @Nullable Integer limit);

  @RequestMapping(
    method = RequestMethod.GET,
    value = SEARCH_PATH + PARENT_EVENT_ID_PATH
  )
  @ResponseBody
  @Override
  List<String> suggestParentEventId(@RequestParam(QUERY_PARAM) String prefix, @RequestParam(PARAM_LIMIT) @Nullable Integer limit);

}
