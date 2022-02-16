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
package org.gbif.occurrence.ws.client;

import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.service.occurrence.OccurrenceSearchService;

import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.springframework.cloud.openfeign.SpringQueryMap;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import static org.gbif.api.model.common.paging.PagingConstants.PARAM_LIMIT;
import static org.gbif.api.model.common.search.SearchConstants.QUERY_PARAM;
import static org.gbif.ws.paths.OccurrencePaths.*;

/**
 * Ws client for {@link OccurrenceSearchService}.
 */
@RequestMapping(
  value = OCCURRENCE_PATH,
  produces = MediaType.APPLICATION_JSON_VALUE
)
public interface OccurrenceWsSearchClient extends  OccurrenceSearchService {

  String SEARCH_PATH ="search/";


  @Override
  default SearchResponse<Occurrence, OccurrenceSearchParameter> search(@SpringQueryMap OccurrenceSearchRequest occurrenceSearchRequest) {
    return search(occurrenceSearchRequest,
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.DATASET_KEY),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.YEAR),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.MONTH),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.EVENT_DATE),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.MODIFIED),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.EVENT_ID),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.PARENT_EVENT_ID),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.SAMPLING_PROTOCOL),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.LAST_INTERPRETED),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.DECIMAL_LATITUDE),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.DECIMAL_LONGITUDE),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.COORDINATE_UNCERTAINTY_IN_METERS),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.COUNTRY),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.CONTINENT),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.PUBLISHING_COUNTRY),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.ELEVATION),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.DEPTH),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.INSTITUTION_CODE),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.COLLECTION_CODE),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.CATALOG_NUMBER),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.RECORDED_BY),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.IDENTIFIED_BY),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.RECORD_NUMBER),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.BASIS_OF_RECORD),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.TAXON_KEY),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.ACCEPTED_TAXON_KEY),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.KINGDOM_KEY),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.PHYLUM_KEY),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.CLASS_KEY),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.ORDER_KEY),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.FAMILY_KEY),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.GENUS_KEY),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.SUBGENUS_KEY),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.SPECIES_KEY),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.SCIENTIFIC_NAME),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.VERBATIM_SCIENTIFIC_NAME),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.TAXON_ID),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.TAXONOMIC_STATUS),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.HAS_COORDINATE),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.GEOMETRY),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.HAS_GEOSPATIAL_ISSUE),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.ISSUE),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.TYPE_STATUS),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.MEDIA_TYPE),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.OCCURRENCE_ID),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.ESTABLISHMENT_MEANS),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.REPATRIATED),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.ORGANISM_ID),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.STATE_PROVINCE),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.WATER_BODY),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.LOCALITY),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.PROTOCOL),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.LICENSE),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.PUBLISHING_ORG),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.NETWORK_KEY),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.INSTALLATION_KEY),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.HOSTING_ORGANIZATION_KEY),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.CRAWL_ID),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.PROJECT_ID),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.PROGRAMME),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.ORGANISM_QUANTITY),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.ORGANISM_QUANTITY_TYPE),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.SAMPLE_SIZE_UNIT),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.SAMPLE_SIZE_VALUE),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.RELATIVE_ORGANISM_QUANTITY),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.COLLECTION_KEY),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.INSTITUTION_KEY),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.RECORDED_BY_ID),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.IDENTIFIED_BY_ID),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.OCCURRENCE_STATUS),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.GADM_GID),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.GADM_LEVEL_0_GID),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.GADM_LEVEL_1_GID),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.GADM_LEVEL_2_GID),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.GADM_LEVEL_3_GID),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.LIFE_STAGE),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.IS_IN_CLUSTER),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.DWCA_EXTENSION),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.DATASET_ID),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.DATASET_NAME),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.OTHER_CATALOG_NUMBERS),
                  occurrenceSearchRequest.getParameters().get(OccurrenceSearchParameter.PREPARATIONS)
                  );

  }

  @RequestMapping(
    method = RequestMethod.GET,
    value = SEARCH_PATH
  )
  @ResponseBody
  SearchResponse<Occurrence, OccurrenceSearchParameter> search(@SpringQueryMap OccurrenceSearchRequest occurrenceSearchRequest,
                                                               @RequestParam(value = "datasetKey", required = false) Set<String> datasetKey,
                                                               @RequestParam(value = "year", required = false) Set<String> year,
                                                               @RequestParam(value = "month", required = false) Set<String> month,
                                                               @RequestParam(value = "eventDate", required = false) Set<String> eventDate,
                                                               @RequestParam(value = "modified", required = false) Set<String> modified,
                                                               @RequestParam(value = "eventId", required = false) Set<String> eventId,
                                                               @RequestParam(value = "parentEventId", required = false) Set<String> parentEventId,
                                                               @RequestParam(value = "samplingProtocol", required = false) Set<String> samplingProtocol,
                                                               @RequestParam(value = "lastInterpreted", required = false) Set<String> lastInterpreted,
                                                               @RequestParam(value = "decimalLatitude", required = false) Set<String> decimalLatitude,
                                                               @RequestParam(value = "decimalLongitude", required = false) Set<String> decimalLongitude,
                                                               @RequestParam(value = "coordinateUncertaintyInMeters", required = false) Set<String> coordinateUncertaintyInMeters,
                                                               @RequestParam(value = "country", required = false) Set<String> country,
                                                               @RequestParam(value = "continent", required = false) Set<String> continent,
                                                               @RequestParam(value = "publishingCountry", required = false) Set<String> publishingCountry,
                                                               @RequestParam(value = "elevation", required = false) Set<String> elevation,
                                                               @RequestParam(value = "depth", required = false) Set<String> depth,
                                                               @RequestParam(value = "institutionCode", required = false) Set<String> institutionCode,
                                                               @RequestParam(value = "collectionCode", required = false) Set<String> collectionCode,
                                                               @RequestParam(value = "catalogNumber", required = false) Set<String> catalogNumber,
                                                               @RequestParam(value = "recordedBy", required = false) Set<String> recordedBy,
                                                               @RequestParam(value = "identifiedBy", required = false) Set<String> identifiedBy,
                                                               @RequestParam(value = "recordNumber", required = false) Set<String> recordNumber,
                                                               @RequestParam(value = "basisOfRecord", required = false) Set<String> basisOfRecord,
                                                               @RequestParam(value = "taxonKey", required = false) Set<String> taxonKey,
                                                               @RequestParam(value = "acceptedTaxonKey", required = false) Set<String> acceptedTaxonKey,
                                                               @RequestParam(value = "kingdomKey", required = false) Set<String> kingdomKey,
                                                               @RequestParam(value = "phylumKey", required = false) Set<String> phylumKey,
                                                               @RequestParam(value = "classKey", required = false) Set<String> classKey,
                                                               @RequestParam(value = "orderKey", required = false) Set<String> orderKey,
                                                               @RequestParam(value = "familyKey", required = false) Set<String> familyKey,
                                                               @RequestParam(value = "genusKey", required = false) Set<String> genusKey,
                                                               @RequestParam(value = "subgenusKey", required = false) Set<String> subgenusKey,
                                                               @RequestParam(value = "speciesKey", required = false) Set<String> speciesKey,
                                                               @RequestParam(value = "scientificName", required = false) Set<String> scientificName,
                                                               @RequestParam(value = "verbatimScientificName", required = false) Set<String> verbatimScientificName,
                                                               @RequestParam(value = "taxonId", required = false) Set<String> taxonId,
                                                               @RequestParam(value = "taxonomicStatus", required = false) Set<String> taxonomicStatus,
                                                               @RequestParam(value = "hasCoordinate", required = false) Set<String> hasCoordinate,
                                                               @RequestParam(value = "geometry", required = false) Set<String> geometry,
                                                               @RequestParam(value = "hasGeospatialIssue", required = false) Set<String> hasGeospatialIssue,
                                                               @RequestParam(value = "issue", required = false) Set<String> issue,
                                                               @RequestParam(value = "typeStatus", required = false) Set<String> typeStatus,
                                                               @RequestParam(value = "mediaType", required = false) Set<String> mediaType,
                                                               @RequestParam(value = "occurrenceId", required = false) Set<String> occurrenceId,
                                                               @RequestParam(value = "establishmentMeans", required = false) Set<String> establishmentMeans,
                                                               @RequestParam(value = "repatriated", required = false) Set<String> repatriated,
                                                               @RequestParam(value = "organismId", required = false) Set<String> organismId,
                                                               @RequestParam(value = "stateProvince", required = false) Set<String> stateProvince,
                                                               @RequestParam(value = "waterBody", required = false) Set<String> waterBody,
                                                               @RequestParam(value = "locality", required = false) Set<String> locality,
                                                               @RequestParam(value = "protocol", required = false) Set<String> protocol,
                                                               @RequestParam(value = "license", required = false) Set<String> license,
                                                               @RequestParam(value = "publishingOrg", required = false) Set<String> publishingOrg,
                                                               @RequestParam(value = "networkKey", required = false) Set<String> networkKey,
                                                               @RequestParam(value = "installationKey", required = false) Set<String> installationKey,
                                                               @RequestParam(value = "hostingOrganizationKey", required = false) Set<String> hostingOrganizationKey,
                                                               @RequestParam(value = "crawlId", required = false) Set<String> crawlId,
                                                               @RequestParam(value = "projectId", required = false) Set<String> projectId,
                                                               @RequestParam(value = "programme", required = false) Set<String> programme,
                                                               @RequestParam(value = "organismQuantity", required = false) Set<String> organismQuantity,
                                                               @RequestParam(value = "organismQuantityType", required = false) Set<String> organismQuantityType,
                                                               @RequestParam(value = "sampleSizeUnit", required = false) Set<String> sampleSizeUnit,
                                                               @RequestParam(value = "sampleSizeValue", required = false) Set<String> sampleSizeValue,
                                                               @RequestParam(value = "relativeOrganismQuantity", required = false) Set<String> relativeOrganismQuantity,
                                                               @RequestParam(value = "collectionKey", required = false) Set<String> collectionKey,
                                                               @RequestParam(value = "institutionKey", required = false) Set<String> institutionKey,
                                                               @RequestParam(value = "recordedById", required = false) Set<String> recordedById,
                                                               @RequestParam(value = "identifiedById", required = false) Set<String> identifiedById,
                                                               @RequestParam(value = "occurrenceStatus", required = false) Set<String> occurrenceStatus,
                                                               @RequestParam(value = "gadmGid", required = false) Set<String> gadmGid,
                                                               @RequestParam(value = "gadmLevel0Gid", required = false) Set<String> gadmLevel0Gid,
                                                               @RequestParam(value = "gadmLevel1Gid", required = false) Set<String> gadmLevel1Gid,
                                                               @RequestParam(value = "gadmLevel2Gid", required = false) Set<String> gadmLevel2Gid,
                                                               @RequestParam(value = "gadmLevel3Gid", required = false) Set<String> gadmLevel3Gid,
                                                               @RequestParam(value = "lifeStage", required = false) Set<String> lifeStage,
                                                               @RequestParam(value = "isInCluster", required = false) Set<String> isInCluster,
                                                               @RequestParam(value = "dwcaExtension", required = false) Set<String> dwcaExtension,
                                                               @RequestParam(value = "datasetID", required = false) Set<String> datasetID,
                                                               @RequestParam(value = "datasetName", required = false) Set<String> datasetName,
                                                               @RequestParam(value = "otherCatalogNumbers", required = false) Set<String> otherCatalogNumbers,
                                                               @RequestParam(value = "preparations", required = false) Set<String> preparations);


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

  @RequestMapping(
    method = RequestMethod.GET,
    value = SEARCH_PATH + DATASET_NAME_PATH
  )
  @ResponseBody
  @Override
  List<String> suggestDatasetName(@RequestParam(QUERY_PARAM) String prefix, @RequestParam(PARAM_LIMIT) @Nullable Integer limit);

  @RequestMapping(
    method = RequestMethod.GET,
    value = SEARCH_PATH + OTHER_CATALOG_NUMBERS_PATH
  )
  @ResponseBody
  @Override
  List<String> suggestOtherCatalogNumbers(@RequestParam(QUERY_PARAM) String prefix, @RequestParam(PARAM_LIMIT) @Nullable Integer limit);

}
