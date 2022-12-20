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
package org.gbif.event.search.es;

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GadmTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.IucnTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.UnknownTerm;
import org.gbif.occurrence.search.es.BaseEsField;
import org.gbif.occurrence.search.es.EsField;
import org.gbif.occurrence.search.es.OccurrenceBaseEsFieldMapper;

import java.util.Optional;
import java.util.Set;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/** Enum that contains the mapping of symbolic names and field names of valid Elasticsearch fields. */
public enum EventEsField implements EsField {

  ID(new BaseEsField("id", DcTerm.identifier)),

  //Dataset derived
  DATASET_KEY(new BaseEsField("metadata.datasetKey", GbifTerm.datasetKey)),
  PUBLISHING_COUNTRY(new BaseEsField("metadata.publishingCountry", GbifTerm.publishingCountry)),
  PUBLISHING_ORGANIZATION_KEY(new BaseEsField("metadata.publishingOrganizationKey", GbifInternalTerm.publishingOrgKey)),
  HOSTING_ORGANIZATION_KEY(new BaseEsField("metadata.hostingOrganizationKey", GbifInternalTerm.hostingOrganizationKey)),
  INSTALLATION_KEY(new BaseEsField("metadata.installationKey", GbifInternalTerm.installationKey)),
  NETWORK_KEY(new BaseEsField("metadata.networkKeys", GbifInternalTerm.networkKey)),
  PROTOCOL(new BaseEsField("metadata.protocol", GbifTerm.protocol)),
  LICENSE(new BaseEsField("metadata.license", DcTerm.license)),
  PROJECT_ID(new BaseEsField("metadata.projectId", GbifInternalTerm.projectId)),
  PROGRAMME(new BaseEsField("metadata.programmeAcronym", GbifInternalTerm.programmeAcronym)),

  //Core identification
  INSTITUTION_CODE(new BaseEsField("event.institutionCode", DwcTerm.institutionCode, true)),
  COLLECTION_CODE(new BaseEsField("event.collectionCode", DwcTerm.collectionCode, true)),
  CATALOG_NUMBER(new BaseEsField("event.catalogNumber", DwcTerm.catalogNumber, true)),

  ORGANISM_ID(new BaseEsField("event.organismId", DwcTerm.organismID, true)),
  OCCURRENCE_ID(new BaseEsField("event.occurrenceId", DwcTerm.occurrenceID, true)),
  RECORDED_BY(new BaseEsField("event.recordedBy", DwcTerm.recordedBy, true)),
  IDENTIFIED_BY(new BaseEsField("event.identifiedBy", DwcTerm.identifiedBy, true)),
  RECORDED_BY_ID(new BaseEsField("event.recordedByIds", "event.recordedByIds.value", DwcTerm.recordedByID)),
  IDENTIFIED_BY_ID(new BaseEsField("event.identifiedByIds", "event.identifiedByIds.value", DwcTerm.identifiedByID)),
  RECORD_NUMBER(new BaseEsField("event.recordNumber", DwcTerm.recordNumber, true)),
  BASIS_OF_RECORD(new BaseEsField("event.basisOfRecord", DwcTerm.basisOfRecord)),
  TYPE_STATUS(new BaseEsField("event.typeStatus", DwcTerm.typeStatus)),
  OCCURRENCE_STATUS(new BaseEsField("event.occurrenceStatus", DwcTerm.occurrenceStatus)),
  DATASET_ID(new BaseEsField("event.datasetID", DwcTerm.datasetID)),
  DATASET_NAME(new BaseEsField("event.datasetName", DwcTerm.datasetName, true)),
  OTHER_CATALOG_NUMBERS(new BaseEsField("event.otherCatalogNumbers", DwcTerm.otherCatalogNumbers, true)),
  PREPARATIONS(new BaseEsField("event.preparations", DwcTerm.preparations, true)),

  //Temporal
  YEAR(new BaseEsField("event.year", DwcTerm.year)),
  MONTH(new BaseEsField("event.month", DwcTerm.month)),
  DAY(new BaseEsField("event.day", DwcTerm.day)),
  EVENT_DATE(new BaseEsField("event.eventDateSingle", DwcTerm.eventDate)),

  //Location
  COORDINATE_SHAPE(new BaseEsField("event.scoordinates", null)),
  COORDINATE_POINT(new BaseEsField("event.coordinates", null)),
  LATITUDE(new BaseEsField("event.decimalLatitude", DwcTerm.decimalLatitude)),
  LONGITUDE(new BaseEsField("event.decimalLongitude", DwcTerm.decimalLongitude)),
  COUNTRY_CODE(new BaseEsField("event.countryCode", DwcTerm.countryCode)),
  CONTINENT(new BaseEsField("event.continent", DwcTerm.continent)),
  COORDINATE_ACCURACY(new BaseEsField("event.coordinateAccuracy", GbifTerm.coordinateAccuracy)),
  ELEVATION_ACCURACY(new BaseEsField("event.elevationAccuracy", GbifTerm.elevationAccuracy)),
  DEPTH_ACCURACY(new BaseEsField("event.depthAccuracy", GbifTerm.depthAccuracy)),
  ELEVATION(new BaseEsField("event.elevation", GbifTerm.elevation)),
  DEPTH(new BaseEsField("event.depth", GbifTerm.depth)),
  STATE_PROVINCE(new BaseEsField("event.stateProvince", DwcTerm.stateProvince, true)), //NOT INTERPRETED
  WATER_BODY(new BaseEsField("event.waterBody", DwcTerm.waterBody, true)),
  LOCALITY(new BaseEsField("event.locality", DwcTerm.locality, true)),
  COORDINATE_PRECISION(new BaseEsField("event.coordinatePrecision", DwcTerm.coordinatePrecision)),
  COORDINATE_UNCERTAINTY_IN_METERS(new BaseEsField("event.coordinateUncertaintyInMeters", DwcTerm.coordinateUncertaintyInMeters)),
  DISTANCE_FROM_CENTROID_IN_METERS(new BaseEsField("distanceFromCentroidInMeters", GbifTerm.distanceFromCentroidInMeters)),
  GADM_GID(new BaseEsField("event.gadm.gids", null)),
  GADM_LEVEL_0_GID(new BaseEsField("event.gadm.level0Gid", GadmTerm.level0Gid)),
  GADM_LEVEL_0_NAME(new BaseEsField("event.gadm.level0Name", GadmTerm.level0Name)),
  GADM_LEVEL_1_GID(new BaseEsField("event.gadm.level1Gid", GadmTerm.level1Gid)),
  GADM_LEVEL_1_NAME(new BaseEsField("event.gadm.level1Name", GadmTerm.level1Name)),
  GADM_LEVEL_2_GID(new BaseEsField("event.gadm.level2Gid", GadmTerm.level2Gid)),
  GADM_LEVEL_2_NAME(new BaseEsField("event.gadm.level2Name", GadmTerm.level2Name)),
  GADM_LEVEL_3_GID(new BaseEsField("event.gadm.level3Gid", GadmTerm.level3Gid)),
  GADM_LEVEL_3_NAME(new BaseEsField("event.gadm.level3Name", GadmTerm.level3Name)),

  //Location GBIF specific
  HAS_GEOSPATIAL_ISSUES(new BaseEsField("event.hasGeospatialIssue", GbifTerm.hasGeospatialIssues)),
  HAS_COORDINATE(new BaseEsField("event.hasCoordinate", GbifTerm.hasCoordinate)),
  REPATRIATED(new BaseEsField("event.repatriated", GbifTerm.repatriated)),

  //Taxonomic classification
  TAXON_KEY(new BaseEsField("derivedMetadata.taxonomicCoverage.taxonKey", GbifTerm.taxonKey)),
  USAGE_TAXON_KEY(new BaseEsField("derivedMetadata.taxonomicCoverage.usage.key", GbifTerm.taxonKey)),
  TAXON_RANK(new BaseEsField("derivedMetadata.taxonomicCoverage.usage.rank", DwcTerm.taxonRank)),
  ACCEPTED_TAXON_KEY(new BaseEsField("derivedMetadata.taxonomicCoverage.acceptedUsage.key", GbifTerm.acceptedTaxonKey)),
  ACCEPTED_SCIENTIFIC_NAME(new BaseEsField("derivedMetadata.taxonomicCoverage.acceptedUsage.name", GbifTerm.acceptedScientificName)),
  KINGDOM_KEY(new BaseEsField("derivedMetadata.taxonomicCoverage.kingdomKey", GbifTerm.kingdomKey)),
  KINGDOM(new BaseEsField("derivedMetadata.taxonomicCoverage.kingdom", DwcTerm.kingdom)),
  PHYLUM_KEY(new BaseEsField("derivedMetadata.taxonomicCoverage.phylumKey", GbifTerm.phylumKey)),
  PHYLUM(new BaseEsField("derivedMetadata.taxonomicCoverage.phylum", DwcTerm.phylum)),
  CLASS_KEY(new BaseEsField("derivedMetadata.taxonomicCoverage.classKey", GbifTerm.classKey)),
  CLASS(new BaseEsField("derivedMetadata.taxonomicCoverage.class", DwcTerm.class_)),
  ORDER_KEY(new BaseEsField("derivedMetadata.taxonomicCoverage.orderKey", GbifTerm.orderKey)),
  ORDER(new BaseEsField("derivedMetadata.taxonomicCoverage.order", DwcTerm.order)),
  FAMILY_KEY(new BaseEsField("derivedMetadata.taxonomicCoverage.familyKey", GbifTerm.familyKey)),
  FAMILY(new BaseEsField("derivedMetadata.taxonomicCoverage.family", DwcTerm.family)),
  GENUS_KEY(new BaseEsField("derivedMetadata.taxonomicCoverage.genusKey", GbifTerm.genusKey)),
  GENUS(new BaseEsField("derivedMetadata.taxonomicCoverage.genus", DwcTerm.genus)),
  SUBGENUS_KEY(new BaseEsField("derivedMetadata.taxonomicCoverage.subgenusKey", GbifTerm.subgenusKey)),
  SUBGENUS(new BaseEsField("derivedMetadata.taxonomicCoverage.subgenus", DwcTerm.subgenus)),
  SPECIES_KEY(new BaseEsField("derivedMetadata.taxonomicCoverage.speciesKey", GbifTerm.speciesKey)),
  SPECIES(new BaseEsField("derivedMetadata.taxonomicCoverage.species", GbifTerm.species)),
  SCIENTIFIC_NAME(new BaseEsField("derivedMetadata.taxonomicCoverage.usage.name", DwcTerm.scientificName)),
  SPECIFIC_EPITHET(new BaseEsField("derivedMetadata.taxonomicCoverage.usageParsedName.specificEpithet", DwcTerm.specificEpithet)),
  INFRA_SPECIFIC_EPITHET(new BaseEsField("derivedMetadata.taxonomicCoverage.usageParsedName.infraspecificEpithet", DwcTerm.infraspecificEpithet)),
  GENERIC_NAME(new BaseEsField("derivedMetadata.taxonomicCoverage.usageParsedName.genericName", DwcTerm.genericName)),
  TAXONOMIC_STATUS(new BaseEsField("derivedMetadata.taxonomicCoverage.diagnostics.status", DwcTerm.taxonomicStatus)),
  TAXON_ID(new BaseEsField("derivedMetadata.taxonomicCoverage.taxonID", DwcTerm.taxonID)),
  VERBATIM_SCIENTIFIC_NAME(new BaseEsField("derivedMetadata.taxonomicCoverage.verbatimScientificName", GbifTerm.verbatimScientificName)),
  IUCN_RED_LIST_CATEGORY(new BaseEsField("derivedMetadata.taxonomicCoverage.iucnRedListCategoryCode", IucnTerm.iucnRedListCategory)),

  // GrSciColl
  COLLECTION_KEY(new BaseEsField("event.collectionKey", GbifInternalTerm.collectionKey)),
  INSTITUTION_KEY(new BaseEsField("event.institutionKey", GbifInternalTerm.institutionKey)),

  //Sampling
  EVENT_ID(new BaseEsField("event.eventId", DwcTerm.eventID, true)),
  PARENT_EVENT_ID(new BaseEsField("event.parentEventId", DwcTerm.parentEventID, true)),
  SAMPLING_PROTOCOL(new BaseEsField("event.samplingProtocol", DwcTerm.samplingProtocol, true)),
  LIFE_STAGE(new BaseEsField("event.lifeStage.lineage", "lifeStage.concept", DwcTerm.lifeStage)),
  DATE_IDENTIFIED(new BaseEsField("event.dateIdentified", DwcTerm.dateIdentified)),
  MODIFIED(new BaseEsField("event.modified", DcTerm.modified)),
  REFERENCES(new BaseEsField("event.references", DcTerm.references)),
  SEX(new BaseEsField("event.sex", DwcTerm.sex)),
  IDENTIFIER(new BaseEsField("event.identifier", DcTerm.identifier)),
  INDIVIDUAL_COUNT(new BaseEsField("event.individualCount", DwcTerm.individualCount)),
  RELATION(new BaseEsField("event.relation", DcTerm.relation)),
  TYPIFIED_NAME(new BaseEsField("event.typifiedName", GbifTerm.typifiedName)),
  ORGANISM_QUANTITY(new BaseEsField("event.organismQuantity", DwcTerm.organismQuantity)),
  ORGANISM_QUANTITY_TYPE(new BaseEsField("event.organismQuantityType", DwcTerm.organismQuantityType)),
  SAMPLE_SIZE_UNIT(new BaseEsField("event.sampleSizeUnit", DwcTerm.sampleSizeUnit)),
  SAMPLE_SIZE_VALUE(new BaseEsField("event.sampleSizeValue", DwcTerm.sampleSizeValue)),
  RELATIVE_ORGANISM_QUANTITY(new BaseEsField("event.relativeOrganismQuantity", GbifTerm.relativeOrganismQuantity)),

  //Crawling
  CRAWL_ID(new BaseEsField("crawlId", GbifInternalTerm.crawlId)),
  LAST_INTERPRETED(new BaseEsField("created", GbifTerm.lastInterpreted)),
  LAST_CRAWLED(new BaseEsField("lastCrawled", GbifTerm.lastCrawled)),
  LAST_PARSED(new BaseEsField("created", GbifTerm.lastParsed)),

  //Media
  MEDIA_TYPE(new BaseEsField("event.mediaTypes", GbifTerm.mediaType)),
  MEDIA_ITEMS(new BaseEsField("event.multimediaItems", null)),

  //Issues
  ISSUE(new BaseEsField("event.issues", GbifTerm.issue)),

  ESTABLISHMENT_MEANS(new BaseEsField("event.establishmentMeans.lineage", "event.establishmentMeans.concept", DwcTerm.establishmentMeans)),
  DEGREE_OF_ESTABLISHMENT_MEANS(new BaseEsField("event.degreeOfEstablishment.lineage", "event.degreeOfEstablishment.concept", DwcTerm.degreeOfEstablishment)),
  PATHWAY(new BaseEsField("event.pathway.lineage", "event.pathway.concept", DwcTerm.pathway)),
  FACTS(new BaseEsField("event.measurementOrFactItems", null)),
  GBIF_ID(new BaseEsField("event.gbifId", GbifTerm.gbifID)),
  FULL_TEXT(new BaseEsField("all", null)),
  IS_IN_CLUSTER(new BaseEsField("event.isClustered", GbifInternalTerm.isInCluster)),
  EXTENSIONS(new BaseEsField("event.extensions", GbifInternalTerm.dwcaExtension)),

  //Event
  START_DAY_OF_YEAR(new BaseEsField("event.startDayOfYear", DwcTerm.startDayOfYear)),
  END_DAY_OF_YEAR(new BaseEsField("event.endDayOfYear", DwcTerm.startDayOfYear)),
  EVENT_TYPE(new BaseEsField("event.eventType", GbifTerm.eventType)),
  LOCATION_ID(new BaseEsField("event.locationID", DwcTerm.locationID)),
  PARENTS_LINEAGE(new BaseEsField("event.parentsLineage", UnknownTerm.build("event.parentsLineage"))),

  //Verbatim
  VERBATIM(new BaseEsField("verbatim", UnknownTerm.build("verbatim")));

  private final BaseEsField esField;

  public BaseEsField getEsField() {
    return esField;
  }

  EventEsField(BaseEsField esField) {
    this.esField = esField;
  }


  public static final ImmutableMap<OccurrenceSearchParameter, EsField> SEARCH_TO_ES_MAPPING =
    ImmutableMap.<OccurrenceSearchParameter, EsField>builder()
      .put(OccurrenceSearchParameter.DECIMAL_LATITUDE, LATITUDE)
      .put(OccurrenceSearchParameter.DECIMAL_LONGITUDE, LONGITUDE)
      .put(OccurrenceSearchParameter.YEAR, YEAR)
      .put(OccurrenceSearchParameter.MONTH, MONTH)
      .put(OccurrenceSearchParameter.CATALOG_NUMBER, CATALOG_NUMBER)
      .put(OccurrenceSearchParameter.RECORDED_BY, RECORDED_BY)
      .put(OccurrenceSearchParameter.IDENTIFIED_BY, IDENTIFIED_BY)
      .put(OccurrenceSearchParameter.RECORD_NUMBER, RECORD_NUMBER)
      .put(OccurrenceSearchParameter.COLLECTION_CODE, COLLECTION_CODE)
      .put(OccurrenceSearchParameter.INSTITUTION_CODE, INSTITUTION_CODE)
      .put(OccurrenceSearchParameter.DEPTH, DEPTH)
      .put(OccurrenceSearchParameter.ELEVATION, ELEVATION)
      .put(OccurrenceSearchParameter.BASIS_OF_RECORD, BASIS_OF_RECORD)
      .put(OccurrenceSearchParameter.DATASET_KEY, DATASET_KEY)
      .put(OccurrenceSearchParameter.HAS_GEOSPATIAL_ISSUE, HAS_GEOSPATIAL_ISSUES)
      .put(OccurrenceSearchParameter.HAS_COORDINATE, HAS_COORDINATE)
      .put(OccurrenceSearchParameter.EVENT_DATE, EVENT_DATE)
      .put(OccurrenceSearchParameter.MODIFIED, MODIFIED)
      .put(OccurrenceSearchParameter.LAST_INTERPRETED, LAST_INTERPRETED)
      .put(OccurrenceSearchParameter.COUNTRY, COUNTRY_CODE)
      .put(OccurrenceSearchParameter.PUBLISHING_COUNTRY, PUBLISHING_COUNTRY)
      .put(OccurrenceSearchParameter.CONTINENT, CONTINENT)
      .put(OccurrenceSearchParameter.TAXON_KEY, TAXON_KEY)
      .put(OccurrenceSearchParameter.KINGDOM_KEY, KINGDOM_KEY)
      .put(OccurrenceSearchParameter.PHYLUM_KEY, PHYLUM_KEY)
      .put(OccurrenceSearchParameter.CLASS_KEY, CLASS_KEY)
      .put(OccurrenceSearchParameter.ORDER_KEY, ORDER_KEY)
      .put(OccurrenceSearchParameter.FAMILY_KEY, FAMILY_KEY)
      .put(OccurrenceSearchParameter.GENUS_KEY, GENUS_KEY)
      .put(OccurrenceSearchParameter.SUBGENUS_KEY, SUBGENUS_KEY)
      .put(OccurrenceSearchParameter.SPECIES_KEY, SPECIES_KEY)
      .put(OccurrenceSearchParameter.SCIENTIFIC_NAME, SCIENTIFIC_NAME)
      .put(OccurrenceSearchParameter.VERBATIM_SCIENTIFIC_NAME, VERBATIM_SCIENTIFIC_NAME)
      .put(OccurrenceSearchParameter.TAXON_ID, TAXON_ID)
      .put(OccurrenceSearchParameter.TYPE_STATUS, TYPE_STATUS)
      .put(OccurrenceSearchParameter.TAXONOMIC_STATUS, TAXONOMIC_STATUS)
      .put(OccurrenceSearchParameter.MEDIA_TYPE, MEDIA_TYPE)
      .put(OccurrenceSearchParameter.ISSUE, ISSUE)
      .put(OccurrenceSearchParameter.OCCURRENCE_ID, OCCURRENCE_ID)
      .put(OccurrenceSearchParameter.ESTABLISHMENT_MEANS, ESTABLISHMENT_MEANS)
      .put(OccurrenceSearchParameter.DEGREE_OF_ESTABLISHMENT, DEGREE_OF_ESTABLISHMENT_MEANS)
      .put(OccurrenceSearchParameter.PATHWAY, PATHWAY)
      .put(OccurrenceSearchParameter.REPATRIATED, REPATRIATED)
      .put(OccurrenceSearchParameter.LOCALITY, LOCALITY)
      .put(OccurrenceSearchParameter.COORDINATE_UNCERTAINTY_IN_METERS, COORDINATE_UNCERTAINTY_IN_METERS)
      .put(OccurrenceSearchParameter.GADM_GID, GADM_GID)
      .put(OccurrenceSearchParameter.GADM_LEVEL_0_GID, GADM_LEVEL_0_GID)
      .put(OccurrenceSearchParameter.GADM_LEVEL_1_GID, GADM_LEVEL_1_GID)
      .put(OccurrenceSearchParameter.GADM_LEVEL_2_GID, GADM_LEVEL_2_GID)
      .put(OccurrenceSearchParameter.GADM_LEVEL_3_GID, GADM_LEVEL_3_GID)
      .put(OccurrenceSearchParameter.STATE_PROVINCE, STATE_PROVINCE)
      .put(OccurrenceSearchParameter.WATER_BODY, WATER_BODY)
      .put(OccurrenceSearchParameter.LICENSE, LICENSE)
      .put(OccurrenceSearchParameter.PROTOCOL, PROTOCOL)
      .put(OccurrenceSearchParameter.ORGANISM_ID, ORGANISM_ID)
      .put(OccurrenceSearchParameter.PUBLISHING_ORG, PUBLISHING_ORGANIZATION_KEY)
      .put(OccurrenceSearchParameter.HOSTING_ORGANIZATION_KEY, HOSTING_ORGANIZATION_KEY)
      .put(OccurrenceSearchParameter.CRAWL_ID, CRAWL_ID)
      .put(OccurrenceSearchParameter.INSTALLATION_KEY, INSTALLATION_KEY)
      .put(OccurrenceSearchParameter.NETWORK_KEY, NETWORK_KEY)
      .put(OccurrenceSearchParameter.EVENT_ID, EVENT_ID)
      .put(OccurrenceSearchParameter.PARENT_EVENT_ID, PARENT_EVENT_ID)
      .put(OccurrenceSearchParameter.SAMPLING_PROTOCOL, SAMPLING_PROTOCOL)
      .put(OccurrenceSearchParameter.PROJECT_ID, PROJECT_ID)
      .put(OccurrenceSearchParameter.PROGRAMME, PROGRAMME)
      .put(OccurrenceSearchParameter.ORGANISM_QUANTITY, ORGANISM_QUANTITY)
      .put(OccurrenceSearchParameter.ORGANISM_QUANTITY_TYPE, ORGANISM_QUANTITY_TYPE)
      .put(OccurrenceSearchParameter.SAMPLE_SIZE_VALUE, SAMPLE_SIZE_VALUE)
      .put(OccurrenceSearchParameter.SAMPLE_SIZE_UNIT, SAMPLE_SIZE_UNIT)
      .put(OccurrenceSearchParameter.RELATIVE_ORGANISM_QUANTITY, RELATIVE_ORGANISM_QUANTITY)
      .put(OccurrenceSearchParameter.COLLECTION_KEY, COLLECTION_KEY)
      .put(OccurrenceSearchParameter.INSTITUTION_KEY, INSTITUTION_KEY)
      .put(OccurrenceSearchParameter.IDENTIFIED_BY_ID, IDENTIFIED_BY_ID)
      .put(OccurrenceSearchParameter.RECORDED_BY_ID, RECORDED_BY_ID)
      .put(OccurrenceSearchParameter.OCCURRENCE_STATUS, OCCURRENCE_STATUS)
      .put(OccurrenceSearchParameter.LIFE_STAGE, LIFE_STAGE)
      .put(OccurrenceSearchParameter.IS_IN_CLUSTER, IS_IN_CLUSTER)
      .put(OccurrenceSearchParameter.DWCA_EXTENSION, EXTENSIONS)
      .put(OccurrenceSearchParameter.IUCN_RED_LIST_CATEGORY, IUCN_RED_LIST_CATEGORY)
      .put(OccurrenceSearchParameter.DATASET_ID, DATASET_ID)
      .put(OccurrenceSearchParameter.DATASET_NAME, DATASET_NAME)
      .put(OccurrenceSearchParameter.OTHER_CATALOG_NUMBERS, OTHER_CATALOG_NUMBERS)
      .put(OccurrenceSearchParameter.PREPARATIONS, PREPARATIONS)
      .put(OccurrenceSearchParameter.DISTANCE_FROM_CENTROID_IN_METERS, DISTANCE_FROM_CENTROID_IN_METERS)
      .build();

  private static final Set<EsField> DATE_FIELDS =
    ImmutableSet.of(EVENT_DATE, DATE_IDENTIFIED, MODIFIED, LAST_INTERPRETED, LAST_CRAWLED, LAST_PARSED);

  public static OccurrenceBaseEsFieldMapper buildFieldMapper() {
    return OccurrenceBaseEsFieldMapper.builder()
      .fullTextField(FULL_TEXT)
      .geoShapeField(COORDINATE_SHAPE)
      .geoDistanceField(COORDINATE_POINT)
      .uniqueIdField(ID)
      .defaultFilter(Optional.of(QueryBuilders.termQuery("type","event")))
      .defaultSort(ImmutableList.of(SortBuilders.fieldSort(YEAR.getSearchFieldName()).order(SortOrder.DESC),
                                    SortBuilders.fieldSort(MONTH.getSearchFieldName()).order(SortOrder.ASC),
                                    SortBuilders.fieldSort(ID.getSearchFieldName()).order(SortOrder.ASC)))
      .searchToEsMapping(SEARCH_TO_ES_MAPPING)
      .dateFields(DATE_FIELDS)
      .fieldEnumClass(EventEsField.class)
      .build();
  }

  @Override
  public String getSearchFieldName() {
    return esField.getSearchFieldName();
  }

  @Override
  public String getValueFieldName() {
    return esField.getValueFieldName();
  }

  @Override
  public Term getTerm() {
    return esField.getTerm();
  }

  @Override
  public boolean isAutoSuggest() {
    return esField.isAutoSuggest();
  }
}
