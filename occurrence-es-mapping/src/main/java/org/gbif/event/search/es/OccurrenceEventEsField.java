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
import org.gbif.occurrence.search.es.BaseEsField;
import org.gbif.occurrence.search.es.ChecklistEsField;
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
public enum OccurrenceEventEsField implements EsField {

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
  PROJECT_ID(new BaseEsField("metadata.projectId", GbifTerm.projectId)),
  PROGRAMME(new BaseEsField("metadata.programmeAcronym", GbifInternalTerm.programmeAcronym)),

  //Core identification
  INSTITUTION_CODE(new BaseEsField("occurrence.institutionCode", DwcTerm.institutionCode, true)),
  COLLECTION_CODE(new BaseEsField("occurrence.collectionCode", DwcTerm.collectionCode, true)),
  CATALOG_NUMBER(new BaseEsField("occurrence.catalogNumber", DwcTerm.catalogNumber, true)),

  ORGANISM_ID(new BaseEsField("occurrence.organismId", DwcTerm.organismID, true)),
  OCCURRENCE_ID(new BaseEsField("occurrence.occurrenceId", DwcTerm.occurrenceID, true)),
  RECORDED_BY(new BaseEsField("occurrence.recordedBy", DwcTerm.recordedBy, true)),
  IDENTIFIED_BY(new BaseEsField("occurrence.identifiedBy", DwcTerm.identifiedBy, true)),
  RECORDED_BY_ID(new BaseEsField("occurrence.recordedByIds.value", DwcTerm.recordedByID)),
  IDENTIFIED_BY_ID(new BaseEsField("occurrence.identifiedByIds.value", DwcTerm.identifiedByID)),
  RECORD_NUMBER(new BaseEsField("occurrence.recordNumber", DwcTerm.recordNumber, true)),
  BASIS_OF_RECORD(new BaseEsField("occurrence.basisOfRecord", DwcTerm.basisOfRecord)),
  TYPE_STATUS(new BaseEsField("occurrence.typeStatus", DwcTerm.typeStatus)),
  OCCURRENCE_STATUS(new BaseEsField("occurrence.occurrenceStatus", DwcTerm.occurrenceStatus)),
  IS_SEQUENCED(new BaseEsField("occurrence.isSequenced", GbifTerm.isSequenced)),
  ASSOCIATED_SEQUENCES(new BaseEsField("occurrence.associatedSequences", DwcTerm.associatedSequences)),
  DATASET_ID(new BaseEsField("occurrence.datasetID", DwcTerm.datasetID)),
  DATASET_NAME(new BaseEsField("occurrence.datasetName", DwcTerm.datasetName, true)),
  OTHER_CATALOG_NUMBERS(new BaseEsField("occurrence.otherCatalogNumbers", DwcTerm.otherCatalogNumbers, true)),
  PREPARATIONS(new BaseEsField("occurrence.preparations", DwcTerm.preparations, true)),

  //Temporal
  YEAR(new BaseEsField("occurrence.year", DwcTerm.year)),
  MONTH(new BaseEsField("occurrence.month", DwcTerm.month)),
  DAY(new BaseEsField("occurrence.day", DwcTerm.day)),
  EVENT_DATE(new BaseEsField("occurrence.eventDateInterval", DwcTerm.eventDate)),
  EVENT_DATE_GTE(new BaseEsField("occurrence.eventDateSingle", GbifInternalTerm.eventDateGte)),
  EVENT_DATE_INTERVAL(new BaseEsField("occurrence.eventDateInterval", EsField.EVENT_DATE_INTERVAL)),

  //Location
  COORDINATE_SHAPE(new BaseEsField("occurrence.scoordinates", null)),
  COORDINATE_POINT(new BaseEsField("occurrence.coordinates", null)),
  LATITUDE(new BaseEsField("occurrence.decimalLatitude", DwcTerm.decimalLatitude)),
  LONGITUDE(new BaseEsField("occurrence.decimalLongitude", DwcTerm.decimalLongitude)),
  COUNTRY_CODE(new BaseEsField("occurrence.countryCode", DwcTerm.countryCode)),
  GBIF_REGION(new BaseEsField("occurrence.gbifRegion", GbifTerm.gbifRegion)),
  CONTINENT(new BaseEsField("occurrence.continent", DwcTerm.continent)),
  COORDINATE_ACCURACY(new BaseEsField("occurrence.coordinateAccuracy", GbifTerm.coordinateAccuracy)),
  ELEVATION_ACCURACY(new BaseEsField("occurrence.elevationAccuracy", GbifTerm.elevationAccuracy)),
  DEPTH_ACCURACY(new BaseEsField("occurrence.depthAccuracy", GbifTerm.depthAccuracy)),
  ELEVATION(new BaseEsField("occurrence.elevation", GbifTerm.elevation)),
  DEPTH(new BaseEsField("occurrence.depth", GbifTerm.depth)),
  STATE_PROVINCE(new BaseEsField("occurrence.stateProvince", DwcTerm.stateProvince, true)), //NOT INTERPRETED
  WATER_BODY(new BaseEsField("occurrence.waterBody", DwcTerm.waterBody, true)),
  LOCALITY(new BaseEsField("occurrence.locality", DwcTerm.locality, true)),
  COORDINATE_PRECISION(new BaseEsField("occurrence.coordinatePrecision", DwcTerm.coordinatePrecision)),
  COORDINATE_UNCERTAINTY_IN_METERS(new BaseEsField("occurrence.coordinateUncertaintyInMeters", DwcTerm.coordinateUncertaintyInMeters)),
  DISTANCE_FROM_CENTROID_IN_METERS(new BaseEsField("occurrence.distanceFromCentroidInMeters", GbifTerm.distanceFromCentroidInMeters)),
  ISLAND(new BaseEsField("occurrence.island", DwcTerm.island)),
  ISLAND_GROUP(new BaseEsField("occurrence.islandGroup", DwcTerm.islandGroup)),
  HIGHER_GEOGRAPHY(new BaseEsField("occurrence.higherGeography", DwcTerm.higherGeography)),
  GEOREFERENCED_BY(new BaseEsField("occurrence.georeferencedBy", DwcTerm.georeferencedBy)),

  GADM_GID(new BaseEsField("occurrence.gadm.gids", null)),
  GADM_LEVEL_0_GID(new BaseEsField("occurrence.gadm.level0Gid", GadmTerm.level0Gid)),
  GADM_LEVEL_0_NAME(new BaseEsField("occurrence.gadm.level0Name", GadmTerm.level0Name)),
  GADM_LEVEL_1_GID(new BaseEsField("occurrence.gadm.level1Gid", GadmTerm.level1Gid)),
  GADM_LEVEL_1_NAME(new BaseEsField("occurrence.gadm.level1Name", GadmTerm.level1Name)),
  GADM_LEVEL_2_GID(new BaseEsField("occurrence.gadm.level2Gid", GadmTerm.level2Gid)),
  GADM_LEVEL_2_NAME(new BaseEsField("occurrence.gadm.level2Name", GadmTerm.level2Name)),
  GADM_LEVEL_3_GID(new BaseEsField("occurrence.gadm.level3Gid", GadmTerm.level3Gid)),
  GADM_LEVEL_3_NAME(new BaseEsField("occurrence.gadm.level3Name", GadmTerm.level3Name)),

  //Location GBIF specific
  HAS_GEOSPATIAL_ISSUES(new BaseEsField("occurrence.hasGeospatialIssue", GbifTerm.hasGeospatialIssues)),
  HAS_COORDINATE(new BaseEsField("occurrence.hasCoordinate", GbifTerm.hasCoordinate)),
  REPATRIATED(new BaseEsField("occurrence.repatriated", GbifTerm.repatriated)),

  //Taxonomic classification
  USAGE_TAXON_KEY(new ChecklistEsField("occurrence.classifications.%s.usage.key", GbifTerm.taxonKey)),
  TAXON_KEY(new ChecklistEsField("occurrence.classifications.%s.taxonKeys", GbifTerm.taxonKey)),
  TAXON_RANK(new ChecklistEsField("occurrence.classifications.%s.usage.rank", DwcTerm.taxonRank)),
  ACCEPTED_TAXON_KEY(new ChecklistEsField("occurrence.classifications.%s.acceptedUsage.key", GbifTerm.acceptedTaxonKey)),
  ACCEPTED_SCIENTIFIC_NAME(new ChecklistEsField("occurrence.classifications.%s.acceptedUsage.name", GbifTerm.acceptedScientificName)),
  KINGDOM_KEY(new ChecklistEsField("occurrence.classifications.%s.classificationKeys.KINGDOM", GbifTerm.kingdomKey)),
  KINGDOM(new ChecklistEsField("occurrence.classifications.%s.classification.KINGDOM", DwcTerm.kingdom)),
  PHYLUM_KEY(new ChecklistEsField("occurrence.classifications.%s.classificationKeys.PHYLUM", GbifTerm.phylumKey)),
  PHYLUM(new ChecklistEsField("occurrence.classifications.%s.classification.PHYLUM", DwcTerm.phylum)),
  CLASS_KEY(new ChecklistEsField("occurrence.classifications.%s.classificationKeys.CLASS", GbifTerm.classKey)),
  CLASS(new ChecklistEsField("occurrence.classifications.%s.classification.CLASS", DwcTerm.class_)),
  ORDER_KEY(new ChecklistEsField("occurrence.classifications.%s.classificationKeys.ORDER", GbifTerm.orderKey)),
  ORDER(new ChecklistEsField("occurrence.classifications.%s.classification.ORDER", DwcTerm.order)),
  FAMILY_KEY(new ChecklistEsField("occurrence.classifications.%s.classificationKeys.FAMILY", GbifTerm.familyKey)),
  FAMILY(new ChecklistEsField("occurrence.classifications.%s.classification.FAMILY", DwcTerm.family)),
  GENUS_KEY(new ChecklistEsField("occurrence.classifications.%s.classificationKeys.GENUS", GbifTerm.genusKey)),
  GENUS(new ChecklistEsField("occurrence.classifications.%s.classification.GENUS", DwcTerm.genus)),
  SUBGENUS_KEY(new ChecklistEsField("occurrence.classifications.%s.classificationKeys.SUBGENUS", GbifTerm.subgenusKey)),
  SUBGENUS(new ChecklistEsField("occurrence.classifications.%s.classification.SUBGENUS", DwcTerm.subgenus)),
  SPECIES_KEY(new ChecklistEsField("occurrence.classifications.%s.classificationKeys.SPECIES", GbifTerm.speciesKey)),
  SPECIES(new ChecklistEsField("occurrence.classifications.%s.classification.SPECIES", GbifTerm.species)),
  SCIENTIFIC_NAME(new ChecklistEsField("occurrence.classifications.%s.usage.name", DwcTerm.scientificName)),
  SCIENTIFIC_NAME_AUTHORSHIP(new ChecklistEsField("occurrence.classifications.%s.usage.authorship", DwcTerm.scientificNameAuthorship)),
  SPECIFIC_EPITHET(new ChecklistEsField("occurrence.classifications.%s.usage.specificEpithet", DwcTerm.specificEpithet)),
  INFRA_SPECIFIC_EPITHET(new ChecklistEsField("occurrence.classifications.%s.usage.infraspecificEpithet", DwcTerm.infraspecificEpithet)),
  GENERIC_NAME(new ChecklistEsField("occurrence.classifications.%s.usage.genericName", DwcTerm.genericName)),
  TAXONOMIC_STATUS(new ChecklistEsField("occurrence.classifications.%s.status", DwcTerm.taxonomicStatus)),
  IUCN_RED_LIST_CATEGORY(new ChecklistEsField("occurrence.classifications.%s.iucnRedListCategoryCode", IucnTerm.iucnRedListCategory)),
  TAXONOMIC_ISSUE(new ChecklistEsField("occurrence.classifications.%s.issues", GbifTerm.taxonomicIssue)),

  TAXON_ID(new ChecklistEsField("occurrence.taxonID", DwcTerm.taxonID)),
  VERBATIM_SCIENTIFIC_NAME(new BaseEsField("occurrence.verbatimScientificName", GbifTerm.verbatimScientificName)),

  // GrSciColl
  COLLECTION_KEY(new BaseEsField("occurrence.collectionKey", GbifInternalTerm.collectionKey)),
  INSTITUTION_KEY(new BaseEsField("occurrence.institutionKey", GbifInternalTerm.institutionKey)),

  //Sampling
  EVENT_ID(new BaseEsField("occurrence.eventId", DwcTerm.eventID, true)),
  PARENT_EVENT_ID(new BaseEsField("occurrence.parentEventId", DwcTerm.parentEventID, true)),
  SAMPLING_PROTOCOL(new BaseEsField("occurrence.samplingProtocol", DwcTerm.samplingProtocol, true)),
  LIFE_STAGE(new BaseEsField("occurrence.lifeStage.lineage", "lifeStage.concept", DwcTerm.lifeStage)),
  DATE_IDENTIFIED(new BaseEsField("occurrence.dateIdentified", DwcTerm.dateIdentified)),
  FIELD_NUMBER(new BaseEsField("occurrence.fieldNumber", DwcTerm.fieldNumber)),
  MODIFIED(new BaseEsField("occurrence.modified", DcTerm.modified)),
  REFERENCES(new BaseEsField("occurrence.references", DcTerm.references)),
  SEX(new BaseEsField("occurrence.sex", DwcTerm.sex)),
  IDENTIFIER(new BaseEsField("occurrence.identifier", DcTerm.identifier)),
  INDIVIDUAL_COUNT(new BaseEsField("occurrence.individualCount", DwcTerm.individualCount)),
  RELATION(new BaseEsField("occurrence.relation", DcTerm.relation)),
  TYPIFIED_NAME(new BaseEsField("occurrence.typifiedName", GbifTerm.typifiedName)),
  ORGANISM_QUANTITY(new BaseEsField("occurrence.organismQuantity", DwcTerm.organismQuantity)),
  ORGANISM_QUANTITY_TYPE(new BaseEsField("occurrence.organismQuantityType", DwcTerm.organismQuantityType)),
  SAMPLE_SIZE_UNIT(new BaseEsField("occurrence.sampleSizeUnit", DwcTerm.sampleSizeUnit)),
  SAMPLE_SIZE_VALUE(new BaseEsField("occurrence.sampleSizeValue", DwcTerm.sampleSizeValue)),
  RELATIVE_ORGANISM_QUANTITY(new BaseEsField("occurrence.relativeOrganismQuantity", GbifTerm.relativeOrganismQuantity)),

  EARLIEST_EON_OR_LOWEST_EONOTHEM(
    new BaseEsField(
      "occurrence.geologicalContext.earliestEonOrLowestEonothem.lineage",
      "occurrence.geologicalContext.earliestEonOrLowestEonothem.concept",
      DwcTerm.earliestEonOrLowestEonothem)),
  LATEST_EON_OR_HIGHEST_EONOTHEM(
    new BaseEsField(
      "occurrence.geologicalContext.latestEonOrHighestEonothem.lineage",
      "occurrence.geologicalContext.latestEonOrHighestEonothem.concept",
      DwcTerm.latestEonOrHighestEonothem)),
  EARLIEST_ERA_OR_LOWEST_ERATHEM(
    new BaseEsField(
      "occurrence.geologicalContext.earliestEraOrLowestErathem.lineage",
      "occurrence.geologicalContext.earliestEraOrLowestErathem.concept",
      DwcTerm.earliestEraOrLowestErathem)),
  LATEST_ERA_OR_HIGHEST_ERATHEM(
    new BaseEsField(
      "occurrence.geologicalContext.latestEraOrHighestErathem.lineage",
      "occurrence.geologicalContext.latestEraOrHighestErathem.concept",
      DwcTerm.latestEraOrHighestErathem)),
  EARLIEST_PERIOD_OR_LOWEST_SYSTEM(
    new BaseEsField(
      "occurrence.geologicalContext.earliestPeriodOrLowestSystem.lineage",
      "occurrence.geologicalContext.earliestPeriodOrLowestSystem.concept",
      DwcTerm.earliestPeriodOrLowestSystem)),
  LATEST_PERIOD_OR_HIGHEST_SYSTEM(
    new BaseEsField(
      "occurrence.geologicalContext.latestPeriodOrHighestSystem.lineage",
      "occurrence.geologicalContext.latestPeriodOrHighestSystem.concept",
      DwcTerm.latestPeriodOrHighestSystem)),
  EARLIEST_EPOCH_OR_LOWEST_SERIES(
    new BaseEsField(
      "occurrence.geologicalContext.earliestEpochOrLowestSeries.lineage",
      "occurrence.geologicalContext.earliestEpochOrLowestSeries.concept",
      DwcTerm.earliestEpochOrLowestSeries)),
  LATEST_EPOCH_OR_HIGHEST_SERIES(
    new BaseEsField(
      "occurrence.geologicalContext.latestEpochOrHighestSeries.lineage",
      "occurrence.geologicalContext.latestEpochOrHighestSeries.concept",
      DwcTerm.latestEpochOrHighestSeries)),
  EARLIEST_AGE_OR_LOWEST_STAGE(
    new BaseEsField(
      "occurrence.geologicalContext.earliestAgeOrLowestStage.lineage",
      "occurrence.geologicalContext.earliestAgeOrLowestStage.concept",
      DwcTerm.earliestAgeOrLowestStage)),
  LATEST_AGE_OR_HIGHEST_STAGE(
    new BaseEsField(
      "occurrence.geologicalContext.latestAgeOrHighestStage.lineage",
      "occurrence.geologicalContext.latestAgeOrHighestStage.concept",
      DwcTerm.latestAgeOrHighestStage)),
  LOWEST_BIOSTRATIGRAPHIC_ZONE(
    new BaseEsField(
      "occurrence.geologicalContext.lowestBiostratigraphicZone", DwcTerm.lowestBiostratigraphicZone)),
  HIGHEST_BIOSTRATIGRAPHIC_ZONE(
    new BaseEsField(
      "occurrence.geologicalContext.highestBiostratigraphicZone", DwcTerm.highestBiostratigraphicZone)),
  GROUP(new BaseEsField("occurrence.geologicalContext.group", DwcTerm.group)),
  FORMATION(new BaseEsField("occurrence.geologicalContext.formation", DwcTerm.formation)),
  MEMBER(new BaseEsField("occurrence.geologicalContext.member", DwcTerm.member)),
  BED(new BaseEsField("occurrence.geologicalContext.bed", DwcTerm.bed)),
  GEOLOGICAL_TIME(new BaseEsField("occurrence.geologicalContext.range", null)),
  LITHOSTRATIGRAPHY(new BaseEsField("occurrence.geologicalContext.lithostratigraphy", null, true)),
  BIOSTRATIGRAPHY(new BaseEsField("occurrence.geologicalContext.biostratigraphy", null, true)),



  //Crawling
  CRAWL_ID(new BaseEsField("crawlId", GbifInternalTerm.crawlId)),
  LAST_INTERPRETED(new BaseEsField("created", GbifTerm.lastInterpreted)),
  LAST_CRAWLED(new BaseEsField("lastCrawled", GbifTerm.lastCrawled)),
  LAST_PARSED(new BaseEsField("created", GbifTerm.lastParsed)),

  //Media
  MEDIA_TYPE(new BaseEsField("occurrence.mediaTypes", GbifTerm.mediaType)),
  MEDIA_ITEMS(new BaseEsField("occurrence.multimediaItems", EsField.MEDIA_ITEMS)),

  // DNA
  DNA_SEQUENCE_ID(new BaseEsField("occurrence.dnaSequenceID", GbifTerm.dnaSequenceID)),

  //Issues
  ISSUE(new BaseEsField("occurrence.issues", GbifTerm.issue)),

  ESTABLISHMENT_MEANS(new BaseEsField("occurrence.establishmentMeans.lineage", "occurrence.establishmentMeans.concept", DwcTerm.establishmentMeans)),
  DEGREE_OF_ESTABLISHMENT_MEANS(new BaseEsField("occurrence.degreeOfEstablishment.lineage", "occurrence.degreeOfEstablishment.concept", DwcTerm.degreeOfEstablishment)),
  PATHWAY(new BaseEsField("occurrence.pathway.lineage", "occurrence.pathway.concept", DwcTerm.pathway)),
  FACTS(new BaseEsField("occurrence.measurementOrFactItems", EsField.FACTS)),
  GBIF_ID(new BaseEsField("occurrence.gbifId", GbifTerm.gbifID)),
  FULL_TEXT(new BaseEsField("all", EsField.ALL)),
  IS_IN_CLUSTER(new BaseEsField("occurrence.isClustered", GbifInternalTerm.isInCluster)),
  EXTENSIONS(new BaseEsField("occurrence.extensions", GbifInternalTerm.dwcaExtension)),

  //Event
  START_DAY_OF_YEAR(new BaseEsField("occurrence.startDayOfYear", DwcTerm.startDayOfYear)),
  END_DAY_OF_YEAR(new BaseEsField("occurrence.endDayOfYear", DwcTerm.endDayOfYear)),
  EVENT_TYPE(new BaseEsField("occurrence.eventType", DwcTerm.eventType)),
  LOCATION_ID(new BaseEsField("occurrence.locationID", DwcTerm.locationID)),
  PARENTS_LINEAGE(new BaseEsField("occurrence.parentsLineage", EsField.PARENTS_LINEAGE)),

  //Verbatim
  VERBATIM(new BaseEsField("verbatim", EsField.VERBATIM));

  private final BaseEsField esField;

  public BaseEsField getEsField() {
    return esField;
  }

  OccurrenceEventEsField(BaseEsField esField) {
    this.esField = esField;
  }

  public static final ImmutableMap<OccurrenceSearchParameter, EsField> SEARCH_TO_ES_MAPPING =
    ImmutableMap.<OccurrenceSearchParameter, EsField>builder()
      .put(OccurrenceSearchParameter.DECIMAL_LATITUDE, LATITUDE)
      .put(OccurrenceSearchParameter.DECIMAL_LONGITUDE, LONGITUDE)
      .put(OccurrenceSearchParameter.YEAR, YEAR)
      .put(OccurrenceSearchParameter.MONTH, MONTH)
      .put(OccurrenceSearchParameter.DAY, DAY)
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
      .put(OccurrenceSearchParameter.GBIF_ID, GBIF_ID)
      .build();

  private static final Set<EsField> DATE_FIELDS = ImmutableSet.of(EVENT_DATE, DATE_IDENTIFIED, MODIFIED, LAST_INTERPRETED, LAST_CRAWLED,LAST_PARSED);

  public static OccurrenceBaseEsFieldMapper buildFieldMapper() {
      return OccurrenceBaseEsFieldMapper.builder()
        .fullTextField(FULL_TEXT)
        .geoShapeField(COORDINATE_SHAPE)
        .geoDistanceField(COORDINATE_POINT)
        .uniqueIdField(ID)
        .defaultFilter(Optional.of(QueryBuilders.termQuery("type", "occurrence")))
        .defaultSort(ImmutableList.of(SortBuilders.fieldSort("yearMonthGbifIdSort").order(SortOrder.ASC)))
        .searchToEsMapping(SEARCH_TO_ES_MAPPING)
        .dateFields(DATE_FIELDS)
        .fieldEnumClass(OccurrenceEventEsField.class)
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
  public String getNestedPath() {
    return esField.getNestedPath();
  }

  @Override
  public Term getTerm() {
    return esField.getTerm();
  }

  @Override
  public boolean isAutoSuggest() {
    return esField.isAutoSuggest();
  }

  @Override
  public boolean isUsingText() {
    return esField.isUsingText();
  }

  @Override
  public boolean isNestedField() {
    return esField.isNestedField();
  }
}
