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
package org.gbif.occurrence.search.es;

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GadmTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.IucnTerm;
import org.gbif.dwc.terms.Term;

import java.util.Optional;
import java.util.Set;

import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/** Enum that contains the mapping of symbolic names and field names of valid Elasticsearch fields. */
public enum OccurrenceEsField implements EsField {

  ID(new BaseEsField("id", DcTerm.identifier)),

  //Dataset derived
  DATASET_KEY(new BaseEsField("datasetKey", GbifTerm.datasetKey)),
  PUBLISHING_COUNTRY(new BaseEsField("publishingCountry", GbifTerm.publishingCountry)),
  PUBLISHED_BY_GBIF_REGION(new BaseEsField("publishedByGbifRegion", GbifTerm.publishedByGbifRegion)),
  PUBLISHING_ORGANIZATION_KEY(new BaseEsField("publishingOrganizationKey", GbifInternalTerm.publishingOrgKey)),
  HOSTING_ORGANIZATION_KEY(new BaseEsField("hostingOrganizationKey", GbifInternalTerm.hostingOrganizationKey)),
  INSTALLATION_KEY(new BaseEsField("installationKey", GbifInternalTerm.installationKey)),
  NETWORK_KEY(new BaseEsField("networkKeys", GbifInternalTerm.networkKey)),
  PROTOCOL(new BaseEsField("protocol", GbifTerm.protocol)),
  LICENSE(new BaseEsField("license", DcTerm.license)),
  PROJECT_ID(new BaseEsField("projectId", GbifTerm.projectId)),
  PROGRAMME(new BaseEsField("programmeAcronym", GbifInternalTerm.programmeAcronym)),

  //Core identification
  INSTITUTION_CODE(new BaseEsField("institutionCode", DwcTerm.institutionCode, true)),
  COLLECTION_CODE(new BaseEsField("collectionCode", DwcTerm.collectionCode, true)),
  CATALOG_NUMBER(new BaseEsField("catalogNumber", DwcTerm.catalogNumber, true)),

  ORGANISM_ID(new BaseEsField("organismId", DwcTerm.organismID, true)),
  OCCURRENCE_ID(new BaseEsField("occurrenceId", DwcTerm.occurrenceID, true)),
  RECORDED_BY(new BaseEsField("recordedBy", DwcTerm.recordedBy, true)),
  IDENTIFIED_BY(new BaseEsField("identifiedBy", DwcTerm.identifiedBy, true)),
  RECORDED_BY_ID(new BaseEsField("recordedByIds.value", DwcTerm.recordedByID)),
  IDENTIFIED_BY_ID(new BaseEsField("identifiedByIds.value", DwcTerm.identifiedByID)),
  RECORD_NUMBER(new BaseEsField("recordNumber", DwcTerm.recordNumber, true)),
  BASIS_OF_RECORD(new BaseEsField("basisOfRecord", DwcTerm.basisOfRecord)),
  TYPE_STATUS(new BaseEsField("typeStatus", DwcTerm.typeStatus)),
  OCCURRENCE_STATUS(new BaseEsField("occurrenceStatus", DwcTerm.occurrenceStatus)),
  IS_SEQUENCED(new BaseEsField("isSequenced", GbifTerm.isSequenced)),
  ASSOCIATED_SEQUENCES(new BaseEsField("associatedSequences", DwcTerm.associatedSequences)),
  DATASET_ID(new BaseEsField("datasetID", DwcTerm.datasetID)),
  DATASET_NAME(new BaseEsField("datasetName", DwcTerm.datasetName, true)),
  OTHER_CATALOG_NUMBERS(new BaseEsField("otherCatalogNumbers", DwcTerm.otherCatalogNumbers, true)),
  PREPARATIONS(new BaseEsField("preparations", DwcTerm.preparations, true)),

  //Temporal
  YEAR(new BaseEsField("year", DwcTerm.year)),
  MONTH(new BaseEsField("month", DwcTerm.month)),
  DAY(new BaseEsField("day", DwcTerm.day)),
  EVENT_DATE(new BaseEsField("eventDate", DwcTerm.eventDate)),
  EVENT_DATE_INTERVAL(new BaseEsField("eventDateInterval", EsField.EVENT_DATE_INTERVAL)),
  START_DAY_OF_YEAR(new BaseEsField("startDayOfYear", DwcTerm.startDayOfYear)),
  END_DAY_OF_YEAR(new BaseEsField("endDayOfYear", DwcTerm.endDayOfYear)),

  //Location
  COORDINATE_SHAPE(new BaseEsField("scoordinates", null)),
  COORDINATE_POINT(new BaseEsField("coordinates", null)),
  LATITUDE(new BaseEsField("decimalLatitude", DwcTerm.decimalLatitude)),
  LONGITUDE(new BaseEsField("decimalLongitude", DwcTerm.decimalLongitude)),
  COUNTRY_CODE(new BaseEsField("countryCode", DwcTerm.countryCode)),
  GBIF_REGION(new BaseEsField("gbifRegion", GbifTerm.gbifRegion)),
  CONTINENT(new BaseEsField("continent", DwcTerm.continent)),
  COORDINATE_ACCURACY(new BaseEsField("coordinateAccuracy", GbifTerm.coordinateAccuracy)),
  ELEVATION_ACCURACY(new BaseEsField("elevationAccuracy", GbifTerm.elevationAccuracy)),
  DEPTH_ACCURACY(new BaseEsField("depthAccuracy", GbifTerm.depthAccuracy)),
  ELEVATION(new BaseEsField("elevation", GbifTerm.elevation)),
  DEPTH(new BaseEsField("depth", GbifTerm.depth)),
  STATE_PROVINCE(new BaseEsField("stateProvince", DwcTerm.stateProvince, true)), //NOT INTERPRETED
  WATER_BODY(new BaseEsField("waterBody", DwcTerm.waterBody, true)),
  LOCALITY(new BaseEsField("locality", DwcTerm.locality, true)),
  COORDINATE_PRECISION(new BaseEsField("coordinatePrecision", DwcTerm.coordinatePrecision)),
  COORDINATE_UNCERTAINTY_IN_METERS(new BaseEsField("coordinateUncertaintyInMeters", DwcTerm.coordinateUncertaintyInMeters)),
  DISTANCE_FROM_CENTROID_IN_METERS(new BaseEsField("distanceFromCentroidInMeters", GbifTerm.distanceFromCentroidInMeters)),
  ISLAND(new BaseEsField("island", DwcTerm.island)),
  ISLAND_GROUP(new BaseEsField("islandGroup", DwcTerm.islandGroup)),
  HIGHER_GEOGRAPHY(new BaseEsField("higherGeography", DwcTerm.higherGeography)),
  GEOREFERENCED_BY(new BaseEsField("georeferencedBy", DwcTerm.georeferencedBy)),

  GADM_GID(new BaseEsField("gadm.gids", null)),
  GADM_LEVEL_0_GID(new BaseEsField("gadm.level0Gid", GadmTerm.level0Gid)),
  GADM_LEVEL_0_NAME(new BaseEsField("gadm.level0Name", GadmTerm.level0Name)),
  GADM_LEVEL_1_GID(new BaseEsField("gadm.level1Gid", GadmTerm.level1Gid)),
  GADM_LEVEL_1_NAME(new BaseEsField("gadm.level1Name", GadmTerm.level1Name)),
  GADM_LEVEL_2_GID(new BaseEsField("gadm.level2Gid", GadmTerm.level2Gid)),
  GADM_LEVEL_2_NAME(new BaseEsField("gadm.level2Name", GadmTerm.level2Name)),
  GADM_LEVEL_3_GID(new BaseEsField("gadm.level3Gid", GadmTerm.level3Gid)),
  GADM_LEVEL_3_NAME(new BaseEsField("gadm.level3Name", GadmTerm.level3Name)),

  //Location GBIF specific
  HAS_GEOSPATIAL_ISSUES(new BaseEsField("hasGeospatialIssue", GbifTerm.hasGeospatialIssues)),
  HAS_COORDINATE(new BaseEsField("hasCoordinate", GbifTerm.hasCoordinate)),
  REPATRIATED(new BaseEsField("repatriated", GbifTerm.repatriated)),

  //Taxonomic classification
  USAGE_TAXON_KEY(new BaseEsField("gbifClassification.usage.key", GbifTerm.taxonKey)),
  TAXON_KEY(new BaseEsField("gbifClassification.taxonKey", GbifTerm.taxonKey)),
  TAXON_RANK(new BaseEsField("gbifClassification.usage.rank", DwcTerm.taxonRank)),
  ACCEPTED_TAXON_KEY(new BaseEsField("gbifClassification.acceptedUsage.key", GbifTerm.acceptedTaxonKey)),
  ACCEPTED_SCIENTIFIC_NAME(new BaseEsField("gbifClassification.acceptedUsage.name", GbifTerm.acceptedScientificName)),
  KINGDOM_KEY(new BaseEsField("gbifClassification.kingdomKey", GbifTerm.kingdomKey)),
  KINGDOM(new BaseEsField("gbifClassification.kingdom", DwcTerm.kingdom)),
  PHYLUM_KEY(new BaseEsField("gbifClassification.phylumKey", GbifTerm.phylumKey)),
  PHYLUM(new BaseEsField("gbifClassification.phylum", DwcTerm.phylum)),
  CLASS_KEY(new BaseEsField("gbifClassification.classKey", GbifTerm.classKey)),
  CLASS(new BaseEsField("gbifClassification.class", DwcTerm.class_)),
  ORDER_KEY(new BaseEsField("gbifClassification.orderKey", GbifTerm.orderKey)),
  ORDER(new BaseEsField("gbifClassification.order", DwcTerm.order)),
  FAMILY_KEY(new BaseEsField("gbifClassification.familyKey", GbifTerm.familyKey)),
  FAMILY(new BaseEsField("gbifClassification.family", DwcTerm.family)),
  GENUS_KEY(new BaseEsField("gbifClassification.genusKey", GbifTerm.genusKey)),
  GENUS(new BaseEsField("gbifClassification.genus", DwcTerm.genus)),
  SUBGENUS_KEY(new BaseEsField("gbifClassification.subgenusKey", GbifTerm.subgenusKey)),
  SUBGENUS(new BaseEsField("gbifClassification.subgenus", DwcTerm.subgenus)),
  SPECIES_KEY(new BaseEsField("gbifClassification.speciesKey", GbifTerm.speciesKey)),
  SPECIES(new BaseEsField("gbifClassification.species", GbifTerm.species)),
  SCIENTIFIC_NAME(new BaseEsField("gbifClassification.usage.name", DwcTerm.scientificName)),
  SPECIFIC_EPITHET(new BaseEsField("gbifClassification.usageParsedName.specificEpithet", DwcTerm.specificEpithet)),
  INFRA_SPECIFIC_EPITHET(new BaseEsField("gbifClassification.usageParsedName.infraspecificEpithet", DwcTerm.infraspecificEpithet)),
  GENERIC_NAME(new BaseEsField("gbifClassification.usageParsedName.genericName", DwcTerm.genericName)),
  TAXONOMIC_STATUS(new BaseEsField("gbifClassification.diagnostics.status", DwcTerm.taxonomicStatus)),
  TAXON_ID(new BaseEsField("gbifClassification.taxonID", DwcTerm.taxonID)),
  TAXON_CONCEPT_ID(new BaseEsField("gbifClassification.taxonConceptID", DwcTerm.taxonConceptID)),
  VERBATIM_SCIENTIFIC_NAME(new BaseEsField("gbifClassification.verbatimScientificName", GbifTerm.verbatimScientificName)),
  IUCN_RED_LIST_CATEGORY(new BaseEsField("gbifClassification.iucnRedListCategoryCode", IucnTerm.iucnRedListCategory)),

  // GrSciColl
  COLLECTION_KEY(new BaseEsField("collectionKey", GbifInternalTerm.collectionKey)),
  INSTITUTION_KEY(new BaseEsField("institutionKey", GbifInternalTerm.institutionKey)),

  //Sampling
  EVENT_ID(new BaseEsField("eventId", DwcTerm.eventID, true)),
  PARENT_EVENT_ID(new BaseEsField("parentEventId", DwcTerm.parentEventID, true)),
  SAMPLING_PROTOCOL(new BaseEsField("samplingProtocol", DwcTerm.samplingProtocol, true)),
  PREVIOUS_IDENTIFICATIONS(new BaseEsField("previousIdentifications", DwcTerm.previousIdentifications)),
  LIFE_STAGE(new BaseEsField("lifeStage.lineage", "lifeStage.concept", DwcTerm.lifeStage)),
  DATE_IDENTIFIED(new BaseEsField("dateIdentified", DwcTerm.dateIdentified)),
  FIELD_NUMBER(new BaseEsField("fieldNumber", DwcTerm.fieldNumber)),

  EARLIEST_EON_OR_LOWEST_EONOTHEM(
      new BaseEsField(
          "geologicalContext.earliestEonOrLowestEonothem.lineage",
          "geologicalContext.earliestEonOrLowestEonothem.concept",
          DwcTerm.earliestEonOrLowestEonothem)),
  LATEST_EON_OR_HIGHEST_EONOTHEM(
      new BaseEsField(
          "geologicalContext.latestEonOrHighestEonothem.lineage",
          "geologicalContext.latestEonOrHighestEonothem.concept",
          DwcTerm.latestEonOrHighestEonothem)),
  EARLIEST_ERA_OR_LOWEST_ERATHEM(
      new BaseEsField(
          "geologicalContext.earliestEraOrLowestErathem.lineage",
          "geologicalContext.earliestEraOrLowestErathem.concept",
          DwcTerm.earliestEraOrLowestErathem)),
  LATEST_ERA_OR_HIGHEST_ERATHEM(
      new BaseEsField(
          "geologicalContext.latestEraOrHighestErathem.lineage",
          "geologicalContext.latestEraOrHighestErathem.concept",
          DwcTerm.latestEraOrHighestErathem)),
  EARLIEST_PERIOD_OR_LOWEST_SYSTEM(
      new BaseEsField(
          "geologicalContext.earliestPeriodOrLowestSystem.lineage",
          "geologicalContext.earliestPeriodOrLowestSystem.concept",
          DwcTerm.earliestPeriodOrLowestSystem)),
  LATEST_PERIOD_OR_HIGHEST_SYSTEM(
      new BaseEsField(
          "geologicalContext.latestPeriodOrHighestSystem.lineage",
          "geologicalContext.latestPeriodOrHighestSystem.concept",
          DwcTerm.latestPeriodOrHighestSystem)),
  EARLIEST_EPOCH_OR_LOWEST_SERIES(
      new BaseEsField(
          "geologicalContext.earliestEpochOrLowestSeries.lineage",
          "geologicalContext.earliestEpochOrLowestSeries.concept",
          DwcTerm.earliestEpochOrLowestSeries)),
  LATEST_EPOCH_OR_HIGHEST_SERIES(
      new BaseEsField(
          "geologicalContext.latestEpochOrHighestSeries.lineage",
          "geologicalContext.latestEpochOrHighestSeries.concept",
          DwcTerm.latestEpochOrHighestSeries)),
  EARLIEST_AGE_OR_LOWEST_STAGE(
      new BaseEsField(
          "geologicalContext.earliestAgeOrLowestStage.lineage",
          "geologicalContext.earliestAgeOrLowestStage.concept",
          DwcTerm.earliestAgeOrLowestStage)),
  LATEST_AGE_OR_HIGHEST_STAGE(
      new BaseEsField(
          "geologicalContext.latestAgeOrHighestStage.lineage",
          "geologicalContext.latestAgeOrHighestStage.concept",
          DwcTerm.latestAgeOrHighestStage)),
  LOWEST_BIOSTRATIGRAPHIC_ZONE(
      new BaseEsField(
          "geologicalContext.lowestBiostratigraphicZone", DwcTerm.lowestBiostratigraphicZone)),
  HIGHEST_BIOSTRATIGRAPHIC_ZONE(
      new BaseEsField(
          "geologicalContext.highestBiostratigraphicZone", DwcTerm.highestBiostratigraphicZone)),
  GROUP(new BaseEsField("geologicalContext.group", DwcTerm.group)),
  FORMATION(new BaseEsField("geologicalContext.formation", DwcTerm.formation)),
  MEMBER(new BaseEsField("geologicalContext.member", DwcTerm.member)),
  BED(new BaseEsField("geologicalContext.bed", DwcTerm.bed)),

  MODIFIED(new BaseEsField("modified", DcTerm.modified)),
  REFERENCES(new BaseEsField("references", DcTerm.references)),
  SEX(new BaseEsField("sex", DwcTerm.sex)),
  IDENTIFIER(new BaseEsField("identifier", DcTerm.identifier)),
  INDIVIDUAL_COUNT(new BaseEsField("individualCount", DwcTerm.individualCount)),
  RELATION(new BaseEsField("relation", DcTerm.relation)),
  TYPIFIED_NAME(new BaseEsField("typifiedName", GbifTerm.typifiedName)),
  ORGANISM_QUANTITY(new BaseEsField("organismQuantity", DwcTerm.organismQuantity)),
  ORGANISM_QUANTITY_TYPE(new BaseEsField("organismQuantityType", DwcTerm.organismQuantityType)),
  SAMPLE_SIZE_UNIT(new BaseEsField("sampleSizeUnit", DwcTerm.sampleSizeUnit)),
  SAMPLE_SIZE_VALUE(new BaseEsField("sampleSizeValue", DwcTerm.sampleSizeValue)),
  RELATIVE_ORGANISM_QUANTITY(new BaseEsField("relativeOrganismQuantity", GbifTerm.relativeOrganismQuantity)),

  //Crawling
  CRAWL_ID(new BaseEsField("crawlId", GbifInternalTerm.crawlId)),
  LAST_INTERPRETED(new BaseEsField("created", GbifTerm.lastInterpreted)),
  LAST_CRAWLED(new BaseEsField("lastCrawled", GbifTerm.lastCrawled)),
  LAST_PARSED(new BaseEsField("created", GbifTerm.lastParsed)),

  //Media
  MEDIA_TYPE(new BaseEsField("mediaTypes", GbifTerm.mediaType)),
  MEDIA_ITEMS(new BaseEsField("multimediaItems", EsField.MEDIA_ITEMS)),

  //Issues
  ISSUE(new BaseEsField("issues", GbifTerm.issue)),

  ESTABLISHMENT_MEANS(new BaseEsField("establishmentMeans.lineage", "establishmentMeans.concept", DwcTerm.establishmentMeans)),
  DEGREE_OF_ESTABLISHMENT_MEANS(new BaseEsField("degreeOfEstablishment.lineage", "degreeOfEstablishment.concept", DwcTerm.degreeOfEstablishment)),
  PATHWAY(new BaseEsField("pathway.lineage", "pathway.concept", DwcTerm.pathway)),
  FACTS(new BaseEsField("measurementOrFactItems", EsField.FACTS)),
  GBIF_ID(new BaseEsField("gbifId", GbifTerm.gbifID)),
  FULL_TEXT(new BaseEsField("all", EsField.ALL)),
  IS_IN_CLUSTER(new BaseEsField("isClustered", GbifInternalTerm.isInCluster)),
  EXTENSIONS(new BaseEsField("extensions", GbifInternalTerm.dwcaExtension)),

  //Event
  EVENT_TYPE(new BaseEsField("eventType", DwcTerm.eventType)),
  LOCATION_ID(new BaseEsField("locationID", DwcTerm.locationID)),
  PARENTS_LINEAGE(new BaseEsField("parentsLineage", EsField.PARENTS_LINEAGE)),

  //Verbatim
  VERBATIM(new BaseEsField("verbatim", EsField.VERBATIM));

  private final BaseEsField esField;

  public BaseEsField getEsField() {
    return esField;
  }

  OccurrenceEsField(BaseEsField esField) {
    this.esField = esField;
  }

  public static final ImmutableMap<OccurrenceSearchParameter, EsField> SEARCH_TO_ES_MAPPING =
    ImmutableMap.<OccurrenceSearchParameter, EsField>builder()
      .put(OccurrenceSearchParameter.DECIMAL_LATITUDE, LATITUDE)
      .put(OccurrenceSearchParameter.DECIMAL_LONGITUDE, LONGITUDE)
      .put(OccurrenceSearchParameter.YEAR, YEAR)
      .put(OccurrenceSearchParameter.MONTH, MONTH)
      .put(OccurrenceSearchParameter.START_DAY_OF_YEAR, START_DAY_OF_YEAR)
      .put(OccurrenceSearchParameter.END_DAY_OF_YEAR, END_DAY_OF_YEAR)
      .put(OccurrenceSearchParameter.CATALOG_NUMBER, CATALOG_NUMBER)
      .put(OccurrenceSearchParameter.RECORDED_BY, RECORDED_BY)
      .put(OccurrenceSearchParameter.IDENTIFIED_BY, IDENTIFIED_BY)
      .put(OccurrenceSearchParameter.RECORD_NUMBER, RECORD_NUMBER)
      .put(OccurrenceSearchParameter.COLLECTION_CODE, COLLECTION_CODE)
      .put(OccurrenceSearchParameter.INSTITUTION_CODE, INSTITUTION_CODE)
      .put(OccurrenceSearchParameter.DEPTH, DEPTH)
      .put(OccurrenceSearchParameter.ELEVATION, ELEVATION)
      .put(OccurrenceSearchParameter.BASIS_OF_RECORD, BASIS_OF_RECORD)
      .put(OccurrenceSearchParameter.SEX, SEX)
      .put(OccurrenceSearchParameter.IS_SEQUENCED, IS_SEQUENCED)
      .put(OccurrenceSearchParameter.DATASET_KEY, DATASET_KEY)
      .put(OccurrenceSearchParameter.HAS_GEOSPATIAL_ISSUE, HAS_GEOSPATIAL_ISSUES)
      .put(OccurrenceSearchParameter.HAS_COORDINATE, HAS_COORDINATE)
      .put(OccurrenceSearchParameter.EVENT_DATE, EVENT_DATE)
      .put(OccurrenceSearchParameter.MODIFIED, MODIFIED)
      .put(OccurrenceSearchParameter.LAST_INTERPRETED, LAST_INTERPRETED)
      .put(OccurrenceSearchParameter.COUNTRY, COUNTRY_CODE)
      .put(OccurrenceSearchParameter.GBIF_REGION, GBIF_REGION)
      .put(OccurrenceSearchParameter.PUBLISHING_COUNTRY, PUBLISHING_COUNTRY)
      .put(OccurrenceSearchParameter.PUBLISHED_BY_GBIF_REGION, PUBLISHED_BY_GBIF_REGION)
      .put(OccurrenceSearchParameter.CONTINENT, CONTINENT)
      .put(OccurrenceSearchParameter.TAXON_KEY, TAXON_KEY)
      .put(OccurrenceSearchParameter.ACCEPTED_TAXON_KEY, ACCEPTED_TAXON_KEY)
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
      .put(OccurrenceSearchParameter.TAXON_CONCEPT_ID, TAXON_CONCEPT_ID)
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
      .put(OccurrenceSearchParameter.PREVIOUS_IDENTIFICATIONS, PREVIOUS_IDENTIFICATIONS)
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
      .put(OccurrenceSearchParameter.ISLAND, ISLAND)
      .put(OccurrenceSearchParameter.ISLAND_GROUP, ISLAND_GROUP)
      .put(OccurrenceSearchParameter.GEOREFERENCED_BY, GEOREFERENCED_BY)
      .put(OccurrenceSearchParameter.HIGHER_GEOGRAPHY, HIGHER_GEOGRAPHY)
      .put(OccurrenceSearchParameter.FIELD_NUMBER, FIELD_NUMBER)
      .put(OccurrenceSearchParameter.EARLIEST_EON_OR_LOWEST_EONOTHEM, EARLIEST_EON_OR_LOWEST_EONOTHEM)
      .put(OccurrenceSearchParameter.LATEST_EON_OR_HIGHEST_EONOTHEM, LATEST_EON_OR_HIGHEST_EONOTHEM)
      .put(OccurrenceSearchParameter.EARLIEST_ERA_OR_LOWEST_ERATHEM, EARLIEST_ERA_OR_LOWEST_ERATHEM)
      .put(OccurrenceSearchParameter.LATEST_ERA_OR_HIGHEST_ERATHEM, LATEST_ERA_OR_HIGHEST_ERATHEM)
      .put(OccurrenceSearchParameter.EARLIEST_PERIOD_OR_LOWEST_SYSTEM, EARLIEST_PERIOD_OR_LOWEST_SYSTEM)
      .put(OccurrenceSearchParameter.LATEST_PERIOD_OR_HIGHEST_SYSTEM, LATEST_PERIOD_OR_HIGHEST_SYSTEM)
      .put(OccurrenceSearchParameter.EARLIEST_EPOCH_OR_LOWEST_SERIES, EARLIEST_EPOCH_OR_LOWEST_SERIES)
      .put(OccurrenceSearchParameter.LATEST_EPOCH_OR_HIGHEST_SERIES, LATEST_EPOCH_OR_HIGHEST_SERIES)
      .put(OccurrenceSearchParameter.EARLIEST_AGE_OR_LOWEST_STAGE, EARLIEST_AGE_OR_LOWEST_STAGE)
      .put(OccurrenceSearchParameter.LATEST_AGE_OR_HIGHEST_STAGE, LATEST_AGE_OR_HIGHEST_STAGE)
      .put(OccurrenceSearchParameter.LOWEST_BIOSTRATIGRAPHIC_ZONE, LOWEST_BIOSTRATIGRAPHIC_ZONE)
      .put(OccurrenceSearchParameter.HIGHEST_BIOSTRATIGRAPHIC_ZONE, HIGHEST_BIOSTRATIGRAPHIC_ZONE)
      .put(OccurrenceSearchParameter.GROUP, GROUP)
      .put(OccurrenceSearchParameter.FORMATION, FORMATION)
      .put(OccurrenceSearchParameter.MEMBER, MEMBER)
      .put(OccurrenceSearchParameter.BED, BED)
      .put(OccurrenceSearchParameter.ASSOCIATED_SEQUENCES, ASSOCIATED_SEQUENCES)
      .put(OccurrenceSearchParameter.GBIF_ID, GBIF_ID)
      .build();

  public static final ImmutableMap<OccurrenceSearchParameter, EsField> FACET_TO_ES_MAPPING =
    ImmutableMap.<OccurrenceSearchParameter, EsField>builder()
      .put(OccurrenceSearchParameter.EVENT_DATE, EVENT_DATE_INTERVAL)
      .build();
  private static final Set<EsField> DATE_FIELDS = ImmutableSet.of(EVENT_DATE, DATE_IDENTIFIED, MODIFIED, LAST_INTERPRETED, LAST_CRAWLED,LAST_PARSED);

  public static OccurrenceBaseEsFieldMapper buildFieldMapper() {
      return OccurrenceBaseEsFieldMapper.builder()
        .fullTextField(FULL_TEXT)
        .geoShapeField(COORDINATE_SHAPE)
        .geoDistanceField(COORDINATE_POINT)
        .uniqueIdField(ID)
        .defaultFilter(Optional.empty())
        .defaultSort(ImmutableList.of(SortBuilders.fieldSort(YEAR.getSearchFieldName()).order(SortOrder.DESC),
                                      SortBuilders.fieldSort(MONTH.getSearchFieldName()).order(SortOrder.ASC),
                                      SortBuilders.fieldSort(ID.getSearchFieldName()).order(SortOrder.ASC)))
        .searchToEsMapping(SEARCH_TO_ES_MAPPING)
        .dateFields(DATE_FIELDS)
        .facetToEsMapping(FACET_TO_ES_MAPPING)
        .fieldEnumClass(OccurrenceEsField.class)
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
