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
package org.gbif.search.es.event;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.gbif.api.model.event.search.EventSearchParameter;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.EcoTerm;
import org.gbif.dwc.terms.GadmTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.IucnTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.UnknownTerm;
import org.gbif.occurrence.common.TermUtils;
import org.gbif.predicate.query.EsField;
import org.gbif.search.es.BaseEsField;
import org.gbif.search.es.ChecklistEsField;

/** Enum that contains the mapping of symbolic names and field names of valid Elasticsearch fields. */
public enum EventEsField implements EsField {

  ID(new BaseEsField("id", DcTerm.identifier)),

  //Dataset derived
  DATASET_KEY(new BaseEsField("metadata.datasetKey", GbifTerm.datasetKey)),
  PUBLISHING_COUNTRY(new BaseEsField("metadata.publishingCountry", GbifTerm.publishingCountry)),
  PUBLISHED_BY_GBIF_REGION(new BaseEsField("publishedByGbifRegion", GbifTerm.publishedByGbifRegion)),
  PUBLISHING_ORGANIZATION_KEY(new BaseEsField("metadata.publishingOrganizationKey", GbifInternalTerm.publishingOrgKey)),
  HOSTING_ORGANIZATION_KEY(new BaseEsField("metadata.hostingOrganizationKey", GbifInternalTerm.hostingOrganizationKey)),
  INSTALLATION_KEY(new BaseEsField("metadata.installationKey", GbifInternalTerm.installationKey)),
  NETWORK_KEY(new BaseEsField("metadata.networkKeys", GbifInternalTerm.networkKey)),
  PROTOCOL(new BaseEsField("metadata.protocol", GbifTerm.protocol)),
  LICENSE(new BaseEsField("metadata.license", DcTerm.license)),
  PROJECT_ID(new BaseEsField("metadata.projectId", GbifTerm.projectId)),
  PROGRAMME(new BaseEsField("metadata.programmeAcronym", GbifInternalTerm.programmeAcronym)),

  //Core identification
  INSTITUTION_CODE(new BaseEsField("event.institutionCode", DwcTerm.institutionCode, true, true)),
  COLLECTION_CODE(new BaseEsField("event.collectionCode", DwcTerm.collectionCode, true, true)),
  CATALOG_NUMBER(new BaseEsField("event.catalogNumber", DwcTerm.catalogNumber, true, true)),


  RECORDED_BY(new BaseEsField("event.recordedBy", DwcTerm.recordedBy, true, true)),
  IDENTIFIED_BY(new BaseEsField("event.identifiedBy", DwcTerm.identifiedBy, true, true)),
  RECORDED_BY_ID(new BaseEsField("event.recordedByIds", "event.recordedByIds.value", DwcTerm.recordedByID)),
  IDENTIFIED_BY_ID(new BaseEsField("event.identifiedByIds", "event.identifiedByIds.value", DwcTerm.identifiedByID)),
  RECORD_NUMBER(new BaseEsField("event.recordNumber", DwcTerm.recordNumber, true, true)),
  BASIS_OF_RECORD(new BaseEsField("event.basisOfRecord", DwcTerm.basisOfRecord)),
  ASSOCIATED_SEQUENCES(new BaseEsField("event.associatedSequences", DwcTerm.associatedSequences)),
  DATASET_ID(new BaseEsField("event.datasetID", DwcTerm.datasetID)),
  DATASET_NAME(new BaseEsField("event.datasetName", DwcTerm.datasetName, true, true)),
  OTHER_CATALOG_NUMBERS(new BaseEsField("event.otherCatalogNumbers", DwcTerm.otherCatalogNumbers, true, true)),
  FIELD_NUMBER(new BaseEsField("event.fieldNumber", DwcTerm.fieldNumber)),

  //Temporal
  YEAR(new BaseEsField("event.year", DwcTerm.year)),
  MONTH(new BaseEsField("event.month", DwcTerm.month)),
  DAY(new BaseEsField("event.day", DwcTerm.day)),
  EVENT_DATE(new BaseEsField("event.eventDate", DwcTerm.eventDate)),
  EVENT_DATE_INTERVAL(new BaseEsField("event.eventDateInterval", EsField.EVENT_DATE_INTERVAL)),
  START_DAY_OF_YEAR(new BaseEsField("event.startDayOfYear", DwcTerm.startDayOfYear)),
  END_DAY_OF_YEAR(new BaseEsField("event.endDayOfYear", DwcTerm.endDayOfYear)),

  //Location
  COORDINATE_SHAPE(new BaseEsField("event.scoordinates", null)),
  COORDINATE_POINT(new BaseEsField("event.coordinates", null)),
  LATITUDE(new BaseEsField("event.decimalLatitude", DwcTerm.decimalLatitude)),
  LONGITUDE(new BaseEsField("event.decimalLongitude", DwcTerm.decimalLongitude)),
  COUNTRY_CODE(new BaseEsField("event.countryCode", DwcTerm.countryCode)),
  GBIF_REGION(new BaseEsField("event.gbifRegion", GbifTerm.gbifRegion)),
  CONTINENT(new BaseEsField("event.continent", DwcTerm.continent)),
  COORDINATE_ACCURACY(new BaseEsField("event.coordinateAccuracy", GbifTerm.coordinateAccuracy)),
  ELEVATION_ACCURACY(new BaseEsField("event.elevationAccuracy", GbifTerm.elevationAccuracy)),
  DEPTH_ACCURACY(new BaseEsField("event.depthAccuracy", GbifTerm.depthAccuracy)),
  ELEVATION(new BaseEsField("event.elevation", GbifTerm.elevation)),
  DEPTH(new BaseEsField("event.depth", GbifTerm.depth)),
  STATE_PROVINCE(new BaseEsField("event.stateProvince", DwcTerm.stateProvince, true, true)), //NOT INTERPRETED
  WATER_BODY(new BaseEsField("event.waterBody", DwcTerm.waterBody, true, true)),
  LOCALITY(new BaseEsField("event.locality", DwcTerm.locality, true, true)),
  COORDINATE_PRECISION(new BaseEsField("event.coordinatePrecision", DwcTerm.coordinatePrecision)),
  COORDINATE_UNCERTAINTY_IN_METERS(new BaseEsField("event.coordinateUncertaintyInMeters", DwcTerm.coordinateUncertaintyInMeters)),
  ISLAND(new BaseEsField("event.island", DwcTerm.island)),
  ISLAND_GROUP(new BaseEsField("event.islandGroup", DwcTerm.islandGroup)),
  HIGHER_GEOGRAPHY(new BaseEsField("event.higherGeography", DwcTerm.higherGeography)),
  GEOREFERENCED_BY(new BaseEsField("event.HIGHER_GEOGRAPHY", DwcTerm.georeferencedBy)),

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


  //Sampling
  EVENT_ID(new BaseEsField("event.eventID", DwcTerm.eventID, true, true)),
  PARENT_EVENT_ID(new BaseEsField("event.parentEventID", DwcTerm.parentEventID, true,true)),
  EVENT_ID_HIERARCHY(new BaseEsField("event.eventHierarchy", null, true,true)),
  SAMPLING_PROTOCOL(new BaseEsField("event.samplingProtocol", DwcTerm.samplingProtocol, true,true)),
  DATE_IDENTIFIED(new BaseEsField("event.dateIdentified", DwcTerm.dateIdentified)),
  MODIFIED(new BaseEsField("event.modified", DcTerm.modified)),
  REFERENCES(new BaseEsField("event.references", DcTerm.references)),
  IDENTIFIER(new BaseEsField("event.identifier", DcTerm.identifier)),
  INDIVIDUAL_COUNT(new BaseEsField("event.individualCount", DwcTerm.individualCount)),
  RELATION(new BaseEsField("event.relation", DcTerm.relation)),
  TYPIFIED_NAME(new BaseEsField("event.typifiedName", GbifTerm.typifiedName)),
  SAMPLE_SIZE_UNIT(new BaseEsField("event.sampleSizeUnit", DwcTerm.sampleSizeUnit)),
  SAMPLE_SIZE_VALUE(new BaseEsField("event.sampleSizeValue", DwcTerm.sampleSizeValue)),

  //Crawling
  CRAWL_ID(new BaseEsField("crawlId", GbifInternalTerm.crawlId)),
  LAST_INTERPRETED(new BaseEsField("created", GbifTerm.lastInterpreted)),
  LAST_CRAWLED(new BaseEsField("lastCrawled", GbifTerm.lastCrawled)),
  LAST_PARSED(new BaseEsField("created", GbifTerm.lastParsed)),

  //Media
  MEDIA_TYPE(new BaseEsField("event.mediaTypes", GbifTerm.mediaType)),
  MEDIA_ITEMS(new BaseEsField("event.multimediaItems", null)),

  // DNA
  DNA_SEQUENCE_ID(new BaseEsField("event.dnaSequenceID", GbifTerm.dnaSequenceID)),

  //Issues
  ISSUE(new BaseEsField("event.issues", GbifTerm.issue)),

  FACTS(new BaseEsField("event.measurementOrFactItems", null)),
  GBIF_ID(new BaseEsField("event.gbifId", GbifTerm.gbifID)),
  FULL_TEXT(new BaseEsField("all", null)),
  EXTENSIONS(new BaseEsField("event.extensions", GbifInternalTerm.dwcaExtension)),

  //Event
  EVENT_TYPE(new BaseEsField("event.eventType.lineage", "event.eventType.concept", DwcTerm.eventType)),
  VERBATIM_EVENT_TYPE(new BaseEsField("event.verbatimEventType", null)),
  LOCATION_ID(new BaseEsField("event.locationID", DwcTerm.locationID)),
  PARENTS_LINEAGE(new BaseEsField("event.parentsLineage", UnknownTerm.build("event.parentsLineage"))),

  // Humboldt
  HUMBOLDT_ITEMS(new BaseEsField("event.humboldt", null)),
  HUMBOLDT_SITE_COUNT(new BaseEsField("event.humboldt.siteCount", EcoTerm.siteCount)),
  HUMBOLDT_VERBATIM_SITE_NAMES(new BaseEsField("event.humboldt.verbatimSiteNames", EcoTerm.verbatimSiteNames)),
  HUMBOLDT_VERBATIM_SITE_DESCRIPTIONS(new BaseEsField("event.humboldt.verbatimSiteDescriptions", EcoTerm.verbatimSiteDescriptions)),
  HUMBOLDT_GEOSPATIAL_SCOPE_AREA_VALUE(new BaseEsField("event.humboldt.geospatialScopeAreaValue", EcoTerm.geospatialScopeAreaValue)),
  HUMBOLDT_GEOSPATIAL_SCOPE_AREA_UNIT(new BaseEsField("event.humboldt.geospatialScopeAreaUnit", EcoTerm.geospatialScopeAreaUnit)),
  HUMBOLDT_TOTAL_AREA_SAMPLED_VALUE(new BaseEsField("event.humboldt.totalAreaSampledValue", EcoTerm.totalAreaSampledValue)),
  HUMBOLDT_TOTAL_AREA_SAMPLED_UNIT(new BaseEsField("event.humboldt.totalAreaSampledUnit", EcoTerm.totalAreaSampledUnit)),
  HUMBOLDT_TARGET_HABITAT_SCOPE(new BaseEsField("event.humboldt.targetHabitatScope", EcoTerm.targetHabitatScope)),
  HUMBOLDT_EXCLUDED_HABITAT_SCOPE(new BaseEsField("event.humboldt.excludedHabitatScope", EcoTerm.excludedHabitatScope)),
  HUMBOLDT_EVENT_DURATION_VALUE_IN_MINUTES(new BaseEsField("event.humboldt.eventDurationValueInMinutes", null)),
  HUMBOLDT_EVENT_DURATION_VALUE(new BaseEsField("event.humboldt.eventDurationValue", EcoTerm.eventDurationValue)),
  HUMBOLDT_EVENT_DURATION_UNIT(new BaseEsField("event.humboldt.eventDurationUnit", EcoTerm.eventDurationUnit)),
  HUMBOLDT_TARGET_TAXONOMIC_SCOPE(new BaseEsField("event.humboldt.targetTaxonomicScope", EcoTerm.targetTaxonomicScope)),
  HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_NAME(new ChecklistEsField("event.humboldt.targetTaxonomicScope.%s.usageName", null, "event.humboldt")),
  HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_KEY(new ChecklistEsField("event.humboldt.targetTaxonomicScope.%s.usageKey", null, "event.humboldt")),
  HUMBOLDT_TARGET_TAXONOMIC_SCOPE_TAXON_KEY(new ChecklistEsField("event.humboldt.targetTaxonomicScope.%s.taxonKeys", null, "event.humboldt")),
  HUMBOLDT_TAXONOMIC_ISSUE(new ChecklistEsField("event.humboldt.targetTaxonomicScope.%s.issues", GbifTerm.taxonomicIssue, "event.humboldt")),
  HUMBOLDT_EXCLUDED_TAXONOMIC_SCOPE(new BaseEsField("event.humboldt.excludedTaxonomicScope", EcoTerm.excludedTaxonomicScope)),
  HUMBOLDT_TAXON_COMPLETENESS_PROTOCOLS(new BaseEsField("event.humboldt.taxonCompletenessProtocols", EcoTerm.taxonCompletenessProtocols)),
  HUMBOLDT_IS_TAXONOMIC_SCOPE_FULLY_REPORTED(new BaseEsField("event.humboldt.isTaxonomicScopeFullyReported", EcoTerm.isTaxonomicScopeFullyReported)),
  HUMBOLDT_IS_ABSENCE_REPORTED(new BaseEsField("event.humboldt.isAbsenceReported", EcoTerm.isAbsenceReported)),
  HUMBOLDT_ABSENT_TAXA(new BaseEsField("event.humboldt.absentTaxa", EcoTerm.absentTaxa)),
  HUMBOLDT_HAS_NON_TARGET_TAXA(new BaseEsField("event.humboldt.hasNonTargetTaxa", EcoTerm.hasNonTargetTaxa)),
  HUMBOLDT_NON_TARGET_TAXA(new BaseEsField("event.humboldt.nonTargetTaxa", EcoTerm.nonTargetTaxa)),
  HUMBOLDT_ARE_NON_TARGET_TAXA_FULLY_REPORTED(new BaseEsField("event.humboldt.areNonTargetTaxaFullyReported", EcoTerm.areNonTargetTaxaFullyReported)),
  HUMBOLDT_TARGET_LIFE_STAGE_SCOPE(new BaseEsField("event.humboldt.targetLifeStageScope.lineage", "event.humboldt.targetLifeStageScope.concept", EcoTerm.targetLifeStageScope)),
  HUMBOLDT_EXCLUDED_LIFE_STAGE_SCOPE(new BaseEsField("event.humboldt.excludedLifeStageScope.lineage", "event.humboldt.excludedLifeStageScope.concept", EcoTerm.excludedLifeStageScope)),
  HUMBOLDT_IS_LIFE_STAGE_SCOPE_FULLY_REPORTED(new BaseEsField("event.humboldt.isLifeStageScopeFullyReported", EcoTerm.isLifeStageScopeFullyReported)),
  HUMBOLDT_TARGET_DEGREE_OF_ESTABLISHMENT_SCOPE(new BaseEsField("event.humboldt.targetDegreeOfEstablishmentScope.lineage", "event.humboldt.targetDegreeOfEstablishmentScope.concept", EcoTerm.targetDegreeOfEstablishmentScope)),
  HUMBOLDT_EXCLUDED_DEGREE_OF_ESTABLISHMENT_SCOPE(new BaseEsField("event.humboldt.excludedDegreeOfEstablishmentScope.lineage", "event.humboldt.excludedDegreeOfEstablishmentScope.concept", EcoTerm.excludedDegreeOfEstablishmentScope)),
  HUMBOLDT_IS_DEGREE_OF_ESTABLISHMENT_SCOPE_FULLY_REPORTED(new BaseEsField("event.humboldt.isDegreeOfEstablishmentScopeFullyReported", EcoTerm.isDegreeOfEstablishmentScopeFullyReported)),
  HUMBOLDT_TARGET_GROWTH_FORM_SCOPE(new BaseEsField("event.humboldt.targetGrowthFormScope", EcoTerm.targetGrowthFormScope)),
  HUMBOLDT_EXCLUDED_GROWTH_FORM_SCOPE(new BaseEsField("event.humboldt.excludedGrowthFormScope", EcoTerm.excludedGrowthFormScope)),
  HUMBOLDT_IS_GROWTH_FORM_SCOPE_FULLY_REPORTED(new BaseEsField("event.humboldt.isGrowthFormScopeFullyReported", EcoTerm.isGrowthFormScopeFullyReported)),
  HUMBOLDT_HAS_NON_TARGET_ORGANISMS(new BaseEsField("event.humboldt.hasNonTargetOrganisms", EcoTerm.hasNonTargetOrganisms)),
  HUMBOLDT_COMPILATION_TYPES(new BaseEsField("event.humboldt.compilationTypes", EcoTerm.compilationTypes)),
  HUMBOLDT_COMPILATION_SOURCE_TYPES(new BaseEsField("event.humboldt.compilationSourceTypes", EcoTerm.compilationSourceTypes)),
  HUMBOLDT_INVENTORY_TYPES(new BaseEsField("event.humboldt.inventoryTypes", EcoTerm.inventoryTypes)),
  HUMBOLDT_PROTOCOL_NAMES(new BaseEsField("event.humboldt.protocolNames", EcoTerm.protocolNames)),
  HUMBOLDT_PROTOCOL_DESCRIPTIONS(new BaseEsField("event.humboldt.protocolDescriptions", EcoTerm.protocolDescriptions)),
  HUMBOLDT_PROTOCOL_REFERENCES(new BaseEsField("event.humboldt.protocolReferences", EcoTerm.protocolReferences)),
  HUMBOLDT_IS_ABUNDANCE_REPORTED(new BaseEsField("event.humboldt.isAbundanceReported", EcoTerm.isAbundanceReported)),
  HUMBOLDT_IS_ABUNDANCE_CAP_REPORTED(new BaseEsField("event.humboldt.isAbundanceCapReported", EcoTerm.isAbundanceCapReported)),
  HUMBOLDT_ABUNDANCE_CAP(new BaseEsField("event.humboldt.abundanceCap", EcoTerm.abundanceCap)),
  HUMBOLDT_IS_VEGETATION_COVER_REPORTED(new BaseEsField("event.humboldt.isVegetationCoverReported", EcoTerm.isVegetationCoverReported)),
  HUMBOLDT_IS_LEAST_SPECIFIC_TARGET_CATEGORY_QUANTITY_INCLUSIVE(new BaseEsField("event.humboldt.isLeastSpecificTargetCategoryQuantityInclusive", EcoTerm.isLeastSpecificTargetCategoryQuantityInclusive)),
  HUMBOLDT_HAS_VOUCHERS(new BaseEsField("event.humboldt.hasVouchers", EcoTerm.hasVouchers)),
  HUMBOLDT_VOUCHER_INSTITUTIONS(new BaseEsField("event.humboldt.voucherInstitutions", EcoTerm.voucherInstitutions)),
  HUMBOLDT_HAS_MATERIAL_SAMPLES(new BaseEsField("event.humboldt.hasMaterialSamples", EcoTerm.hasMaterialSamples)),
  HUMBOLDT_MATERIAL_SAMPLE_TYPES(new BaseEsField("event.humboldt.materialSampleTypes", EcoTerm.materialSampleTypes)),
  HUMBOLDT_SAMPLING_PERFORMED_BY(new BaseEsField("event.humboldt.samplingPerformedBy", EcoTerm.samplingPerformedBy)),
  HUMBOLDT_IS_SAMPLING_EFFORT_REPORTED(new BaseEsField("event.humboldt.isSamplingEffortReported", EcoTerm.isSamplingEffortReported)),
  HUMBOLDT_SAMPLING_EFFORT_VALUE(new BaseEsField("event.humboldt.samplingEffortValue", EcoTerm.samplingEffortValue)),
  HUMBOLDT_SAMPLING_EFFORT_UNIT(new BaseEsField("event.humboldt.samplingEffortUnit", EcoTerm.samplingEffortUnit)),

  //Verbatim
  VERBATIM(new BaseEsField("verbatim", UnknownTerm.build("verbatim")));

  private final BaseEsField esField;

  public BaseEsField getEsField() {
    return esField;
  }

  EventEsField(BaseEsField esField) {
    this.esField = esField;
  }

  public static final ImmutableMap<EventSearchParameter, EsField> SEARCH_TO_ES_MAPPING =
    ImmutableMap.<EventSearchParameter, EsField>builder()
      .put(EventSearchParameter.DECIMAL_LATITUDE, LATITUDE)
      .put(EventSearchParameter.DECIMAL_LONGITUDE, LONGITUDE)
      .put(EventSearchParameter.YEAR, YEAR)
      .put(EventSearchParameter.MONTH, MONTH)
      .put(EventSearchParameter.DAY, DAY)
      .put(EventSearchParameter.START_DAY_OF_YEAR, START_DAY_OF_YEAR)
      .put(EventSearchParameter.END_DAY_OF_YEAR, END_DAY_OF_YEAR)
      .put(EventSearchParameter.CATALOG_NUMBER, CATALOG_NUMBER)
      .put(EventSearchParameter.RECORD_NUMBER, RECORD_NUMBER)
      .put(EventSearchParameter.COLLECTION_CODE, COLLECTION_CODE)
      .put(EventSearchParameter.INSTITUTION_CODE, INSTITUTION_CODE)
      .put(EventSearchParameter.DEPTH, DEPTH)
      .put(EventSearchParameter.ELEVATION, ELEVATION)
      .put(EventSearchParameter.DATASET_KEY, DATASET_KEY)
      .put(EventSearchParameter.HAS_GEOSPATIAL_ISSUE, HAS_GEOSPATIAL_ISSUES)
      .put(EventSearchParameter.HAS_COORDINATE, HAS_COORDINATE)
      .put(EventSearchParameter.EVENT_DATE, EVENT_DATE)
      .put(EventSearchParameter.MODIFIED, MODIFIED)
      .put(EventSearchParameter.LAST_INTERPRETED, LAST_INTERPRETED)
      .put(EventSearchParameter.COUNTRY, COUNTRY_CODE)
      .put(EventSearchParameter.GBIF_REGION, GBIF_REGION)
      .put(EventSearchParameter.PUBLISHING_COUNTRY, PUBLISHING_COUNTRY)
      .put(EventSearchParameter.CONTINENT, CONTINENT)
      .put(EventSearchParameter.ISLAND, ISLAND)
      .put(EventSearchParameter.ISLAND_GROUP, ISLAND_GROUP)
      .put(EventSearchParameter.HIGHER_GEOGRAPHY, HIGHER_GEOGRAPHY)
      .put(EventSearchParameter.GEOREFERENCED_BY, GEOREFERENCED_BY)
      .put(EventSearchParameter.TAXON_KEY, TAXON_KEY)
      .put(EventSearchParameter.KINGDOM_KEY, KINGDOM_KEY)
      .put(EventSearchParameter.PHYLUM_KEY, PHYLUM_KEY)
      .put(EventSearchParameter.CLASS_KEY, CLASS_KEY)
      .put(EventSearchParameter.ORDER_KEY, ORDER_KEY)
      .put(EventSearchParameter.FAMILY_KEY, FAMILY_KEY)
      .put(EventSearchParameter.GENUS_KEY, GENUS_KEY)
      .put(EventSearchParameter.SUBGENUS_KEY, SUBGENUS_KEY)
      .put(EventSearchParameter.SPECIES_KEY, SPECIES_KEY)
      .put(EventSearchParameter.SCIENTIFIC_NAME, SCIENTIFIC_NAME)
      .put(EventSearchParameter.VERBATIM_SCIENTIFIC_NAME, VERBATIM_SCIENTIFIC_NAME)
      .put(EventSearchParameter.TAXON_ID, TAXON_ID)
      .put(EventSearchParameter.TAXONOMIC_STATUS, TAXONOMIC_STATUS)
      .put(EventSearchParameter.MEDIA_TYPE, MEDIA_TYPE)
      .put(EventSearchParameter.ISSUE, ISSUE)
      .put(EventSearchParameter.REPATRIATED, REPATRIATED)
      .put(EventSearchParameter.LOCALITY, LOCALITY)
      .put(EventSearchParameter.COORDINATE_UNCERTAINTY_IN_METERS, COORDINATE_UNCERTAINTY_IN_METERS)
      .put(EventSearchParameter.GADM_GID, GADM_GID)
      .put(EventSearchParameter.GADM_LEVEL_0_GID, GADM_LEVEL_0_GID)
      .put(EventSearchParameter.GADM_LEVEL_1_GID, GADM_LEVEL_1_GID)
      .put(EventSearchParameter.GADM_LEVEL_2_GID, GADM_LEVEL_2_GID)
      .put(EventSearchParameter.GADM_LEVEL_3_GID, GADM_LEVEL_3_GID)
      .put(EventSearchParameter.STATE_PROVINCE, STATE_PROVINCE)
      .put(EventSearchParameter.WATER_BODY, WATER_BODY)
      .put(EventSearchParameter.LICENSE, LICENSE)
      .put(EventSearchParameter.PROTOCOL, PROTOCOL)
      .put(EventSearchParameter.PUBLISHING_ORG, PUBLISHING_ORGANIZATION_KEY)
      .put(EventSearchParameter.HOSTING_ORGANIZATION_KEY, HOSTING_ORGANIZATION_KEY)
      .put(EventSearchParameter.CRAWL_ID, CRAWL_ID)
      .put(EventSearchParameter.INSTALLATION_KEY, INSTALLATION_KEY)
      .put(EventSearchParameter.NETWORK_KEY, NETWORK_KEY)
      .put(EventSearchParameter.EVENT_ID, EVENT_ID)
      .put(EventSearchParameter.PARENT_EVENT_ID, PARENT_EVENT_ID)
      .put(EventSearchParameter.EVENT_ID_HIERARCHY, EVENT_ID_HIERARCHY)
      .put(EventSearchParameter.SAMPLING_PROTOCOL, SAMPLING_PROTOCOL)
      .put(EventSearchParameter.PROJECT_ID, PROJECT_ID)
      .put(EventSearchParameter.PROGRAMME, PROGRAMME)
      .put(EventSearchParameter.SAMPLE_SIZE_VALUE, SAMPLE_SIZE_VALUE)
      .put(EventSearchParameter.SAMPLE_SIZE_UNIT, SAMPLE_SIZE_UNIT)
      .put(EventSearchParameter.DWCA_EXTENSION, EXTENSIONS)
      .put(EventSearchParameter.IUCN_RED_LIST_CATEGORY, IUCN_RED_LIST_CATEGORY)
      .put(EventSearchParameter.DATASET_ID, DATASET_ID)
      .put(EventSearchParameter.DATASET_NAME, DATASET_NAME)
      .put(EventSearchParameter.OTHER_CATALOG_NUMBERS, OTHER_CATALOG_NUMBERS)
      .put(EventSearchParameter.FIELD_NUMBER, FIELD_NUMBER)
      .put(EventSearchParameter.GBIF_ID, GBIF_ID)
      .put(EventSearchParameter.EVENT_TYPE, EVENT_TYPE)
      .put(EventSearchParameter.VERBATIM_EVENT_TYPE, VERBATIM_EVENT_TYPE)
      .put(EventSearchParameter.HUMBOLDT_SITE_COUNT, HUMBOLDT_SITE_COUNT)
      .put(EventSearchParameter.HUMBOLDT_VERBATIM_SITE_NAMES, HUMBOLDT_VERBATIM_SITE_NAMES)
      .put(EventSearchParameter.HUMBOLDT_GEOSPATIAL_SCOPE_AREA_VALUE, HUMBOLDT_GEOSPATIAL_SCOPE_AREA_VALUE)
      .put(EventSearchParameter.HUMBOLDT_GEOSPATIAL_SCOPE_AREA_UNIT, HUMBOLDT_GEOSPATIAL_SCOPE_AREA_UNIT)
      .put(EventSearchParameter.HUMBOLDT_TOTAL_AREA_SAMPLED_VALUE, HUMBOLDT_TOTAL_AREA_SAMPLED_VALUE)
      .put(EventSearchParameter.HUMBOLDT_TOTAL_AREA_SAMPLED_UNIT, HUMBOLDT_TOTAL_AREA_SAMPLED_UNIT)
      .put(EventSearchParameter.HUMBOLDT_TARGET_HABITAT_SCOPE, HUMBOLDT_TARGET_HABITAT_SCOPE)
      .put(EventSearchParameter.HUMBOLDT_EVENT_DURATION_VALUE_IN_MINUTES, HUMBOLDT_EVENT_DURATION_VALUE_IN_MINUTES)
      .put(EventSearchParameter.HUMBOLDT_EVENT_DURATION_VALUE, HUMBOLDT_EVENT_DURATION_VALUE)
      .put(EventSearchParameter.HUMBOLDT_EVENT_DURATION_UNIT, HUMBOLDT_EVENT_DURATION_UNIT)
      .put(EventSearchParameter.HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_NAME, HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_NAME)
      .put(EventSearchParameter.HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_KEY, HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_KEY)
      .put(EventSearchParameter.HUMBOLDT_TARGET_TAXONOMIC_SCOPE_TAXON_KEY, HUMBOLDT_TARGET_TAXONOMIC_SCOPE_TAXON_KEY)
      .put(EventSearchParameter.TAXONOMIC_ISSUE, HUMBOLDT_TAXONOMIC_ISSUE)
      .put(EventSearchParameter.HUMBOLDT_TAXON_COMPLETENESS_PROTOCOLS, HUMBOLDT_TAXON_COMPLETENESS_PROTOCOLS)
      .put(EventSearchParameter.HUMBOLDT_IS_TAXONOMIC_SCOPE_FULLY_REPORTED, HUMBOLDT_IS_TAXONOMIC_SCOPE_FULLY_REPORTED)
      .put(EventSearchParameter.HUMBOLDT_IS_ABSENCE_REPORTED, HUMBOLDT_IS_ABSENCE_REPORTED)
      .put(EventSearchParameter.HUMBOLDT_HAS_NON_TARGET_TAXA, HUMBOLDT_HAS_NON_TARGET_TAXA)
      .put(EventSearchParameter.HUMBOLDT_ARE_NON_TARGET_TAXA_FULLY_REPORTED, HUMBOLDT_ARE_NON_TARGET_TAXA_FULLY_REPORTED)
      .put(EventSearchParameter.HUMBOLDT_TARGET_LIFE_STAGE_SCOPE, HUMBOLDT_TARGET_LIFE_STAGE_SCOPE)
      .put(EventSearchParameter.HUMBOLDT_IS_LIFE_STAGE_SCOPE_FULLY_REPORTED, HUMBOLDT_IS_LIFE_STAGE_SCOPE_FULLY_REPORTED)
      .put(EventSearchParameter.HUMBOLDT_TARGET_DEGREE_OF_ESTABLISHMENT_SCOPE, HUMBOLDT_TARGET_DEGREE_OF_ESTABLISHMENT_SCOPE)
      .put(EventSearchParameter.HUMBOLDT_IS_DEGREE_OF_ESTABLISHMENT_SCOPE_FULLY_REPORTED, HUMBOLDT_IS_DEGREE_OF_ESTABLISHMENT_SCOPE_FULLY_REPORTED)
      .put(EventSearchParameter.HUMBOLDT_TARGET_GROWTH_FORM_SCOPE, HUMBOLDT_TARGET_GROWTH_FORM_SCOPE)
      .put(EventSearchParameter.HUMBOLDT_IS_GROWTH_FORM_SCOPE_FULLY_REPORTED,HUMBOLDT_IS_GROWTH_FORM_SCOPE_FULLY_REPORTED)
      .put(EventSearchParameter.HUMBOLDT_HAS_NON_TARGET_ORGANISMS, HUMBOLDT_HAS_NON_TARGET_ORGANISMS)
      .put(EventSearchParameter.HUMBOLDT_COMPILATION_TYPES, HUMBOLDT_COMPILATION_TYPES)
      .put(EventSearchParameter.HUMBOLDT_COMPILATION_SOURCE_TYPES, HUMBOLDT_COMPILATION_SOURCE_TYPES)
      .put(EventSearchParameter.HUMBOLDT_INVENTORY_TYPES, HUMBOLDT_INVENTORY_TYPES)
      .put(EventSearchParameter.HUMBOLDT_PROTOCOL_NAMES, HUMBOLDT_PROTOCOL_NAMES)
      .put(EventSearchParameter.HUMBOLDT_IS_ABUNDANCE_REPORTED, HUMBOLDT_IS_ABUNDANCE_REPORTED)
      .put(EventSearchParameter.HUMBOLDT_IS_ABUNDANCE_CAP_REPORTED, HUMBOLDT_IS_ABUNDANCE_CAP_REPORTED)
      .put(EventSearchParameter.HUMBOLDT_ABUNDANCE_CAP, HUMBOLDT_ABUNDANCE_CAP)
      .put(EventSearchParameter.HUMBOLDT_IS_VEGETATION_COVER_REPORTED, HUMBOLDT_IS_VEGETATION_COVER_REPORTED)
      .put(EventSearchParameter.HUMBOLDT_IS_LEAST_SPECIFIC_TARGET_CATEGORY_QUANTITY_INCLUSIVE, HUMBOLDT_IS_LEAST_SPECIFIC_TARGET_CATEGORY_QUANTITY_INCLUSIVE)
      .put(EventSearchParameter.HUMBOLDT_HAS_VOUCHERS, HUMBOLDT_HAS_VOUCHERS)
      .put(EventSearchParameter.HUMBOLDT_VOUCHER_INSTITUTIONS, HUMBOLDT_VOUCHER_INSTITUTIONS)
      .put(EventSearchParameter.HUMBOLDT_HAS_MATERIAL_SAMPLES,HUMBOLDT_HAS_MATERIAL_SAMPLES)
      .put(EventSearchParameter.HUMBOLDT_MATERIAL_SAMPLE_TYPES, HUMBOLDT_MATERIAL_SAMPLE_TYPES)
      .put(EventSearchParameter.HUMBOLDT_SAMPLING_PERFORMED_BY, HUMBOLDT_SAMPLING_PERFORMED_BY)
      .put(EventSearchParameter.HUMBOLDT_IS_SAMPLING_EFFORT_REPORTED, HUMBOLDT_IS_SAMPLING_EFFORT_REPORTED)
      .put(EventSearchParameter.HUMBOLDT_SAMPLING_EFFORT_VALUE, HUMBOLDT_SAMPLING_EFFORT_VALUE)
      .put(EventSearchParameter.HUMBOLDT_SAMPLING_EFFORT_UNIT, HUMBOLDT_SAMPLING_EFFORT_UNIT)
      .build();

  private static final Set<EsField> DATE_FIELDS =
    ImmutableSet.of(EVENT_DATE, DATE_IDENTIFIED, MODIFIED, LAST_INTERPRETED, LAST_CRAWLED, LAST_PARSED);

  public static EventEsFieldMapper buildFieldMapper(String defaultChecklistKey) {
    return EventEsFieldMapper.builder()
        .fullTextField(FULL_TEXT)
        .geoShapeField(COORDINATE_SHAPE)
        .geoDistanceField(COORDINATE_POINT)
        .uniqueIdField(ID)
        .defaultFilter(QueryBuilders.termQuery("type", "event"))
        // FIXME: remove comment when reindexed with changes in pipelines
        //
        // .defaultSort(ImmutableList.of(SortBuilders.fieldSort("event.yearMonthEventIdSort").order(SortOrder.ASC)))
        .defaultSort(
            ImmutableList.of(
                SortBuilders.fieldSort(YEAR.getSearchFieldName()).order(SortOrder.DESC),
                SortBuilders.fieldSort(MONTH.getSearchFieldName()).order(SortOrder.ASC),
                SortBuilders.fieldSort(EVENT_ID.getSearchFieldName()).order(SortOrder.ASC)))
        .searchToEsMapping(SEARCH_TO_ES_MAPPING)
        .dateFields(DATE_FIELDS)
        .fieldEnumClass(EventEsField.class)
        .defaulChecklistKey(defaultChecklistKey)
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
    return esField.getSearchFieldName().startsWith("event.humboldt")
        ? "event.humboldt"
        : esField.getNestedPath();
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
    return esField.getSearchFieldName().startsWith("event.humboldt");
  }

  @Override
  public boolean isVocabulary() {
    return TermUtils.isVocabulary(getTerm());
  }

  @Override
  public String childrenRelation() {
    return "occurrence";
  }
}
