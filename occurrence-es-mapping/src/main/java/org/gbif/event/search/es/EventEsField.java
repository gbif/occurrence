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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Optional;
import java.util.Set;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.EcoTerm;
import org.gbif.dwc.terms.GadmTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.IucnTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.UnknownTerm;
import org.gbif.occurrence.search.es.BaseEsField;
import org.gbif.occurrence.search.es.ChecklistEsField;
import org.gbif.occurrence.search.es.EsField;
import org.gbif.occurrence.search.es.OccurrenceBaseEsFieldMapper;

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

  ORGANISM_ID(new BaseEsField("event.organismId", DwcTerm.organismID, true, true)),
  OCCURRENCE_ID(new BaseEsField("event.occurrenceId", DwcTerm.occurrenceID, true, true)),
  RECORDED_BY(new BaseEsField("event.recordedBy", DwcTerm.recordedBy, true, true)),
  IDENTIFIED_BY(new BaseEsField("event.identifiedBy", DwcTerm.identifiedBy, true, true)),
  RECORDED_BY_ID(new BaseEsField("event.recordedByIds", "event.recordedByIds.value", DwcTerm.recordedByID)),
  IDENTIFIED_BY_ID(new BaseEsField("event.identifiedByIds", "event.identifiedByIds.value", DwcTerm.identifiedByID)),
  RECORD_NUMBER(new BaseEsField("event.recordNumber", DwcTerm.recordNumber, true, true)),
  BASIS_OF_RECORD(new BaseEsField("event.basisOfRecord", DwcTerm.basisOfRecord)),
  TYPE_STATUS(new BaseEsField("event.typeStatus.lineage", "typeStatus.concept", DwcTerm.typeStatus)),
  OCCURRENCE_STATUS(new BaseEsField("event.occurrenceStatus", DwcTerm.occurrenceStatus)),
  IS_SEQUENCED(new BaseEsField("event.isSequenced", GbifTerm.isSequenced)),
  ASSOCIATED_SEQUENCES(new BaseEsField("event.associatedSequences", DwcTerm.associatedSequences)),
  DATASET_ID(new BaseEsField("event.datasetID", DwcTerm.datasetID)),
  DATASET_NAME(new BaseEsField("event.datasetName", DwcTerm.datasetName, true, true)),
  OTHER_CATALOG_NUMBERS(new BaseEsField("event.otherCatalogNumbers", DwcTerm.otherCatalogNumbers, true, true)),
  PREPARATIONS(new BaseEsField("event.preparations", DwcTerm.preparations, true, true)),

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
  DISTANCE_FROM_CENTROID_IN_METERS(new BaseEsField("event.distanceFromCentroidInMeters", GbifTerm.distanceFromCentroidInMeters)),
  ISLAND(new BaseEsField("event.island", DwcTerm.island)),
  ISLAND_GROUP(new BaseEsField("event.islandGroup", DwcTerm.islandGroup)),
  HIGHER_GEOGRAPHY(new BaseEsField("event.higherGeography", DwcTerm.higherGeography)),
  GEOREFERENCED_BY(new BaseEsField("event.georeferencedBy", DwcTerm.georeferencedBy)),

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
  EVENT_ID(new BaseEsField("event.eventID", DwcTerm.eventID, true, true)),
  PARENT_EVENT_ID(new BaseEsField("event.parentEventID", DwcTerm.parentEventID, true,true)),
  EVENT_ID_HIERARCHY(new BaseEsField("event.eventHierarchy", null, true,true)),
  SAMPLING_PROTOCOL(new BaseEsField("event.samplingProtocol", DwcTerm.samplingProtocol, true,true)),
  LIFE_STAGE(new BaseEsField("event.lifeStage.lineage", "lifeStage.concept", DwcTerm.lifeStage)),
  DATE_IDENTIFIED(new BaseEsField("event.dateIdentified", DwcTerm.dateIdentified)),
  MODIFIED(new BaseEsField("event.modified", DcTerm.modified)),
  REFERENCES(new BaseEsField("event.references", DcTerm.references)),
  SEX(new BaseEsField("event.sex.lineage", "sex.concept", DwcTerm.sex)),
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

  // DNA
  DNA_SEQUENCE_ID(new BaseEsField("event.dnaSequenceID", GbifTerm.dnaSequenceID)),

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
  EVENT_TYPE(new BaseEsField("event.eventType.lineage", "event.eventType.concept", DwcTerm.eventType)),
  VERBATIM_EVENT_TYPE(new BaseEsField("event.verbatimEventType", null)),
  LOCATION_ID(new BaseEsField("event.locationID", DwcTerm.locationID)),
  PARENTS_LINEAGE(new BaseEsField("event.parentsLineage", UnknownTerm.build("event.parentsLineage"))),

  // Humboldt
  HUMBOLDT_ITEMS(new BaseEsField("event.humboldt", null)),
  HUMBOLDT_SITE_COUNT(new BaseEsField("event.humboldt.siteCount", EcoTerm.siteCount)),
  HUMBOLDT_VERBATIM_SITE_NAMES(new BaseEsField("event.humboldt.verbatimSiteNames", EcoTerm.verbatimSiteNames)),
  HUMBOLDT_VERBATIM_SITE_DESCRIPTIONS(new BaseEsField("event.humboldt.verbatimSiteDescriptions", EcoTerm.verbatimSiteDescriptions)),
  HUMBOLDT_GEOSPATIAL_SCOPE_AREA_VALUE(new BaseEsField("event.humboldt.geospatialScopeArea.value", EcoTerm.geospatialScopeAreaValue)),
  HUMBOLDT_GEOSPATIAL_SCOPE_AREA_UNIT(new BaseEsField("event.humboldt.geospatialScopeArea.unit", EcoTerm.geospatialScopeAreaUnit)),
  HUMBOLDT_TOTAL_AREA_SAMPLED_VALUE(new BaseEsField("event.humboldt.totalAreaSampled.value", EcoTerm.totalAreaSampledValue)),
  HUMBOLDT_TOTAL_AREA_SAMPLED_UNIT(new BaseEsField("event.humboldt.totalAreaSampled.unit", EcoTerm.totalAreaSampledUnit)),
  HUMBOLDT_TARGET_HABITAT_SCOPE(new BaseEsField("event.humboldt.targetHabitatScope", EcoTerm.targetHabitatScope)),
  HUMBOLDT_EXCLUDED_HABITAT_SCOPE(new BaseEsField("event.humboldt.excludedHabitatScope", EcoTerm.excludedHabitatScope)),
  HUMBOLDT_EVENT_DURATION_VALUE_IN_MINUTES(new BaseEsField("event.humboldt.eventDurationValueInMinutes", null)),
  HUMBOLDT_EVENT_DURATION_VALUE(new BaseEsField("event.humboldt.eventDurationValue.value", EcoTerm.eventDurationValue)),
  HUMBOLDT_EVENT_DURATION_UNIT(new BaseEsField("event.humboldt.eventDurationValue.unit", EcoTerm.eventDurationUnit)),
  HUMBOLDT_TARGET_TAXONOMIC_SCOPE(new BaseEsField("event.humboldt.targetTaxonomicScope", EcoTerm.targetTaxonomicScope)),
  HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_NAME(new ChecklistEsField("event.humboldt.targetTaxonomicScope.%s.usageName", null)),
  HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_KEY(new ChecklistEsField("event.humboldt.targetTaxonomicScope.%s.usageKey", null)),
  HUMBOLDT_TARGET_TAXONOMIC_SCOPE_TAXON_KEY(new ChecklistEsField("event.humboldt.targetTaxonomicScope.%s.taxonKeys", null)),
  HUMBOLDT_TAXONOMIC_ISSUE(new ChecklistEsField("event.humboldt.targetTaxonomicScope.%s.issues", GbifTerm.taxonomicIssue)),
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
  HUMBOLDT_SAMPLING_EFFORT_VALUE(new BaseEsField("event.humboldt.samplingEffort.value", EcoTerm.samplingEffortValue)),
  HUMBOLDT_SAMPLING_EFFORT_UNIT(new BaseEsField("event.humboldt.samplingEffort.unit", EcoTerm.samplingEffortUnit)),

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
      .put(OccurrenceSearchParameter.EVENT_ID_HIERARCHY, EVENT_ID_HIERARCHY)
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
      .put(OccurrenceSearchParameter.EVENT_TYPE, EVENT_TYPE)
      .put(OccurrenceSearchParameter.VERBATIM_EVENT_TYPE, VERBATIM_EVENT_TYPE)
      .put(OccurrenceSearchParameter.HUMBOLDT_SITE_COUNT, HUMBOLDT_SITE_COUNT)
      .put(OccurrenceSearchParameter.HUMBOLDT_VERBATIM_SITE_NAMES, HUMBOLDT_VERBATIM_SITE_NAMES)
      .put(OccurrenceSearchParameter.HUMBOLDT_GEOSPATIAL_SCOPE_AREA_VALUE, HUMBOLDT_GEOSPATIAL_SCOPE_AREA_VALUE)
      .put(OccurrenceSearchParameter.HUMBOLDT_GEOSPATIAL_SCOPE_AREA_UNIT, HUMBOLDT_GEOSPATIAL_SCOPE_AREA_UNIT)
      .put(OccurrenceSearchParameter.HUMBOLDT_TOTAL_AREA_SAMPLED_VALUE, HUMBOLDT_TOTAL_AREA_SAMPLED_VALUE)
      .put(OccurrenceSearchParameter.HUMBOLDT_TOTAL_AREA_SAMPLED_UNIT, HUMBOLDT_TOTAL_AREA_SAMPLED_UNIT)
      .put(OccurrenceSearchParameter.HUMBOLDT_TARGET_HABITAT_SCOPE, HUMBOLDT_TARGET_HABITAT_SCOPE)
      .put(OccurrenceSearchParameter.HUMBOLDT_EVENT_DURATION_VALUE, HUMBOLDT_EVENT_DURATION_VALUE_IN_MINUTES)
      .put(OccurrenceSearchParameter.HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_NAME, HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_NAME)
      .put(OccurrenceSearchParameter.HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_KEY, HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_KEY)
      .put(OccurrenceSearchParameter.HUMBOLDT_TARGET_TAXONOMIC_SCOPE_TAXON_KEY, HUMBOLDT_TARGET_TAXONOMIC_SCOPE_TAXON_KEY)
      .put(OccurrenceSearchParameter.TAXONOMIC_ISSUE, HUMBOLDT_TAXONOMIC_ISSUE)
      .put(OccurrenceSearchParameter.HUMBOLDT_TAXON_COMPLETENESS_PROTOCOLS, HUMBOLDT_TAXON_COMPLETENESS_PROTOCOLS)
      .put(OccurrenceSearchParameter.HUMBOLDT_IS_TAXONOMIC_SCOPE_FULLY_REPORTED, HUMBOLDT_IS_TAXONOMIC_SCOPE_FULLY_REPORTED)
      .put(OccurrenceSearchParameter.HUMBOLDT_IS_ABSENCE_REPORTED, HUMBOLDT_IS_ABSENCE_REPORTED)
      .put(OccurrenceSearchParameter.HUMBOLDT_HAS_NON_TARGET_TAXA, HUMBOLDT_HAS_NON_TARGET_TAXA)
      .put(OccurrenceSearchParameter.HUMBOLDT_ARE_NON_TARGET_TAXA_FULLY_REPORTED, HUMBOLDT_ARE_NON_TARGET_TAXA_FULLY_REPORTED)
      .put(OccurrenceSearchParameter.HUMBOLDT_TARGET_LIFE_STAGE_SCOPE, HUMBOLDT_TARGET_LIFE_STAGE_SCOPE)
      .put(OccurrenceSearchParameter.HUMBOLDT_IS_LIFE_STAGE_SCOPE_FULLY_REPORTED, HUMBOLDT_IS_LIFE_STAGE_SCOPE_FULLY_REPORTED)
      .put(OccurrenceSearchParameter.HUMBOLDT_TARGET_DEGREE_OF_ESTABLISHMENT_SCOPE, HUMBOLDT_TARGET_DEGREE_OF_ESTABLISHMENT_SCOPE)
      .put(OccurrenceSearchParameter.HUMBOLDT_IS_DEGREE_OF_ESTABLISHMENT_SCOPE_FULLY_REPORTED, HUMBOLDT_IS_DEGREE_OF_ESTABLISHMENT_SCOPE_FULLY_REPORTED)
      .put(OccurrenceSearchParameter.HUMBOLDT_TARGET_GROWTH_FORM_SCOPE, HUMBOLDT_TARGET_GROWTH_FORM_SCOPE)
      .put(OccurrenceSearchParameter.HUMBOLDT_IS_GROWTH_FORM_SCOPE_FULLY_REPORTED,HUMBOLDT_IS_GROWTH_FORM_SCOPE_FULLY_REPORTED)
      .put(OccurrenceSearchParameter.HUMBOLDT_HAS_NON_TARGET_ORGANISMS, HUMBOLDT_HAS_NON_TARGET_ORGANISMS)
      .put(OccurrenceSearchParameter.HUMBOLDT_COMPILATION_TYPES, HUMBOLDT_COMPILATION_TYPES)
      .put(OccurrenceSearchParameter.HUMBOLDT_COMPILATION_SOURCE_TYPES, HUMBOLDT_COMPILATION_SOURCE_TYPES)
      .put(OccurrenceSearchParameter.HUMBOLDT_INVENTORY_TYPES, HUMBOLDT_INVENTORY_TYPES)
      .put(OccurrenceSearchParameter.HUMBOLDT_PROTOCOL_NAMES, HUMBOLDT_PROTOCOL_NAMES)
      .put(OccurrenceSearchParameter.HUMBOLDT_IS_ABUNDANCE_REPORTED, HUMBOLDT_IS_ABUNDANCE_REPORTED)
      .put(OccurrenceSearchParameter.HUMBOLDT_IS_ABUNDANCE_CAP_REPORTED, HUMBOLDT_IS_ABUNDANCE_CAP_REPORTED)
      .put(OccurrenceSearchParameter.HUMBOLDT_ABUNDANCE_CAP, HUMBOLDT_ABUNDANCE_CAP)
      .put(OccurrenceSearchParameter.HUMBOLDT_IS_VEGETATION_COVER_REPORTED, HUMBOLDT_IS_VEGETATION_COVER_REPORTED)
      .put(OccurrenceSearchParameter.HUMBOLDT_IS_LEAST_SPECIFIC_TARGET_CATEGORY_QUANTITY_INCLUSIVE, HUMBOLDT_IS_LEAST_SPECIFIC_TARGET_CATEGORY_QUANTITY_INCLUSIVE)
      .put(OccurrenceSearchParameter.HUMBOLDT_HAS_VOUCHERS, HUMBOLDT_HAS_VOUCHERS)
      .put(OccurrenceSearchParameter.HUMBOLDT_VOUCHER_INSTITUTIONS, HUMBOLDT_VOUCHER_INSTITUTIONS)
      .put(OccurrenceSearchParameter.HUMBOLDT_HAS_MATERIAL_SAMPLES,HUMBOLDT_HAS_MATERIAL_SAMPLES)
      .put(OccurrenceSearchParameter.HUMBOLDT_MATERIAL_SAMPLE_TYPES, HUMBOLDT_MATERIAL_SAMPLE_TYPES)
      .put(OccurrenceSearchParameter.HUMBOLDT_SAMPLING_PERFORMED_BY, HUMBOLDT_SAMPLING_PERFORMED_BY)
      .put(OccurrenceSearchParameter.HUMBOLDT_IS_SAMPLING_EFFORT_REPORTED, HUMBOLDT_IS_SAMPLING_EFFORT_REPORTED)
      .put(OccurrenceSearchParameter.HUMBOLDT_SAMPLING_EFFORT_VALUE, HUMBOLDT_SAMPLING_EFFORT_VALUE)
      .put(OccurrenceSearchParameter.HUMBOLDT_SAMPLING_EFFORT_UNIT, HUMBOLDT_SAMPLING_EFFORT_UNIT)
      .build();

  private static final Set<EsField> DATE_FIELDS =
    ImmutableSet.of(EVENT_DATE, DATE_IDENTIFIED, MODIFIED, LAST_INTERPRETED, LAST_CRAWLED, LAST_PARSED);

  public static OccurrenceBaseEsFieldMapper buildFieldMapper(String defaultChecklistKey) {
    return OccurrenceBaseEsFieldMapper.builder()
      .fullTextField(FULL_TEXT)
      .geoShapeField(COORDINATE_SHAPE)
      .geoDistanceField(COORDINATE_POINT)
      .uniqueIdField(ID)
      .defaultFilter(Optional.of(QueryBuilders.termQuery("type","event")))
      .defaultSort(ImmutableList.of(SortBuilders.fieldSort("yearMonthGbifIdSort").order(SortOrder.ASC)))
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
}
