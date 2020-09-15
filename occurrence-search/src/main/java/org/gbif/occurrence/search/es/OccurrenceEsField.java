package org.gbif.occurrence.search.es;

import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GadmTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;

/** Enum that contains the mapping of symbolic names and field names of valid Elasticsearch fields. */
public enum OccurrenceEsField {

  ID("id", DcTerm.identifier),

  //Dataset derived
  DATASET_KEY("datasetKey", GbifTerm.datasetKey),
  PUBLISHING_COUNTRY("publishingCountry", GbifTerm.publishingCountry),
  PUBLISHING_ORGANIZATION_KEY("publishingOrganizationKey", GbifInternalTerm.publishingOrgKey),
  INSTALLATION_KEY("installationKey", GbifInternalTerm.installationKey),
  NETWORK_KEY("networkKeys", GbifInternalTerm.networkKey),
  PROTOCOL("protocol", GbifTerm.protocol),
  LICENSE("license", DcTerm.license),
  PROJECT_ID("projectId", GbifInternalTerm.projectId),
  PROGRAMME("programmeAcronym", GbifInternalTerm.programmeAcronym),

  //Core identification
  INSTITUTION_CODE("institutionCode", DwcTerm.institutionCode, true),
  COLLECTION_CODE("collectionCode", DwcTerm.collectionCode, true),
  CATALOG_NUMBER("catalogNumber", DwcTerm.catalogNumber, true),

  ORGANISM_ID("organismId", DwcTerm.organismID, true),
  OCCURRENCE_ID("occurrenceId", DwcTerm.occurrenceID, true),
  RECORDED_BY("recordedBy", DwcTerm.recordedBy, true),
  IDENTIFIED_BY("identifiedBy", DwcTerm.identifiedBy, true),
  RECORDED_BY_ID("recordedByIds", GbifTerm.recordedByID),
  RECORDED_BY_ID_VALUE("recordedByIds.value", GbifTerm.recordedByID),
  IDENTIFIED_BY_ID("identifiedByIds", GbifTerm.identifiedByID),
  IDENTIFIED_BY_ID_VALUE("identifiedByIds.value", GbifTerm.identifiedByID),
  RECORD_NUMBER("recordNumber", DwcTerm.recordNumber, true),
  BASIS_OF_RECORD("basisOfRecord", DwcTerm.basisOfRecord),
  TYPE_STATUS("typeStatus", DwcTerm.typeStatus),
  OCCURRENCE_STATUS("occurrenceStatus", DwcTerm.occurrenceStatus),

  //Temporal
  YEAR("year", DwcTerm.year),
  MONTH("month", DwcTerm.month),
  DAY("day", DwcTerm.day),
  EVENT_DATE("eventDateSingle", DwcTerm.eventDate),

  //Location
  COORDINATE_SHAPE("scoordinates", null),
  COORDINATE_POINT("coordinates", null),
  LATITUDE("decimalLatitude", DwcTerm.decimalLatitude),
  LONGITUDE("decimalLongitude", DwcTerm.decimalLongitude),
  COUNTRY_CODE("countryCode", DwcTerm.countryCode),
  CONTINENT("continent", DwcTerm.continent),
  COORDINATE_ACCURACY("coordinateAccuracy", GbifTerm.coordinateAccuracy),
  ELEVATION_ACCURACY("elevationAccuracy", GbifTerm.elevationAccuracy),
  DEPTH_ACCURACY("depthAccuracy", GbifTerm.depthAccuracy),
  ELEVATION("elevation", GbifTerm.elevation),
  DEPTH("depth", GbifTerm.depth),
  STATE_PROVINCE("stateProvince", DwcTerm.stateProvince, true), //NOT INTERPRETED
  WATER_BODY("waterBody", DwcTerm.waterBody, true),
  LOCALITY("locality", DwcTerm.locality, true),
  COORDINATE_PRECISION("coordinatePrecision", DwcTerm.coordinatePrecision),
  COORDINATE_UNCERTAINTY_IN_METERS("coordinateUncertaintyInMeters", DwcTerm.coordinateUncertaintyInMeters),
  GADM_GID("gadm.gid", null),
  GADM_LEVEL_0_GID("gadm.level0Gid", GadmTerm.level0Gid),
  GADM_LEVEL_0_NAME("gadm.level0Name", GadmTerm.level0Name),
  GADM_LEVEL_1_GID("gadm.level1Gid", GadmTerm.level1Gid),
  GADM_LEVEL_1_NAME("gadm.level1Name", GadmTerm.level1Name),
  GADM_LEVEL_2_GID("gadm.level2Gid", GadmTerm.level2Gid),
  GADM_LEVEL_2_NAME("gadm.level2Name", GadmTerm.level2Name),
  GADM_LEVEL_3_GID("gadm.level3Gid", GadmTerm.level3Gid),
  GADM_LEVEL_3_NAME("gadm.level3Name", GadmTerm.level3Name),

  //Location GBIF specific
  HAS_GEOSPATIAL_ISSUES("hasGeospatialIssue", GbifTerm.hasGeospatialIssues),
  HAS_COORDINATE("hasCoordinate", GbifTerm.hasCoordinate),
  REPATRIATED("repatriated", GbifTerm.repatriated),

  //Taxonomic classification
  TAXON_KEY("gbifClassification.taxonKey", GbifTerm.taxonKey),
  USAGE_TAXON_KEY("gbifClassification.usage.key", GbifTerm.taxonKey),
  TAXON_RANK("gbifClassification.usage.rank", DwcTerm.taxonRank),
  ACCEPTED_TAXON_KEY("gbifClassification.acceptedUsage.key", GbifTerm.acceptedTaxonKey),
  ACCEPTED_SCIENTIFIC_NAME("gbifClassification.acceptedUsage.name", GbifTerm.acceptedScientificName),
  KINGDOM_KEY("gbifClassification.kingdomKey", GbifTerm.kingdomKey),
  KINGDOM("gbifClassification.kingdom", DwcTerm.kingdom),
  PHYLUM_KEY("gbifClassification.phylumKey", GbifTerm.phylumKey),
  PHYLUM("gbifClassification.phylum", DwcTerm.phylum),
  CLASS_KEY("gbifClassification.classKey", GbifTerm.classKey),
  CLASS("gbifClassification.class", DwcTerm.class_),
  ORDER_KEY("gbifClassification.orderKey", GbifTerm.orderKey),
  ORDER("gbifClassification.order", DwcTerm.order),
  FAMILY_KEY("gbifClassification.familyKey", GbifTerm.familyKey),
  FAMILY("gbifClassification.family", DwcTerm.family),
  GENUS_KEY("gbifClassification.genusKey", GbifTerm.genusKey),
  GENUS("gbifClassification.genus", DwcTerm.genus),
  SUBGENUS_KEY("gbifClassification.subgenusKey", GbifTerm.subgenusKey),
  SUBGENUS("gbifClassification.subgenus", DwcTerm.subgenus),
  SPECIES_KEY("gbifClassification.speciesKey", GbifTerm.speciesKey),
  SPECIES("gbifClassification.species", GbifTerm.species),
  SCIENTIFIC_NAME("gbifClassification.usage.name", DwcTerm.scientificName),
  SPECIFIC_EPITHET("gbifClassification.usageParsedName.specificEpithet", DwcTerm.specificEpithet),
  INFRA_SPECIFIC_EPITHET("gbifClassification.usageParsedName.infraspecificEpithet", DwcTerm.infraspecificEpithet),
  GENERIC_NAME("gbifClassification.usageParsedName.genericName", GbifTerm.genericName),
  TAXONOMIC_STATUS("gbifClassification.diagnostics.status", DwcTerm.taxonomicStatus),
  TAXON_ID("gbifClassification.taxonID", DwcTerm.taxonID),
  VERBATIM_SCIENTIFIC_NAME("gbifClassification.verbatimScientificName", GbifTerm.verbatimScientificName),

  // GrSciColl
  COLLECTION_KEY("collectionKey", GbifInternalTerm.collectionKey),
  INSTITUTION_KEY("institutionKey", GbifInternalTerm.institutionKey),

  //Sampling
  EVENT_ID("eventId", DwcTerm.eventID, true),
  PARENT_EVENT_ID("parentEventId", DwcTerm.parentEventID, true),
  SAMPLING_PROTOCOL("samplingProtocol", DwcTerm.samplingProtocol, true),
  LIFE_STAGE("lifeStage", DwcTerm.lifeStage),
  DATE_IDENTIFIED("dateIdentified", DwcTerm.dateIdentified),
  MODIFIED("modified", DcTerm.modified),
  REFERENCES("references", DcTerm.references),
  SEX("sex", DwcTerm.sex),
  IDENTIFIER("identifier", DcTerm.identifier),
  INDIVIDUAL_COUNT("individualCount", DwcTerm.individualCount),
  RELATION("relation", DcTerm.relation),
  TYPIFIED_NAME("typifiedName", GbifTerm.typifiedName),
  ORGANISM_QUANTITY("organismQuantity", DwcTerm.organismQuantity),
  ORGANISM_QUANTITY_TYPE("organismQuantityType", DwcTerm.organismQuantityType),
  SAMPLE_SIZE_UNIT("sampleSizeUnit", DwcTerm.sampleSizeUnit),
  SAMPLE_SIZE_VALUE("sampleSizeValue", DwcTerm.sampleSizeValue),
  RELATIVE_ORGANISM_QUANTITY("relativeOrganismQuantity", GbifTerm.relativeOrganismQuantity),

  //Crawling
  CRAWL_ID("crawlId", GbifInternalTerm.crawlId),
  LAST_INTERPRETED("created", GbifTerm.lastInterpreted),
  LAST_CRAWLED("lastCrawled", GbifTerm.lastCrawled),
  LAST_PARSED("created", GbifTerm.lastParsed),

  //Media
  MEDIA_TYPE("mediaTypes", GbifTerm.mediaType),
  MEDIA_ITEMS("multimediaItems", null),

  //Issues
  ISSUE("issues", GbifTerm.issue),

  ESTABLISHMENT_MEANS("establishmentMeans", DwcTerm.establishmentMeans),
  FACTS("measurementOrFactItems", null),
  GBIF_ID("gbifId", GbifTerm.gbifID),
  FULL_TEXT("all", null);


  private final String fieldName;

  private final Term term;

  private boolean autosuggest;

  OccurrenceEsField(String fieldName, Term term) {
    this.fieldName = fieldName;
    this.term = term;
    this.autosuggest = false;
  }

  OccurrenceEsField(String fieldName, Term term, boolean autosuggest) {
    this.fieldName = fieldName;
    this.term = term;
    this.autosuggest = autosuggest;
  }

  /** @return the fieldName */
  public String getFieldName() {
    return fieldName;
  }

  public String getExactMatchFieldName() {
    if (autosuggest) {
      return fieldName + ".keyword";
    }
    return fieldName;
  }

  public String getVerbatimFieldName() {
    if (autosuggest) {
      return fieldName + ".verbatim";
    }
    return fieldName;
  }

  public String getSuggestFieldName() {
    if (autosuggest) {
      return fieldName + ".suggest";
    }
    return fieldName;
  }

  /** @return the term */
  public Term getTerm() {
    return term;
  }

  public boolean isAutosuggest() {
    return autosuggest;
  }
}
