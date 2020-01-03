package org.gbif.occurrence.search.es;

/**
 * Enumeration that holds a map
 */

import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;

/** Enum that contains the mapping of symbolic names and field names of valid Solr fields. */
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
  INSTITUTION_CODE("institutionCode", DwcTerm.institutionCode),
  COLLECTION_CODE("collectionCode", DwcTerm.collectionCode),
  CATALOG_NUMBER("catalogNumber", DwcTerm.catalogNumber),

  ORGANISM_ID("organismId", DwcTerm.organismID),
  OCCURRENCE_ID("occurrenceId", DwcTerm.occurrenceID),
  RECORDED_BY("recordedBy", DwcTerm.recordedBy),
  RECORD_NUMBER("recordNumber", DwcTerm.recordNumber),
  BASIS_OF_RECORD("basisOfRecord", DwcTerm.basisOfRecord),
  TYPE_STATUS("typeStatus", DwcTerm.typeStatus),

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
  STATE_PROVINCE("stateProvince", DwcTerm.stateProvince), //NOT INTERPRETED
  WATER_BODY("waterBody", DwcTerm.waterBody),
  LOCALITY("locality", DwcTerm.locality),
  COORDINATE_PRECISION("coordinatePrecision", DwcTerm.coordinatePrecision),
  COORDINATE_UNCERTAINTY_METERS("coordinateUncertaintyInMeters", DwcTerm.coordinateUncertaintyInMeters),

  //Location GBIF specific
  HAS_GEOSPATIAL_ISSUES("hasGeospatialIssue", GbifTerm.hasGeospatialIssues),
  HAS_COORDINATE("hasCoordinate", GbifTerm.hasCoordinate),
  REPATRIATED("repatriated", GbifTerm.repatriated),

  //Taxonomic classification
  TAXON_KEY("gbifClassification.taxonKey", GbifTerm.taxonKey),
  USAGE_TAXON_KEY("gbifClassification.usage.key",GbifTerm.taxonKey),
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
  TAXON_ID("gbifClassification.taxonId", DwcTerm.taxonID),
  VERBATIM_SCIENTIFIC_NAME("gbifClassification.verbatimScientificName", GbifTerm.verbatimScientificName),

  //Sampling
  EVENT_ID("eventId", DwcTerm.eventID),
  PARENT_EVENT_ID("parentEventId", DwcTerm.parentEventID),
  SAMPLING_PROTOCOL("samplingProtocol", DwcTerm.samplingProtocol),
  LIFE_STAGE("lifeStage", DwcTerm.lifeStage),
  DATE_IDENTIFIED("dateIdentified", DwcTerm.dateIdentified),
  MODIFIED("modified", DcTerm.modified),
  REFERENCES("references", DcTerm.references),
  SEX("sex", DwcTerm.sex),
  IDENTIFIER("identifier", DcTerm.identifier),
  INDIVIDUAL_COUNT("individualCount", DwcTerm.individualCount),
  RELATION("relation", DcTerm.relation),
  TYPIFIED_NAME("typifiedName", GbifTerm.typifiedName),

  //Crawling
  CRAWL_ID("crawlId", GbifInternalTerm.crawlId),
  LAST_INTERPRETED("created", GbifTerm.lastInterpreted),
  LAST_CRAWLED("lastCrawled", GbifTerm.lastCrawled),
  LAST_PARSED("created", GbifTerm.lastParsed),

  MEDIA_TYPE("mediaTypes", GbifTerm.mediaType),
  MEDIA_ITEMS("multimediaItems", null),
  ISSUE("issues", GbifTerm.issue),

  ESTABLISHMENT_MEANS("establishmentMeans", DwcTerm.establishmentMeans),
  FACTS("measurementOrFactItems", null),
  GBIF_ID("gbifId", GbifTerm.gbifID),
  FULL_TEXT("all", null);



  private final String fieldName;

  private final Term term;

  OccurrenceEsField(String fieldName, Term term) {
    this.fieldName = fieldName;
    this.term = term;
  }

  /** @return the fieldName */
  public String getFieldName() {
    return fieldName;
  }

  /** @return the term */
  public Term getTerm() {
    return term;
  }
}
