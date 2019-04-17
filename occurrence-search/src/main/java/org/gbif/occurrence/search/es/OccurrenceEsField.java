package org.gbif.occurrence.search.es;

/**
 * <field name="key" type="int" indexed="true" stored="true" required="true"docValues="true"/>
 * <field name="dataset_key" type="string" indexed="true" stored="false" docValues="true"/> <field
 * name="installation_key" type="string" indexed="true" stored="false" docValues="true"/> <field
 * name="network_key" type="string" indexed="true" stored="false" multiValued="true"
 * docValues="true"/> <field name="taxon_key" type="int" indexed="true" stored="false"
 * multiValued="true" docValues="true"/> <field name="accepted_taxon_key" type="int" indexed="true"
 * stored="false" docValues="true"/> <field name="kingdom_key" type="int" indexed="true"
 * stored="false" docValues="true"/> <field name="phylum_key" type="int" indexed="true"
 * stored="false" docValues="true"/> <field name="class_key" type="int" indexed="true"
 * stored="false" docValues="true"/> <field name="order_key" type="int" indexed="true"
 * stored="false" docValues="true"/> <field name="family_key" type="int" indexed="true"
 * stored="false" docValues="true"/> <field name="genus_key" type="int" indexed="true"
 * stored="false" docValues="true"/> <field name="subgenus_key" type="int" indexed="true"
 * stored="false" docValues="true"/> <field name="species_key" type="int" indexed="true"
 * stored="false" docValues="true"/> <field name="taxonomic_status" type="string" indexed="true"
 * stored="false" docValues="true"/> <field name="basis_of_record" type="string" indexed="true"
 * stored="false" docValues="true"/> <field name="year" type="int" indexed="true" stored="false"
 * docValues="true"/> <field name="month" type="int" indexed="true" stored="false"
 * docValues="true"/> <field name="elevation" type="double" indexed="true" stored="false"
 * docValues="true"/> <field name="depth" type="double" indexed="true" stored="false"
 * docValues="true"/> <field name="catalog_number" type="string_ci" indexed="true" stored="false" />
 * <field name="recorded_by" type="string_ci" indexed="true" stored="false" /> <field
 * name="record_number" type="string_ci" indexed="true" stored="false" /> <field
 * name="institution_code" type="string_ci" indexed="true" stored="false" /> <field
 * name="collection_code" type="string_ci" indexed="true" stored="false" /> <field name="country"
 * type="string" indexed="true" stored="false" docValues="true"/> <field name="continent"
 * type="string" indexed="true" stored="false" docValues="true"/> <field name="publishing_country"
 * type="string" indexed="true" stored="false" docValues="true"/> <field name="coordinate"
 * type="coordinate" indexed="true" stored="false" /> <field name="spatial_issues" type="boolean"
 * indexed="true" stored="false" /> <field name="has_coordinate" type="boolean" indexed="true"
 * stored="false" /> <field name="latitude" type="double" indexed="true" stored="false" /> <field
 * name="longitude" type="double" indexed="true" stored="false" /> <field name="event_date"
 * type="date" indexed="true" stored="false" /> <field name="last_interpreted" type="date"
 * indexed="true" stored="false" /> <field name="type_status" type="string" indexed="true"
 * stored="false" docValues="true"/> <field name="media_type" type="string" indexed="true"
 * stored="false" multiValued="true" docValues="true"/> <field name="issue" type="string"
 * indexed="true" stored="false" multiValued="true" docValues="true"/> <field
 * name="establishment_means" type="string" indexed="true" stored="false" docValues="true"/> <field
 * name="occurrence_id" type="string" indexed="true" stored="false"/> <field name="scientific_name"
 * type="string_ci" indexed="true" stored="false" /> <field name="organism_id" type="string_ci"
 * indexed="true" stored="false"/> <field name="state_province" type="string_ci" indexed="true"
 * stored="false"/> <field name="water_body" type="string_ci" indexed="true" stored="false"/> <field
 * name="locality" type="string_ci" indexed="true" stored="false"/> <field name="protocol"
 * type="string" indexed="true" stored="false" docValues="true"/> <field name="license"
 * type="string" indexed="true" stored="false" docValues="true"/> <field name="crawl_id" type="int"
 * indexed="true" stored="false" docValues="true"/> <field name="publishing_organization_key"
 * type="string" indexed="true" stored="false" docValues="true"/> <field name="repatriated"
 * type="boolean" indexed="true" stored="false" /> <field name="event_id" type="string"
 * indexed="true" stored="false" docValues="true"/> <field name="parent_event_id" type="string"
 * indexed="true" stored="false" docValues="true"/> <field name="sampling_protocol" type="string"
 * indexed="true" stored="false" docValues="true"/> <field name="full_text" type="text_general"
 * indexed="true" stored="false" multiValued="true"/>
 *
 * <p>Supported fields: // TODO: update fields
 * KEY("key"), LATITUDE("latitude"), LONGITUDE("longitude"),
 * COORDINATE("coordinate"), COUNTRY("country"), PUBLISHING_COUNTRY("publishing_country"),
 * CONTINENT("continent"), YEAR("year"), MONTH("month"), CATALOG_NUMBER("catalog_number"),
 * RECORDED_BY("recorded_by"), RECORD_NUMBER("record_number"), BASIS_OF_RECORD("basis_of_record"),
 * DATASET_KEY("dataset_key"), TAXON_KEY("taxon_key"), ACCEPTED_TAXON_KEY("accepted_taxon_key"),
 * KINGDOM_KEY("kingdom_key"), PHYLUM_KEY("phylum_key"), CLASS_KEY("class_key"),
 * ORDER_KEY("order_key"), FAMILY_KEY("family_key"), GENUS_KEY("genus_key"),
 * SUBGENUS_KEY("subgenus_key"), SPECIES_KEY("species_key"), TAXONOMIC_STATUS("taxonomic_status"),
 * COLLECTION_CODE("collection_code"), ELEVATION("elevation"), DEPTH("depth"),
 * INSTITUTION_CODE("institution_code"), HAS_GEOSPATIAL_ISSUES("spatial_issues"),
 * HAS_COORDINATE("has_coordinate"), EVENT_DATE("event_date"), LAST_INTERPRETED("last_interpreted"),
 * TYPE_STATUS("type_status"), MEDIA_TYPE("media_type"), ISSUE("issue"),
 * ESTABLISHMENT_MEANS("establishment_means"), OCCURRENCE_ID("occurrence_id"),
 * SCIENTIFIC_NAME("scientific_name"), FULL_TEXT("full_text"), REPATRIATED("repatriated"),
 * ORGANISM_ID("organism_id"), STATE_PROVINCE("state_province"), WATER_BODY("water_body"),
 * LOCALITY("locality"), PROTOCOL("protocol"), LICENSE("license"), CRAWL_ID("crawl_id"),
 * PUBLISHING_ORGANIZATION_KEY("publishing_organization_key"), INSTALLATION_KEY("installation_key"),
 * NETWORK_KEY("network_key"), EVENT_ID("event_id"), PARENT_EVENT_ID("parent_event_id"),
 * SAMPLING_PROTOCOL("sampling_protocol");
 */

/** Enum that contains the mapping of symbolic names and field names of valid Solr fields. */
public enum OccurrenceEsField {

  KEY("id"),

  //Dataset derived
  DATASET_KEY("datasetKey"),
  PUBLISHING_COUNTRY("publishingCountry"),
  PUBLISHING_ORGANIZATION_KEY("publishingOrganizationKey"),
  INSTALLATION_KEY("installationKey"),
  NETWORK_KEY("networkKeys"),
  PROTOCOL("protocol"),
  LICENSE("license"),

  //Core identification
  INSTITUTION_CODE("institutionCode"),
  COLLECTION_CODE("collectionCode"),
  CATALOG_NUMBER("catalogNumber"),

  ORGANISM_ID("organismId"),
  OCCURRENCE_ID("occurrenceID"),
  RECORDED_BY("recordBy"),
  RECORD_NUMBER("recordNumber"),
  BASIS_OF_RECORD("basisOfRecord"),
  TYPE_STATUS("typeStatus"),

  //Temporal
  YEAR("year"),
  MONTH("month"),
  DAY("day"),
  EVENT_DATE("eventDateSingle"),

  //Location
  COORDINATE_SHAPE("scoordinates"),
  COORDINATE_POINT("coordinates"),
  LATITUDE("coordinates.lat"),
  LONGITUDE("coordinates.lon"),
  COUNTRY_CODE("countryCode"),
  CONTINENT("continent"),
  COORDINATE_ACCURACY("coordinateAccuracy"),
  ELEVATION_ACCURACY("elevationAccuracy"),
  DEPTH_ACCURACY("depthAccuracy"),
  ELEVATION("elevation"),
  DEPTH("depth"),
  STATE_PROVINCE("stateProvince"), //NOT INTERPRETED
  WATER_BODY("waterBody"),
  LOCALITY("locality"),
  COORDINATE_PRECISION("coordinatePrecision"),
  COORDINATE_UNCERTAINTY_METERS("coordinateUncertaintyInMeters"),

  //Location GBIF specific
  HAS_GEOSPATIAL_ISSUES("hasGeospatialIssue"),
  HAS_COORDINATE("hasCoordinate"),
  REPATRIATED("repatriated"),

  //Taxonomic classification
  TAXON_KEY("gbifClassification.taxonKey"),
  USAGE_TAXON_KEY("gbifClassification.usage.key"),
  TAXON_RANK("gbifClassification.usage.rank"),
  ACCEPTED_TAXON_KEY("gbifClassification.acceptedUsage.key"),
  ACCEPTED_SCIENTIFIC_NAME("gbifClassification.acceptedUsage.name"),
  KINGDOM_KEY("gbifClassification.kingdomKey"),
  KINGDOM("gbifClassification.kingdom"),
  PHYLUM_KEY("gbifClassification.phylumkey"),
  PHYLUM("gbifClassification.phylum"),
  CLASS_KEY("gbifClassification.classkey"),
  CLASS("gbifClassification.class"),
  ORDER_KEY("gbifClassification.orderkey"),
  ORDER("gbifClassification.order"),
  FAMILY_KEY("gbifClassification.familykey"),
  FAMILY("gbifClassification.family"),
  GENUS_KEY("gbifClassification.genusKey"),
  GENUS("gbifClassification.genus"),
  SUBGENUS_KEY("gbifClassification.subgenusKey"),
  SUBGENUS("gbifClassification.subgenus"),
  SPECIES_KEY("gbifClassification.speciesKey"),
  SPECIES("gbifClassification.species"),
  SCIENTIFIC_NAME("gbifClassification.usage.name"),
  SPECIFIC_EPITHET("gbifClassification.usageParsedName.specificEpithet"),
  INFRA_SPECIFIC_EPITHET("gbifClassification.usageParsedName.infraSpecificEpithet"),
  GENERIC_NAME("gbifClassification.usageParsedName.genericName"),
  TAXONOMIC_STATUS("gbifClassification.diagnostics.sttaus"),

  //Sampling
  EVENT_ID("eventId"),
  PARENT_EVENT_ID("parentEventId"),
  SAMPLING_PROTOCOL("samplingProtocol"),
  LIFE_STAGE("lifeStage"),
  DATE_IDENTIFIED("dateIdentified"),
  MODIFIED("modified"),
  REFERENCES("references"),
  SEX("sex"),
  IDENTIFIER("identifier"),
  INDIVIDUAL_COUNT("individualCount"),
  RELATION("relation"),
  TYPIFIED_NAME("typifiedName"),

  //Crawling
  CRAWL_ID("crawlId"),
  LAST_INTERPRETED("created"),
  LAST_CRAWLED("lastCrawled"),
  LAST_PARSED("created"),

  MEDIA_TYPE("multimediaItems.type"),
  MEDIA_ITEMS("multimediaItems"),
  ISSUE("issues"),

  ESTABLISHMENT_MEANS("establishmentMeans"),
  FACTS("measurementOrFactItems"),
  GBIF_ID("gbifId"),
  FULL_TEXT("all");



  private final String fieldName;

  OccurrenceEsField(String fieldName) {
    this.fieldName = fieldName;
  }

  /** @return the fieldName */
  public String getFieldName() {
    return fieldName;
  }
}
