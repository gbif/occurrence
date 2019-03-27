package org.gbif.occurrence.search.es;

/**
 * <field name="key" type="int" indexed="true" stored="true" required="true"docValues="true"/>
 * <field name="dataset_key" type="string" indexed="true" stored="false" docValues="true"/>
 * <field name="installation_key" type="string" indexed="true" stored="false" docValues="true"/>
 * <field name="network_key" type="string" indexed="true" stored="false" multiValued="true" docValues="true"/>
 * <field name="taxon_key" type="int" indexed="true" stored="false" multiValued="true" docValues="true"/>
 * <field name="accepted_taxon_key" type="int" indexed="true" stored="false" docValues="true"/>
 * <field name="kingdom_key" type="int" indexed="true" stored="false" docValues="true"/>
 * <field name="phylum_key" type="int" indexed="true" stored="false" docValues="true"/>
 * <field name="class_key" type="int" indexed="true" stored="false" docValues="true"/>
 * <field name="order_key" type="int" indexed="true" stored="false" docValues="true"/>
 * <field name="family_key" type="int" indexed="true" stored="false" docValues="true"/>
 * <field name="genus_key" type="int" indexed="true" stored="false" docValues="true"/>
 * <field name="subgenus_key" type="int" indexed="true" stored="false" docValues="true"/>
 * <field name="species_key" type="int" indexed="true" stored="false" docValues="true"/>
 * <field name="taxonomic_status" type="string" indexed="true" stored="false" docValues="true"/>
 * <field name="basis_of_record" type="string" indexed="true" stored="false" docValues="true"/>
 * <field name="year" type="int" indexed="true" stored="false" docValues="true"/>
 * <field name="month" type="int" indexed="true" stored="false" docValues="true"/>
 * <field name="elevation" type="double" indexed="true" stored="false" docValues="true"/>
 * <field name="depth" type="double" indexed="true" stored="false" docValues="true"/>
 * <field name="catalog_number" type="string_ci" indexed="true" stored="false" />
 * <field name="recorded_by" type="string_ci" indexed="true" stored="false" />
 * <field name="record_number" type="string_ci" indexed="true" stored="false" />
 * <field name="institution_code" type="string_ci" indexed="true" stored="false" />
 * <field name="collection_code" type="string_ci" indexed="true" stored="false" />
 * <field name="country" type="string" indexed="true" stored="false" docValues="true"/>
 * <field name="continent" type="string" indexed="true" stored="false" docValues="true"/>
 * <field name="publishing_country" type="string" indexed="true" stored="false" docValues="true"/>
 * <field name="coordinate" type="coordinate" indexed="true" stored="false" />
 * <field name="spatial_issues" type="boolean" indexed="true" stored="false" />
 * <field name="has_coordinate" type="boolean" indexed="true" stored="false" />
 * <field name="latitude" type="double" indexed="true" stored="false" />
 * <field name="longitude" type="double" indexed="true" stored="false" />
 * <field name="event_date" type="date" indexed="true" stored="false" />
 * <field name="last_interpreted" type="date" indexed="true" stored="false" />
 * <field name="type_status" type="string" indexed="true" stored="false" docValues="true"/>
 * <field name="media_type" type="string" indexed="true" stored="false" multiValued="true" docValues="true"/>
 * <field name="issue" type="string" indexed="true" stored="false" multiValued="true" docValues="true"/>
 * <field name="establishment_means" type="string" indexed="true" stored="false" docValues="true"/>
 * <field name="occurrence_id" type="string" indexed="true" stored="false"/>
 * <field name="scientific_name" type="string_ci" indexed="true" stored="false" />
 * <field name="organism_id" type="string_ci" indexed="true" stored="false"/>
 * <field name="state_province" type="string_ci" indexed="true" stored="false"/>
 * <field name="water_body" type="string_ci" indexed="true" stored="false"/>
 * <field name="locality" type="string_ci" indexed="true" stored="false"/>
 * <field name="protocol" type="string" indexed="true" stored="false" docValues="true"/>
 * <field name="license" type="string" indexed="true" stored="false" docValues="true"/>
 * <field name="crawl_id" type="int" indexed="true" stored="false" docValues="true"/>
 * <field name="publishing_organization_key" type="string" indexed="true" stored="false" docValues="true"/>
 * <field name="repatriated" type="boolean" indexed="true" stored="false" />
 * <field name="event_id" type="string" indexed="true" stored="false" docValues="true"/>
 * <field name="parent_event_id" type="string" indexed="true" stored="false" docValues="true"/>
 * <field name="sampling_protocol" type="string" indexed="true" stored="false" docValues="true"/>
 * <field name="full_text" type="text_general" indexed="true" stored="false" multiValued="true"/>
 */

/** Enum that contains the mapping of symbolic names and field names of valid Solr fields. */
public enum OccurrenceEsField {
  KEY("id"),
  LATITUDE("coordinate.lat"),
  LONGITUDE("coordinate.lng"),
  COUNTRY("country"),
  COUNTRY_CODE("countryCode"),
  PUBLISHING_COUNTRY("publishingCountry"),
  CONTINENT("continent"),
  YEAR("year"),
  MONTH("month"),
  DAY("day"),
  CATALOG_NUMBER("catalogNumber"),
  RECORDED_BY("recordBy"),
  RECORD_NUMBER("recordnumber"),
  BASIS_OF_RECORD("basisOfRecord"),
  DATASET_KEY("datasetKey"),
  TAXON_KEY("gbifTaxonKey"),
  TAXON_RANK("gbifTaxonRank"),
  KINGDOM_KEY("kingdomkey"),
  KINGDOM("kingdom"),
  PHYLUM_KEY("phylumkey"),
  PHYLUM("phylum"),
  CLASS_KEY("classkey"),
  CLASS("class"),
  ORDER_KEY("orderkey"),
  ORDER("order"),
  FAMILY_KEY("familykey"),
  FAMILY("family"),
  GENUS_KEY("gbifGenusKey"),
  GENUS("genus"),
  SUBGENUS_KEY("subgenuskey"),
  SUBGENUS("subgenus"),
  SPECIES_KEY("specieskey"),
  SPECIES("species"),
  SCIENTIFIC_NAME("gbifScientificName"),
  SPECIFIC_EPITHET("specificepithet"),
  INFRA_SPECIFIC_EPITHET("infraspecificepithet"),
  GENERIC_NAME("genericname"),
  COLLECTION_CODE("collectioncode"),
  //TODO: es use minimumElevationInMeters and maximumElevationInMeters
  ELEVATION("elevation"),
  DEPTH("depth"),
  INSTITUTION_CODE("institutioncode"),
  SPATIAL_ISSUES("hasgeospatialissues"),
  HAS_COORDINATE("hascoordinate"),
  EVENT_DATE("eventdate"),
  LAST_INTERPRETED("lastinterpreted"),
  LAST_CRAWLED("lastcrawled"),
  LAST_PARSED("lastparsed"),
  TYPE_STATUS("typeStatus"),
  MEDIA_TYPE("mediaType"),
  ISSUE("issue"),
  ESTABLISHMENT_MEANS("establishmentMeans"),
  OCCURRENCE_ID("occurrenceID"),
  FULL_TEXT(""),
  REPATRIATED("repatriated"),
  ORGANISM_ID("organismid"),
  STATE_PROVINCE("stateprovince"),
  WATER_BODY("waterbody"),
  LOCALITY("locality"),
  PROTOCOL("protocol"),
  LICENSE("license"),
  CRAWL_ID("crawlid"),
  PUBLISHING_ORGANIZATION_KEY("publishingOrganizationKey"),
  INSTALLATION_KEY("installationKey"),
  NETWORK_KEY("networkKeys"),
  EVENT_ID("eventId"),
  PARENT_EVENT_ID("parentEventId"),
  SAMPLING_PROTOCOL("samplingProtocol"),
  COORDINATE_PRECISION("coordinatePrecision"),
  COORDINATE_UNCERTAINTY_METERS("coordinateUncertaintyInMeters"),
  DATE_IDENTIFIED("dateIdentified"),
  INDIVIDUAL_COUNT("individualCount"),
  LIFE_STAGE("lifeStage"),
  MODIFIED("modified"),
  REFERENCES("references"),
  SEX("sex"),
  COORDINATE_ACCURACY("coordinateAccuracy"),
  ELEVATION_ACCURACY("elevationAccuracy"),
  DEPTH_ACCURACY("depthAccuracy"),
  IDENTIFIER("identifier"),
  RELATION("relation"),
  TYPIFIED_NAME("typifiedName"),
  COORDINATE_SHAPE("coordinate_shape"),
  COORDINATE_POINT("coordinate"),
  GBIF_ID("id");

  private final String fieldName;

  OccurrenceEsField(String fieldName) {
    this.fieldName = fieldName;
  }

  /** @return the fieldName */
  public String getFieldName() {
    return fieldName;
  }
}
