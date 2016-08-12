package org.gbif.occurrence.search.solr;

import org.gbif.api.vocabulary.EndpointType;

/**
 * Enum that contains the mapping of symbolic names and field names of valid Solr fields.
 */
public enum OccurrenceSolrField {
  KEY("key"), LATITUDE("latitude"), LONGITUDE("longitude"), COORDINATE("coordinate"),
  COUNTRY("country"), PUBLISHING_COUNTRY("publishing_country"), CONTINENT("continent"), YEAR("year"), MONTH("month"),
  CATALOG_NUMBER("catalog_number"), RECORDED_BY("recorded_by"), RECORD_NUMBER("record_number"),
  BASIS_OF_RECORD("basis_of_record"), DATASET_KEY("dataset_key"), TAXON_KEY("taxon_key"),
  KINGDOM_KEY("kingdom_key"), PHYLUM_KEY("phylum_key"), CLASS_KEY("class_key"), ORDER_KEY("order_key"),
  FAMILY_KEY("family_key"), GENUS_KEY("genus_key"), SUBGENUS_KEY("subgenus_key"), SPECIES_KEY("species_key"),
  COLLECTION_CODE("collection_code"), ELEVATION("elevation"), DEPTH("depth"), INSTITUTION_CODE("institution_code"),
  SPATIAL_ISSUES("spatial_issues"), HAS_COORDINATE("has_coordinate"), EVENT_DATE("event_date"),
  LAST_INTERPRETED("last_interpreted"), TYPE_STATUS("type_status"), MEDIA_TYPE("media_type"), ISSUE("issue"),
  ESTABLISHMENT_MEANS("establishment_means"), OCCURRENCE_ID("occurrence_id"),
  SCIENTIFIC_NAME("scientific_name"), FULL_TEXT("full_text"), REPATRIATED("repatriated"),
  ORGANISM_ID("organism_id"), STATE_PROVINCE("state_province"), WATER_BODY("water_body"), LOCALITY("locality"),
  PROTOCOL("protocol"), DATASET_LICENSE("dataset_license");

  private final String fieldName;


  OccurrenceSolrField(String fieldName) {
    this.fieldName = fieldName;
  }

  /**
   * @return the fieldName
   */
  public String getFieldName() {
    return fieldName;
  }

}
