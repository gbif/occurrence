package org.gbif.occurrence.search.solr;


/**
 * Enum that contains the mapping of symbolic names and field names of valid Solr fields.
 */
public enum OccurrenceSolrField {
  KEY("key"), LATITUDE("latitude"), LONGITUDE("longitude"), COORDINATE("coordinate"),
  COUNTRY("country"), PUBLISHING_COUNTRY("publishing_country"), CONTINENT("continent"), YEAR("year"), MONTH("month"),
  CATALOG_NUMBER("catalog_number"), RECORDED_BY("recorded_by"), RECORD_NUMBER("record_number"),
  BASIS_OF_RECORD("basis_of_record"), DATASET_KEY("dataset_key"), TAXON_KEY("taxon_key"),
  COLLECTION_CODE("collection_code"), ELEVATION("elevation"), DEPTH("depth"), INSTITUTION_CODE("institution_code"),
  SPATIAL_ISSUES("spatial_issues"), HAS_COORDINATE("has_coordinate"), EVENT_DATE("event_date"),
  LAST_INTERPRETED("last_interpreted"), TYPE_STATUS("type_status");

  private final String fieldName;


  private OccurrenceSolrField(String fieldName) {
    this.fieldName = fieldName;
  }

  /**
   * @return the fieldName
   */
  public String getFieldName() {
    return fieldName;
  }

}
