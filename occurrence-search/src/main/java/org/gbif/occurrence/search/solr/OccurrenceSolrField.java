package org.gbif.occurrence.search.solr;


/**
 * Enum that contains the mapping of symbolic names and field names of valid Solr fields.
 */
public enum OccurrenceSolrField {
  KEY("key"), LATITUDE("latitude"), LONGITUDE("longitude"), COORDINATE("coordinate"),
  COUNTRY("country"), PUBLISHING_COUNTRY("publishing_country"), YEAR("year"), MONTH("month"),
  CATALOG_NUMBER("catalog_number"), COLLECTOR_NAME("collector_name"),
  BASIS_OF_RECORD("basis_of_record"), DATASET_KEY("dataset_key"), TAXON_KEY("taxon_key"),
  COLLECTION_CODE("collection_code"), ALTITUDE("altitude"), DEPTH("depth"), INSTITUTION_CODE("institution_code"),
  GEOSPATIAL_ISSUE("geospatial_issue"), GEOREFERENCED("georeferenced"), DATE("date"), MODIFIED("modified");

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
