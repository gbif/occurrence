package org.gbif.occurrencestore.common.model.constants;

/**
 * Each of the fields of an occurrence along with its FieldType.
 */
public enum FieldName {
  // common
  ID(FieldType.INT),
  DATASET_KEY(FieldType.STRING),
  OWNING_ORG_KEY(FieldType.STRING),
  DATA_PROVIDER_ID(FieldType.INT),
  DATA_RESOURCE_ID(FieldType.INT),
  RESOURCE_ACCESS_POINT_ID(FieldType.INT),
  INSTITUTION_CODE(FieldType.STRING),
  COLLECTION_CODE(FieldType.STRING),
  CATALOG_NUMBER(FieldType.STRING),
  IDENTIFIER_COUNT(FieldType.INT),

  // parsing fragment
  DWC_OCCURRENCE_ID(FieldType.STRING),
  CRAWL_ID(FieldType.INT),
  HARVESTED_DATE(FieldType.LONG),
  FRAGMENT(FieldType.BYTES),
  FRAGMENT_HASH(FieldType.BYTES),
  XML_SCHEMA(FieldType.STRING),
  UNIT_QUALIFIER(FieldType.STRING),
  PROTOCOL(FieldType.STRING),

  // verbatim occurrence
  SCIENTIFIC_NAME(FieldType.STRING),
  AUTHOR(FieldType.STRING),
  RANK(FieldType.STRING),
  KINGDOM(FieldType.STRING),
  PHYLUM(FieldType.STRING),
  CLASS(FieldType.STRING),
  ORDER(FieldType.STRING),
  FAMILY(FieldType.STRING),
  GENUS(FieldType.STRING),
  SPECIES(FieldType.STRING),
  SUBSPECIES(FieldType.STRING),
  LATITUDE(FieldType.STRING),
  LONGITUDE(FieldType.STRING),
  LAT_LNG_PRECISION(FieldType.STRING),
  MAX_ALTITUDE(FieldType.STRING),
  MIN_ALTITUDE(FieldType.STRING),
  ALTITUDE_PRECISION(FieldType.STRING),
  MIN_DEPTH(FieldType.STRING),
  MAX_DEPTH(FieldType.STRING),
  DEPTH_PRECISION(FieldType.STRING),
  CONTINENT_OCEAN(FieldType.STRING),
  STATE_PROVINCE(FieldType.STRING),
  COUNTY(FieldType.STRING),
  COUNTRY(FieldType.STRING),
  COLLECTOR_NAME(FieldType.STRING),
  LOCALITY(FieldType.STRING),
  YEAR(FieldType.STRING),
  MONTH(FieldType.STRING),
  DAY(FieldType.STRING),
  OCCURRENCE_DATE(FieldType.STRING),
  BASIS_OF_RECORD(FieldType.STRING),
  IDENTIFIER_NAME(FieldType.STRING),
  IDENTIFICATION_DATE(FieldType.LONG),
  CREATED(FieldType.LONG),
  MODIFIED(FieldType.LONG),

  // interpreted occurrence
  I_KINGDOM(FieldType.STRING),
  I_PHYLUM(FieldType.STRING),
  I_CLASS(FieldType.STRING),
  I_ORDER(FieldType.STRING),
  I_FAMILY(FieldType.STRING),
  I_GENUS(FieldType.STRING),
  I_SPECIES(FieldType.STRING),
  I_SCIENTIFIC_NAME(FieldType.STRING),
  I_KINGDOM_ID(FieldType.INT),
  I_PHYLUM_ID(FieldType.INT),
  I_CLASS_ID(FieldType.INT),
  I_ORDER_ID(FieldType.INT),
  I_FAMILY_ID(FieldType.INT),
  I_GENUS_ID(FieldType.INT),
  I_SPECIES_ID(FieldType.INT),
  I_NUB_ID(FieldType.INT),
  I_ISO_COUNTRY_CODE(FieldType.STRING),
  I_LATITUDE(FieldType.DOUBLE),
  I_LONGITUDE(FieldType.DOUBLE),
  I_CELL_ID(FieldType.INT),
  I_CENTI_CELL_ID(FieldType.INT),
  I_MOD360_CELL_ID(FieldType.INT),
  I_YEAR(FieldType.INT),
  I_MONTH(FieldType.INT),
  I_OCCURRENCE_DATE(FieldType.LONG),
  I_BASIS_OF_RECORD(FieldType.INT),
  I_TAXONOMIC_ISSUE(FieldType.INT),
  I_GEOSPATIAL_ISSUE(FieldType.INT),
  I_OTHER_ISSUE(FieldType.INT),
  I_ALTITUDE(FieldType.INT),
  I_DEPTH(FieldType.INT),
  I_MODIFIED(FieldType.LONG),
  HOST_COUNTRY(FieldType.STRING);

  private final FieldType fieldType;

  private FieldName(FieldType fieldType) {
    this.fieldType = fieldType;
  }

  public FieldType getType() {
    return this.fieldType;
  }
}
