package org.gbif.occurrence.common.constants;

/**
 * Each of the fields of an occurrence along with its FieldType.
 */
public enum FieldName {
  // common
  KEY(FieldType.INT),
  DATASET_KEY(FieldType.STRING),
  PUB_ORG_KEY(FieldType.STRING),
  PUB_COUNTRY_CODE(FieldType.STRING),
  INSTITUTION_CODE(FieldType.STRING),
  COLLECTION_CODE(FieldType.STRING),
  CATALOG_NUMBER(FieldType.STRING),
  IDENTIFIER_COUNT(FieldType.INT),

  // parsing fragment
  CRAWL_ID(FieldType.INT),
  LAST_CRAWLED(FieldType.LONG),
  FRAGMENT(FieldType.BYTES),
  FRAGMENT_HASH(FieldType.BYTES),
  XML_SCHEMA(FieldType.STRING),
  PROTOCOL(FieldType.STRING),
  CREATED(FieldType.LONG),

  // verbatim occurrence
  LAST_PARSED(FieldType.LONG),

  // interpreted occurrence
  LAST_INTERPRETED(FieldType.LONG),

  // taxonomy
  I_KINGDOM(FieldType.STRING),
  I_PHYLUM(FieldType.STRING),
  I_CLASS(FieldType.STRING),
  I_ORDER(FieldType.STRING),
  I_FAMILY(FieldType.STRING),
  I_GENUS(FieldType.STRING),
  I_SUBGENUS(FieldType.STRING),
  I_SPECIES(FieldType.STRING),
  I_GENERIC_NAME(FieldType.STRING),
  I_SPECIFIC_EPITHET(FieldType.STRING),
  I_INFRASPECIFIC_EPITHET(FieldType.STRING),
  I_TAXON_RANK(FieldType.STRING),
  I_SCIENTIFIC_NAME(FieldType.STRING),
  I_KINGDOM_KEY(FieldType.INT),
  I_PHYLUM_KEY(FieldType.INT),
  I_CLASS_KEY(FieldType.INT),
  I_ORDER_KEY(FieldType.INT),
  I_FAMILY_KEY(FieldType.INT),
  I_GENUS_KEY(FieldType.INT),
  I_SUBGENUS_KEY(FieldType.INT),
  I_SPECIES_KEY(FieldType.INT),
  I_TAXON_KEY(FieldType.INT),

  // date
  I_YEAR(FieldType.INT),
  I_MONTH(FieldType.INT),
  I_DAY(FieldType.INT),
  I_EVENT_DATE(FieldType.LONG),
  I_DATE_IDENTIFIED(FieldType.LONG),

  // location
  I_CONTINENT(FieldType.STRING),
  I_COUNTRY(FieldType.STRING),
  I_STATE_PROVINCE(FieldType.STRING),
  I_WATERBODY(FieldType.STRING),
  I_COORD_ACCURACY(FieldType.DOUBLE),
  I_GEODETIC_DATUM(FieldType.STRING),
  I_DECIMAL_LATITUDE(FieldType.DOUBLE),
  I_DECIMAL_LONGITUDE(FieldType.DOUBLE),

  SOURCE_MODIFIED(FieldType.LONG),
  I_ELEVATION(FieldType.INT),
  I_ELEVATION_ACC(FieldType.INT),
  I_DEPTH(FieldType.INT),
  I_DEPTH_ACC(FieldType.INT),
  I_DIST_ABOVE_SURFACE(FieldType.INT),
  I_DIST_ABOVE_SURFACE_ACC(FieldType.INT),
  I_BASIS_OF_RECORD(FieldType.STRING),
  I_MODIFIED(FieldType.LONG),
  I_ESTAB_MEANS(FieldType.STRING),
  I_INDIVIDUAL_COUNT(FieldType.INT),
  I_LIFE_STAGE(FieldType.STRING),
  I_SEX(FieldType.STRING),

  // type
  I_TYPE_STATUS(FieldType.STRING),
  I_TYPIFIED_NAME(FieldType.STRING);

  private final FieldType fieldType;

  private FieldName(FieldType fieldType) {
    this.fieldType = fieldType;
  }

  public FieldType getType() {
    return this.fieldType;
  }
}
