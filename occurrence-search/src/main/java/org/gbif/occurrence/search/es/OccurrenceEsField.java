package org.gbif.occurrence.search.es;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

/** Enum that contains the mapping of symbolic names and field names of valid Solr fields. */
public enum OccurrenceEsField {
  // TODO: geo point queries by coordinates
  KEY("key"),
  LATITUDE("lat"),
  LONGITUDE("lon"),
  COORDINATE(""),
  COUNTRY("country"),
  COUNTRY_CODE("countryCode"),
  PUBLISHING_COUNTRY(""),
  CONTINENT("continent"),
  YEAR("year"),
  MONTH("month"),
  DAY("day"),
  CATALOG_NUMBER(""),
  RECORDED_BY(""),
  RECORD_NUMBER(""),
  BASIS_OF_RECORD("basisOfRecord"),
  DATASET_KEY("datasetId"),
  TAXON_KEY("taxonKey"),
  KINGDOM_KEY("gbifKingdomKey"),
  PHYLUM_KEY("gbifPhylumKey"),
  CLASS_KEY("gbifClassKey"),
  ORDER_KEY("gbifOrderKey"),
  FAMILY_KEY("gbifFamilyKey"),
  GENUS_KEY("gbifGenusKey"),
  SUBGENUS_KEY("gbifSubgenusKey"),
  SPECIES_KEY("gbifSpeciesKey"),
  COLLECTION_CODE("collection_code"),
  ELEVATION("elevation"),
  DEPTH("depth"),
  INSTITUTION_CODE(""),
  SPATIAL_ISSUES(""),
  HAS_COORDINATE(""),
  EVENT_DATE("eventDate"),
  LAST_INTERPRETED(""),
  TYPE_STATUS("typeStatus"),
  MEDIA_TYPE(""),
  ISSUE("issue"),
  ESTABLISHMENT_MEANS("establishmentMeans"),
  OCCURRENCE_ID(""),
  SCIENTIFIC_NAME(""),
  FULL_TEXT(""),
  REPATRIATED("repatriated"),
  ORGANISM_ID(""),
  STATE_PROVINCE("stateProvince"),
  WATER_BODY("waterBody"),
  LOCALITY("locality"),
  PROTOCOL("protocol"),
  LICENSE("license"),
  CRAWL_ID(""),
  PUBLISHING_ORGANIZATION_KEY("publishingOrganizationKey"),
  INSTALLATION_KEY("installationKey"),
  NETWORK_KEY(""),
  EVENT_ID(""),
  PARENT_EVENT_ID(""),
  SAMPLING_PROTOCOL(""),
  COORDINATE_PRECISION("coordinatePrecision"),
  COORDINATE_UNCERTAINTY_METERS("coordinateUncertaintyInMeters"),
  DATE_IDENTIFIED("dateIdentified"),
  INDIVIDUAL_COUNT("individualCount"),
  LIFE_STAGE("lifeStage"),
  MODIFIED("modified"),
  REFERENCES("references"),
  SEX("sex");

  public static final List<OccurrenceEsField> TAXON_KEYS_LIST =
      ImmutableList.of(
          KINGDOM_KEY,
          PHYLUM_KEY,
          CLASS_KEY,
          ORDER_KEY,
          FAMILY_KEY,
          GENUS_KEY,
          SUBGENUS_KEY,
          SPECIES_KEY);

  private final String fieldName;

  OccurrenceEsField(String fieldName) {
    this.fieldName = fieldName;
  }

  /** @return the fieldName */
  public String getFieldName() {
    return fieldName;
  }
}
