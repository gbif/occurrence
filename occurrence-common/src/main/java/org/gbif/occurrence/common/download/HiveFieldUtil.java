package org.gbif.occurrence.common.download;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.constants.FieldName;

import java.util.List;
import java.util.Map;

import javax.validation.constraints.NotNull;

import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 * Translation util between FieldName enums and the hive column name and header row of the download.
 * As per http://dev.gbif.org/wiki/display/POR/Occurrence+Download+Service.
 */
public class HiveFieldUtil {

  // TODO: downloads missing all old style verbatim fields

  public static final List<FieldName> DOWNLOAD_COLUMNS = ImmutableList.of(
    FieldName.KEY,
      FieldName.DATASET_KEY,
  FieldName.INSTITUTION_CODE,
    FieldName.COLLECTION_CODE,
    FieldName.CATALOG_NUMBER,
    FieldName.I_BASIS_OF_RECORD,
    FieldName.I_SCIENTIFIC_NAME,
    // FieldName.AUTHOR,
    FieldName.I_TAXON_KEY,
    FieldName.I_KINGDOM,
    FieldName.I_PHYLUM,
    FieldName.I_CLASS,
    FieldName.I_ORDER,
    FieldName.I_FAMILY,
    FieldName.I_GENUS,
    FieldName.I_SPECIES,
    FieldName.I_KINGDOM_KEY,
    FieldName.I_PHYLUM_KEY,
    FieldName.I_CLASS_KEY,
    FieldName.I_ORDER_KEY,
    FieldName.I_FAMILY_KEY,
    FieldName.I_GENUS_KEY,
    FieldName.I_SPECIES_KEY,
    FieldName.I_COUNTRY,
    FieldName.PUB_COUNTRY_CODE,
    FieldName.I_DECIMAL_LATITUDE,
    FieldName.I_DECIMAL_LONGITUDE,
    FieldName.I_YEAR,
    FieldName.I_MONTH,
    FieldName.I_EVENT_DATE,
    FieldName.I_ELEVATION,
    FieldName.I_DEPTH,
    // FieldName.SCIENTIFIC_NAME,
// FieldName.RANK,
// FieldName.KINGDOM,
// FieldName.PHYLUM,
// FieldName.CLASS,
// FieldName.ORDER,
// FieldName.FAMILY,
// FieldName.GENUS,
// FieldName.SPECIES,
// FieldName.SUBSPECIES,
// FieldName.LATITUDE,
// FieldName.LONGITUDE,
// FieldName.LAT_LNG_PRECISION,
// FieldName.MAX_ALTITUDE,
// FieldName.MIN_ALTITUDE,
// FieldName.ALTITUDE_PRECISION,
// FieldName.MIN_DEPTH,
// FieldName.MAX_DEPTH,
// FieldName.DEPTH_PRECISION,
// FieldName.CONTINENT_OCEAN,
// FieldName.STATE_PROVINCE,
// FieldName.COUNTY,
// FieldName.COUNTRY,
// FieldName.COLLECTOR_NAME,
// FieldName.LOCALITY,
// FieldName.YEAR,
// FieldName.MONTH,
// FieldName.DAY,
// FieldName.BASIS_OF_RECORD,
// FieldName.IDENTIFIER_NAME,
// FieldName.IDENTIFICATION_DATE,
// FieldName.CREATED,
    FieldName.LAST_PARSED);

  private static final Map<FieldName, Term> NAME_MAP = ImmutableMap.<FieldName, Term>builder()
    .put(FieldName.KEY, DwcTerm.occurrenceID)
    .put(FieldName.DATASET_KEY, DwcTerm.datasetID)
    .put(FieldName.INSTITUTION_CODE, DwcTerm.institutionCode)
    .put(FieldName.COLLECTION_CODE, DwcTerm.collectionCode)
    .put(FieldName.CATALOG_NUMBER, DwcTerm.catalogNumber)
    .put(FieldName.I_BASIS_OF_RECORD, DwcTerm.basisOfRecord)
    .put(FieldName.I_SCIENTIFIC_NAME, DwcTerm.scientificName)
    .put(FieldName.I_KINGDOM, DwcTerm.kingdom)
    .put(FieldName.I_PHYLUM, DwcTerm.phylum)
    .put(FieldName.I_CLASS, DwcTerm.class_)
    .put(FieldName.I_ORDER, DwcTerm.order)
    .put(FieldName.I_FAMILY, DwcTerm.family)
    .put(FieldName.I_GENUS, DwcTerm.genus)
    .put(FieldName.I_SPECIES, DwcTerm.specificEpithet)
    // .put(FieldName.AUTHOR, DwcTerm.scientificNameAuthorship)
// .put(FieldName.RANK, DwcTerm.taxonRank)
    .put(FieldName.I_TAXON_KEY, DwcTerm.taxonID)
    .put(FieldName.I_COUNTRY, DwcTerm.countryCode)
    .put(FieldName.I_DECIMAL_LATITUDE, DwcTerm.decimalLatitude)
    .put(FieldName.I_DECIMAL_LONGITUDE, DwcTerm.decimalLongitude)
    .put(FieldName.I_YEAR, DwcTerm.year)
    .put(FieldName.I_MONTH, DwcTerm.month)
    // .put(FieldName.DAY, DwcTerm.day)
    .put(FieldName.I_EVENT_DATE, DwcTerm.eventDate)
    .put(FieldName.I_ELEVATION, GbifTerm.elevationInMeters)
    // .put(FieldName.MIN_ALTITUDE, DwcTerm.minimumElevationInMeters)
// .put(FieldName.MAX_ALTITUDE, DwcTerm.maximumElevationInMeters)
    .put(FieldName.I_DEPTH, GbifTerm.depthInMeters)
    // .put(FieldName.MIN_DEPTH, DwcTerm.minimumDepthInMeters)
// .put(FieldName.MAX_DEPTH, DwcTerm.maximumDepthInMeters)
// .put(FieldName.ALTITUDE_PRECISION, GbifTerm.elevationPrecision)
// .put(FieldName.DEPTH_PRECISION, GbifTerm.depthPrecision)
// .put(FieldName.CONTINENT_OCEAN, DwcTerm.continent)
// .put(FieldName.STATE_PROVINCE, DwcTerm.stateProvince)
// .put(FieldName.COUNTY, DwcTerm.county)
// .put(FieldName.COUNTRY, DwcTerm.country)
// .put(FieldName.COLLECTOR_NAME, DwcTerm.recordedBy)
// .put(FieldName.LOCALITY, DwcTerm.locality)
// .put(FieldName.LAT_LNG_PRECISION, DwcTerm.coordinatePrecision)
// .put(FieldName.IDENTIFIER_NAME, DwcTerm.identifiedBy)
// .put(FieldName.IDENTIFICATION_DATE, DwcTerm.dateIdentified)
    // verbatim as in source
// .put(FieldName.SCIENTIFIC_NAME, GbifTerm.verbatimScientificName)
// .put(FieldName.KINGDOM, GbifTerm.verbatimKingdom)
// .put(FieldName.PHYLUM, GbifTerm.verbatimPhylum)
// .put(FieldName.CLASS, GbifTerm.verbatimClass)
// .put(FieldName.ORDER, GbifTerm.verbatimOrder)
// .put(FieldName.FAMILY, GbifTerm.verbatimFamily)
// .put(FieldName.GENUS, GbifTerm.verbatimGenus)
// .put(FieldName.SPECIES, GbifTerm.verbatimSpecificEpithet)
// .put(FieldName.SUBSPECIES, GbifTerm.verbatimInfraspecificEpithet)
// .put(FieldName.LATITUDE, DwcTerm.verbatimLatitude)
// .put(FieldName.LONGITUDE, DwcTerm.verbatimLongitude)
// .put(FieldName.YEAR, GbifTerm.verbatimYear)
// .put(FieldName.MONTH, GbifTerm.verbatimMonth)
// .put(FieldName.BASIS_OF_RECORD, GbifTerm.verbatimBasisOfRecord)
    // internal things
// .put(FieldName.CREATED, GbifTerm.created)
    .put(FieldName.LAST_PARSED, GbifTerm.lastParsed)
    .put(FieldName.I_KINGDOM_KEY, GbifTerm.kingdomID)
    .put(FieldName.I_PHYLUM_KEY, GbifTerm.phylumID)
    .put(FieldName.I_CLASS_KEY, GbifTerm.classID)
    .put(FieldName.I_ORDER_KEY, GbifTerm.orderID)
    .put(FieldName.I_FAMILY_KEY, GbifTerm.familyID)
    .put(FieldName.I_GENUS_KEY, GbifTerm.genusID)
    .put(FieldName.I_SPECIES_KEY, GbifTerm.speciesID)
    // .put(FieldName.UNIT_QUALIFIER, GbifTerm.unitQualifier)
    .put(FieldName.PUB_COUNTRY_CODE, GbifTerm.publishingCountry)
    .build();

  /**
   * This map contains the hive names.
   */
  private static final Map<FieldName, String> HIVE_MAP = new ImmutableMap.Builder<FieldName, String>()
    .putAll(Maps.transformEntries(NAME_MAP,
      new Maps.EntryTransformer<FieldName, Term, String>() {

        public String transformEntry(@NotNull FieldName key, @NotNull Term value) {
          switch (key) {
            case KEY:
              return "id";
            case I_ORDER:
              return "order_";
            case I_CLASS:
              return "class_";
            case I_DECIMAL_LATITUDE:
              return "latitude";
            case I_DECIMAL_LONGITUDE:
              return "longitude";
// case AUTHOR:
// return "scientific_name_author";
// case CONTINENT_OCEAN:
// return "continent_ocean"; // see dev.gbif.org/issues/browse/OCC-197
            case PUB_ORG_KEY:
              return "publishing_organization";
            case PUB_COUNTRY_CODE:
              return "publishing_country";
            default:
              return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, value.simpleName())
                .replace("_i_d", "_id");
          }
        }
      })).build();

  /**
   * Private default constructor.
   */
  private HiveFieldUtil() {
    throw new UnsupportedOperationException("Can't initialize class");
  }

  /**
   * @return the column name used in the hive occurrence table.
   */
  public static String getHiveField(FieldName field) {
    return HIVE_MAP.get(field);
  }

  /**
   * @return the concept term enum for an hbase field
   */
  public static Term getTerm(FieldName field) {
    return NAME_MAP.get(field);
  }

}
