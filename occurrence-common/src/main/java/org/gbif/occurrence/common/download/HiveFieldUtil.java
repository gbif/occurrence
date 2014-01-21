package org.gbif.occurrence.common.download;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.UnknownTerm;
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


  public static final List<FieldName> DOWNLOAD_COLUMNS = ImmutableList.of(
    FieldName.ID,
    FieldName.DATASET_KEY,
    FieldName.INSTITUTION_CODE,
    FieldName.COLLECTION_CODE,
    FieldName.CATALOG_NUMBER,
    FieldName.I_BASIS_OF_RECORD,
    FieldName.I_SCIENTIFIC_NAME,
    FieldName.AUTHOR,
    FieldName.I_NUB_ID,
    FieldName.I_KINGDOM,
    FieldName.I_PHYLUM,
    FieldName.I_CLASS,
    FieldName.I_ORDER,
    FieldName.I_FAMILY,
    FieldName.I_GENUS,
    FieldName.I_SPECIES,
    FieldName.I_KINGDOM_ID,
    FieldName.I_PHYLUM_ID,
    FieldName.I_CLASS_ID,
    FieldName.I_ORDER_ID,
    FieldName.I_FAMILY_ID,
    FieldName.I_GENUS_ID,
    FieldName.I_SPECIES_ID,
    FieldName.I_ISO_COUNTRY_CODE,
    FieldName.PUBLISHING_COUNTRY,
    FieldName.I_LATITUDE,
    FieldName.I_LONGITUDE,
    FieldName.I_YEAR,
    FieldName.I_MONTH,
    FieldName.I_OCCURRENCE_DATE,
    FieldName.I_ALTITUDE,
    FieldName.I_DEPTH,
    FieldName.SCIENTIFIC_NAME,
    FieldName.RANK,
    FieldName.KINGDOM,
    FieldName.PHYLUM,
    FieldName.CLASS,
    FieldName.ORDER,
    FieldName.FAMILY,
    FieldName.GENUS,
    FieldName.SPECIES,
    FieldName.SUBSPECIES,
    FieldName.LATITUDE,
    FieldName.LONGITUDE,
    FieldName.LAT_LNG_PRECISION,
    FieldName.MAX_ALTITUDE,
    FieldName.MIN_ALTITUDE,
    FieldName.ALTITUDE_PRECISION,
    FieldName.MIN_DEPTH,
    FieldName.MAX_DEPTH,
    FieldName.DEPTH_PRECISION,
    FieldName.CONTINENT_OCEAN,
    FieldName.STATE_PROVINCE,
    FieldName.COUNTY,
    FieldName.COUNTRY,
    FieldName.COLLECTOR_NAME,
    FieldName.LOCALITY,
    FieldName.YEAR,
    FieldName.MONTH,
    FieldName.DAY,
    FieldName.BASIS_OF_RECORD,
    FieldName.IDENTIFIER_NAME,
    FieldName.IDENTIFICATION_DATE,
    FieldName.CREATED,
    FieldName.MODIFIED);

  private static final Map<FieldName, Term> NAME_MAP = ImmutableMap.<FieldName, Term>builder()
    .put(FieldName.ID, DwcTerm.occurrenceID)
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
    .put(FieldName.AUTHOR, DwcTerm.scientificNameAuthorship)
    .put(FieldName.RANK, DwcTerm.taxonRank)
    .put(FieldName.I_NUB_ID, DwcTerm.taxonID)
    .put(FieldName.I_ISO_COUNTRY_CODE, DwcTerm.countryCode)
    .put(FieldName.I_LATITUDE, DwcTerm.decimalLatitude)
    .put(FieldName.I_LONGITUDE, DwcTerm.decimalLongitude)
    .put(FieldName.I_YEAR, DwcTerm.year)
    .put(FieldName.I_MONTH, DwcTerm.month)
    .put(FieldName.DAY, DwcTerm.day)
    .put(FieldName.I_OCCURRENCE_DATE, DwcTerm.eventDate)
    .put(FieldName.I_ALTITUDE, GbifTerm.elevationInMeters)
    .put(FieldName.MIN_ALTITUDE, DwcTerm.minimumElevationInMeters)
    .put(FieldName.MAX_ALTITUDE, DwcTerm.maximumElevationInMeters)
    .put(FieldName.I_DEPTH, GbifTerm.depthInMeters)
    .put(FieldName.MIN_DEPTH, DwcTerm.minimumDepthInMeters)
    .put(FieldName.MAX_DEPTH, DwcTerm.maximumDepthInMeters)
    .put(FieldName.ALTITUDE_PRECISION, GbifTerm.elevationPrecision)
    .put(FieldName.DEPTH_PRECISION, GbifTerm.depthPrecision)
    .put(FieldName.CONTINENT_OCEAN, DwcTerm.continent)
    .put(FieldName.STATE_PROVINCE, DwcTerm.stateProvince)
    .put(FieldName.COUNTY, DwcTerm.county)
    .put(FieldName.COUNTRY, DwcTerm.country)
    .put(FieldName.COLLECTOR_NAME, DwcTerm.recordedBy)
    .put(FieldName.LOCALITY, DwcTerm.locality)
    .put(FieldName.LAT_LNG_PRECISION, DwcTerm.coordinatePrecision)
    .put(FieldName.IDENTIFIER_NAME, DwcTerm.identifiedBy)
    .put(FieldName.IDENTIFICATION_DATE, DwcTerm.dateIdentified)
    // verbatim as in source
    .put(FieldName.SCIENTIFIC_NAME, GbifTerm.verbatimScientificName)
    .put(FieldName.KINGDOM, GbifTerm.verbatimKingdom)
    .put(FieldName.PHYLUM, GbifTerm.verbatimPhylum)
    .put(FieldName.CLASS, GbifTerm.verbatimClass)
    .put(FieldName.ORDER, GbifTerm.verbatimOrder)
    .put(FieldName.FAMILY, GbifTerm.verbatimFamily)
    .put(FieldName.GENUS, GbifTerm.verbatimGenus)
    .put(FieldName.SPECIES, GbifTerm.verbatimSpecificEpithet)
    .put(FieldName.SUBSPECIES, GbifTerm.verbatimInfraspecificEpithet)
    .put(FieldName.LATITUDE, DwcTerm.verbatimLatitude)
    .put(FieldName.LONGITUDE, DwcTerm.verbatimLongitude)
    .put(FieldName.YEAR, GbifTerm.verbatimYear)
    .put(FieldName.MONTH, GbifTerm.verbatimMonth)
    .put(FieldName.BASIS_OF_RECORD, GbifTerm.verbatimBasisOfRecord)
    // internal things
    .put(FieldName.CREATED, GbifTerm.created)
    .put(FieldName.MODIFIED, GbifTerm.modified)
    .put(FieldName.I_KINGDOM_ID, GbifTerm.kingdomID)
    .put(FieldName.I_PHYLUM_ID, GbifTerm.phylumID)
    .put(FieldName.I_CLASS_ID, GbifTerm.classID)
    .put(FieldName.I_ORDER_ID, GbifTerm.orderID)
    .put(FieldName.I_FAMILY_ID, GbifTerm.familyID)
    .put(FieldName.I_GENUS_ID, GbifTerm.genusID)
    .put(FieldName.I_SPECIES_ID, GbifTerm.speciesID)
    .put(FieldName.I_CELL_ID, GbifTerm.cellID)
    .put(FieldName.I_CENTI_CELL_ID, GbifTerm.centiCellID)
    .put(FieldName.I_MOD360_CELL_ID, GbifTerm.mod360CellID)
    .put(FieldName.I_TAXONOMIC_ISSUE, GbifTerm.taxonomicIssueFlag)
    .put(FieldName.I_GEOSPATIAL_ISSUE, GbifTerm.geospatialIssueFlag)
    .put(FieldName.I_OTHER_ISSUE, GbifTerm.otherIssueFlag)
    .put(FieldName.UNIT_QUALIFIER, GbifTerm.unitQualifier)
    .put(FieldName.PUBLISHING_COUNTRY, new UnknownTerm("http://rs.gbif.org/terms/1.0/publishingCountry", "publishingCountry"))
    .build();

  /**
   * This map contains the hive names.
   */
  private static final Map<FieldName, String> HIVE_MAP = new ImmutableMap.Builder<FieldName, String>()
    .putAll(Maps.transformEntries(NAME_MAP,
      new Maps.EntryTransformer<FieldName, Term, String>() {

        public String transformEntry(@NotNull FieldName key, @NotNull Term value) {
          switch (key) {
            case ID:
              return "id";
            case I_ORDER:
              return "order_";
            case I_CLASS:
              return "class_";
            case I_LATITUDE:
              return "latitude";
            case I_LONGITUDE:
              return "longitude";
            case AUTHOR:
              return "scientific_name_author";
            case CONTINENT_OCEAN:
              return "continent_ocean"; // see dev.gbif.org/issues/browse/OCC-197
            case OWNING_ORG_KEY:
              return "owning_organization";
            case I_GEOSPATIAL_ISSUE:
              return "geospatial_issue";
            case PUBLISHING_COUNTRY:
              return "host_country";
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
