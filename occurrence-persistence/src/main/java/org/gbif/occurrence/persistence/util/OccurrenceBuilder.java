package org.gbif.occurrence.persistence.util;

import org.gbif.api.model.common.Identifier;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.IdentifierType;
import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.hbase.util.ResultReader;
import org.gbif.occurrence.common.constants.FieldName;
import org.gbif.occurrence.common.converter.BasisOfRecordConverter;
import org.gbif.occurrence.persistence.OccurrenceResultReader;
import org.gbif.occurrence.persistence.api.Fragment;
import org.gbif.occurrence.persistence.api.VerbatimOccurrence;
import org.gbif.occurrence.persistence.constants.HBaseTableConstants;

import java.util.Date;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;
import javax.validation.ValidationException;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class to build object models from the HBase occurrence "row".
 */
public class OccurrenceBuilder {

  private static final BasisOfRecordConverter BOR_CONVERTER = new BasisOfRecordConverter();
  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceBuilder.class);

  // should never be instantiated
  private OccurrenceBuilder() {
  }

  /**
   * Builds a Fragment object from the given result, assigning the passed in key.
   *
   * @param result an HBase scan/get Result
   * @return the Fragment or null if the passed in Result is null
   * @throws ValidationException if the fragment as stored in the table is invalid
   */
  public static Fragment buildFragment(@Nullable Result result) {
    if (result == null) {
      return null;
    }

    int key = Bytes.toInt(result.getRow());

    String rawDatasetKey = OccurrenceResultReader.getString(result, FieldName.DATASET_KEY);
    if (rawDatasetKey == null) {
      throw new ValidationException("Fragment with key [" + key + "] has no datasetKey.");
    }
    UUID datasetKey = UUID.fromString(rawDatasetKey);

    Integer crawlId = OccurrenceResultReader.getInteger(result, FieldName.CRAWL_ID);
    if (crawlId == null) {
      throw new ValidationException("Fragment with key [" + key + "] has no crawlId.");
    }
    Long harvested = OccurrenceResultReader.getLong(result, FieldName.HARVESTED_DATE);
    if (harvested == null) {
      throw new ValidationException("Fragment with key [" + key + "] has no harvestedDate.");
    }
    Date harvestedDate = new Date(harvested);
    String unitQualifier = OccurrenceResultReader.getString(result, FieldName.UNIT_QUALIFIER);
    byte[] data = OccurrenceResultReader.getBytes(result, FieldName.FRAGMENT);
    byte[] dataHash = OccurrenceResultReader.getBytes(result, FieldName.FRAGMENT_HASH);
    Long created = OccurrenceResultReader.getLong(result, FieldName.CREATED);
    String rawSchema = OccurrenceResultReader.getString(result, FieldName.XML_SCHEMA);
    OccurrenceSchemaType schema;
    if (rawSchema == null) {
      // this is typically called just before updating the fragment, meaning schemaType will then be correctly set
      LOG.debug("Fragment with key [{}] has no schema type - assuming DWCA.", key);
      schema = OccurrenceSchemaType.DWCA;
    } else {
      schema = OccurrenceSchemaType.valueOf(rawSchema);
    }
    String rawProtocol = OccurrenceResultReader.getString(result, FieldName.PROTOCOL);
    EndpointType protocol = rawProtocol == null ? null : EndpointType.valueOf(rawProtocol);

    Fragment frag;
    if (schema == null || schema == OccurrenceSchemaType.DWCA) {
      frag =
        new Fragment(datasetKey, data, dataHash, Fragment.FragmentType.JSON, protocol, harvestedDate, crawlId, schema,
          null, created);
    } else {
      frag =
        new Fragment(datasetKey, data, dataHash, Fragment.FragmentType.XML, protocol, harvestedDate, crawlId, schema,
          unitQualifier, created);
    }
    frag.setKey(key);

    return frag;
  }

  /**
   * Utility to build an API Occurrence from an HBase row.
   *
   * @return A complete occurrence, or null
   */
  public static Occurrence buildOccurrence(@Nullable Result row) {
    if (row == null || row.isEmpty()) {
      return null;
    } else {
      Occurrence occ = new Occurrence();
      Integer key = Bytes.toInt(row.getRow());
      occ.setKey(key);
      occ.setAltitude(OccurrenceResultReader.getInteger(row, FieldName.I_ALTITUDE));
      occ.setBasisOfRecord(BOR_CONVERTER.toEnum(OccurrenceResultReader.getInteger(row, FieldName.I_BASIS_OF_RECORD)));
//      occ.setCatalogNumber(OccurrenceResultReader.getString(row, FieldName.CATALOG_NUMBER));
      occ.setClassKey(OccurrenceResultReader.getInteger(row, FieldName.I_CLASS_ID));
      occ.setClazz(OccurrenceResultReader.getString(row, FieldName.I_CLASS));
//      occ.setCollectionCode(OccurrenceResultReader.getString(row, FieldName.COLLECTION_CODE));
//      occ.setDataProviderId(OccurrenceResultReader.getInteger(row, FieldName.DATA_PROVIDER_ID));
//      occ.setDataResourceId(OccurrenceResultReader.getInteger(row, FieldName.DATA_RESOURCE_ID));
      occ.setDatasetKey(OccurrenceResultReader.getUuid(row, FieldName.DATASET_KEY));
      occ.setDepth(OccurrenceResultReader.getInteger(row, FieldName.I_DEPTH));
//      occ.setOccurrenceId(OccurrenceResultReader.getString(row, FieldName.DWC_OCCURRENCE_ID));
      occ.setFamily(OccurrenceResultReader.getString(row, FieldName.I_FAMILY));
      occ.setFamilyKey(OccurrenceResultReader.getInteger(row, FieldName.I_FAMILY_ID));
      occ.setGenus(OccurrenceResultReader.getString(row, FieldName.I_GENUS));
      occ.setGenusKey(OccurrenceResultReader.getInteger(row, FieldName.I_GENUS_ID));
//      occ.setGeospatialIssue(OccurrenceResultReader.getInteger(row, FieldName.I_GEOSPATIAL_ISSUE));
//      occ.setHostCountry(Country.fromIsoCode(OccurrenceResultReader.getString(row, FieldName.HOST_COUNTRY)));
//      occ.setInstitutionCode(OccurrenceResultReader.getString(row, FieldName.INSTITUTION_CODE));
      occ.setCountry(Country.fromIsoCode(OccurrenceResultReader.getString(row, FieldName.I_ISO_COUNTRY_CODE)));
      occ.setKingdom(OccurrenceResultReader.getString(row, FieldName.I_KINGDOM));
      occ.setKingdomKey(OccurrenceResultReader.getInteger(row, FieldName.I_KINGDOM_ID));
      occ.setLatitude(OccurrenceResultReader.getDouble(row, FieldName.I_LATITUDE));
      occ.setLongitude(OccurrenceResultReader.getDouble(row, FieldName.I_LONGITUDE));
      occ.setModified(OccurrenceResultReader.getDate(row, FieldName.I_MODIFIED));
//      occ.setOccurrenceMonth(OccurrenceResultReader.getInteger(row, FieldName.I_MONTH));
//      occ.setNubKey(OccurrenceResultReader.getInteger(row, FieldName.I_NUB_ID));
//      occ.setOccurrenceDate(OccurrenceResultReader.getDate(row, FieldName.I_OCCURRENCE_DATE));
      occ.setOrder(OccurrenceResultReader.getString(row, FieldName.I_ORDER));
      occ.setOrderKey(OccurrenceResultReader.getInteger(row, FieldName.I_ORDER_ID));
//      occ.setOtherIssue(OccurrenceResultReader.getInteger(row, FieldName.I_OTHER_ISSUE));
//      occ.setOwningOrgKey(OccurrenceResultReader.getUuid(row, FieldName.OWNING_ORG_KEY));
      occ.setPhylum(OccurrenceResultReader.getString(row, FieldName.I_PHYLUM));
      occ.setPhylumKey(OccurrenceResultReader.getInteger(row, FieldName.I_PHYLUM_ID));
      String rawEndpointType = OccurrenceResultReader.getString(row, FieldName.PROTOCOL);
      if (rawEndpointType == null) {
        LOG.warn("EndpointType is null for occurrence [{}] - possibly corrupt record.", key);
      } else {
        EndpointType endpointType = EndpointType.valueOf(rawEndpointType);
        occ.setProtocol(endpointType);
      }
//      occ.setResourceAccessPointId(OccurrenceResultReader.getInteger(row, FieldName.RESOURCE_ACCESS_POINT_ID));
      occ.setScientificName(OccurrenceResultReader.getString(row, FieldName.I_SCIENTIFIC_NAME));
      occ.setSpecies(OccurrenceResultReader.getString(row, FieldName.I_SPECIES));
      occ.setSpeciesKey(OccurrenceResultReader.getInteger(row, FieldName.I_SPECIES_ID));
//      occ.setTaxonomicIssue(OccurrenceResultReader.getInteger(row, FieldName.I_TAXONOMIC_ISSUE));
//      occ.setUnitQualifier(OccurrenceResultReader.getString(row, FieldName.UNIT_QUALIFIER));
//      occ.setOccurrenceYear(OccurrenceResultReader.getInteger(row, FieldName.I_YEAR));
//      occ.setLocality(OccurrenceResultReader.getString(row, FieldName.LOCALITY));
//      occ.setCounty(OccurrenceResultReader.getString(row, FieldName.COUNTY));
      occ.setStateProvince(OccurrenceResultReader.getString(row, FieldName.STATE_PROVINCE));
//      occ.setContinent(OccurrenceResultReader.getString(row, FieldName.CONTINENT_OCEAN)); // no enums in hbase
//      occ.setCollectorName(OccurrenceResultReader.getString(row, FieldName.COLLECTOR_NAME));
//      occ.setIdentifierName(OccurrenceResultReader.getString(row, FieldName.IDENTIFIER_NAME));
      occ.setIdentificationDate(OccurrenceResultReader.getDate(row, FieldName.IDENTIFICATION_DATE));
      occ.setIdentifiers(extractIdentifiers(key, row, HBaseTableConstants.OCCURRENCE_COLUMN_FAMILY));
      return occ;
    }
  }

  /**
   * Utility to build an API Occurrence from an HBase row.
   *
   * @return A complete occurrence, or null
   */
  public static VerbatimOccurrence buildVerbatimOccurrence(@Nullable Result row) {
    if (row == null || row.isEmpty()) {
      return null;
    } else {
      String rawDatasetKey = OccurrenceResultReader.getString(row, FieldName.DATASET_KEY);
      UUID datasetKey = rawDatasetKey == null ? null : UUID.fromString(rawDatasetKey);

      String rawProtocol = OccurrenceResultReader.getString(row, FieldName.PROTOCOL);
      EndpointType protocol = rawProtocol == null ? null : EndpointType.valueOf(rawProtocol);

      VerbatimOccurrence occ = VerbatimOccurrence.builder().key(Bytes.toInt(row.getRow())).datasetKey(datasetKey)
        .dataProviderId(OccurrenceResultReader.getInteger(row, FieldName.DATA_PROVIDER_ID))
        .dataResourceId(OccurrenceResultReader.getInteger(row, FieldName.DATA_RESOURCE_ID))
        .institutionCode(OccurrenceResultReader.getString(row, FieldName.INSTITUTION_CODE))
        .collectionCode(OccurrenceResultReader.getString(row, FieldName.COLLECTION_CODE))
        .catalogNumber(OccurrenceResultReader.getString(row, FieldName.CATALOG_NUMBER))
        .unitQualifier(OccurrenceResultReader.getString(row, FieldName.UNIT_QUALIFIER))

        .kingdom(OccurrenceResultReader.getString(row, FieldName.KINGDOM))
        .phylum(OccurrenceResultReader.getString(row, FieldName.PHYLUM))
        .order(OccurrenceResultReader.getString(row, FieldName.ORDER))
        .klass(OccurrenceResultReader.getString(row, FieldName.CLASS))
        .family(OccurrenceResultReader.getString(row, FieldName.FAMILY))
        .genus(OccurrenceResultReader.getString(row, FieldName.GENUS))
        .species(OccurrenceResultReader.getString(row, FieldName.SPECIES))
        .subspecies(OccurrenceResultReader.getString(row, FieldName.SUBSPECIES))
        .scientificName(OccurrenceResultReader.getString(row, FieldName.SCIENTIFIC_NAME))
        .rank(OccurrenceResultReader.getString(row, FieldName.RANK))
        .author(OccurrenceResultReader.getString(row, FieldName.AUTHOR))
        .basisOfRecord(OccurrenceResultReader.getString(row, FieldName.BASIS_OF_RECORD))
        .collectorName(OccurrenceResultReader.getString(row, FieldName.COLLECTOR_NAME))
        .identifierName(OccurrenceResultReader.getString(row, FieldName.IDENTIFIER_NAME))

        .continentOrOcean(OccurrenceResultReader.getString(row, FieldName.CONTINENT_OCEAN))
        .country(OccurrenceResultReader.getString(row, FieldName.COUNTRY))
        .county(OccurrenceResultReader.getString(row, FieldName.COUNTY))
        .stateOrProvince(OccurrenceResultReader.getString(row, FieldName.STATE_PROVINCE))
        .locality(OccurrenceResultReader.getString(row, FieldName.LOCALITY))

        .altitudePrecision(OccurrenceResultReader.getString(row, FieldName.ALTITUDE_PRECISION))
        .minAltitude(OccurrenceResultReader.getString(row, FieldName.MIN_ALTITUDE))
        .maxAltitude(OccurrenceResultReader.getString(row, FieldName.MAX_ALTITUDE))
        .depthPrecision(OccurrenceResultReader.getString(row, FieldName.DEPTH_PRECISION))
        .minDepth(OccurrenceResultReader.getString(row, FieldName.MIN_DEPTH))
        .maxDepth(OccurrenceResultReader.getString(row, FieldName.MAX_DEPTH))

        .latitude(OccurrenceResultReader.getString(row, FieldName.LATITUDE))
        .longitude(OccurrenceResultReader.getString(row, FieldName.LONGITUDE))
        .latLongPrecision(OccurrenceResultReader.getString(row, FieldName.LAT_LNG_PRECISION))

        .modified(OccurrenceResultReader.getLong(row, FieldName.MODIFIED))

        .protocol(protocol)

        // TODO: old mysql records were longs, but parsing thinks they're strings - resolve
        // .dateIdentified(OccurrenceResultReader.getLong(result, FieldName.IDENTIFICATION_DATE))
        // TODO: fill in when parsing reads these
        // .dayIdentified();
        // .monthIdentified();
        // .yearIdentified();

        // TODO: include as part of identifiers
        // .dwcOccurrenceId(OccurrenceResultReader.getString(result, FieldName.DWC_OCCURRENCE_ID))

        .day(OccurrenceResultReader.getString(row, FieldName.DAY))
        .month(OccurrenceResultReader.getString(row, FieldName.MONTH))
        .year(OccurrenceResultReader.getString(row, FieldName.YEAR))
        .occurrenceDate(OccurrenceResultReader.getString(row, FieldName.OCCURRENCE_DATE))

        .resourceAccessPointId(OccurrenceResultReader.getInteger(row, FieldName.RESOURCE_ACCESS_POINT_ID))
        .dataProviderId(OccurrenceResultReader.getInteger(row, FieldName.DATA_PROVIDER_ID))
        .dataResourceId(OccurrenceResultReader.getInteger(row, FieldName.DATA_RESOURCE_ID)).build();

      return occ;
    }
  }

  private static List<Identifier> extractIdentifiers(Integer key, Result result, String columnFamily) {
    List<Identifier> records = Lists.newArrayList();
    Integer maxCount = OccurrenceResultReader.getInteger(result, FieldName.IDENTIFIER_COUNT);
    if (maxCount != null) {
      for (int count = 0; count < maxCount; count++) {
        String idCol = HBaseTableConstants.IDENTIFIER_COLUMN + count;
        String idTypeCol = HBaseTableConstants.IDENTIFIER_TYPE_COLUMN + count;
        String id = ResultReader.getString(result, columnFamily, idCol, null);
        String rawType = ResultReader.getString(result, columnFamily, idTypeCol, null);
        if (id != null && rawType != null) {
          IdentifierType idType = null;
          try {
            idType = IdentifierType.valueOf(rawType);
          } catch (IllegalArgumentException e) {
            LOG.warn("Unrecognized value for IdentifierType from field [{}] - data is corrupt.", rawType);
          }
          if (idType != null) {
            Identifier record = new Identifier();
            record.setEntityKey(key);
            record.setIdentifier(id);
            record.setType(idType);
            records.add(record);
          }
        }
      }
    }
    return records;
  }
}
