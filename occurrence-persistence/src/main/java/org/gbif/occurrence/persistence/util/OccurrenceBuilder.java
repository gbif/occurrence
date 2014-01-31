package org.gbif.occurrence.persistence.util;

import org.gbif.api.model.common.Identifier;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.EstablishmentMeans;
import org.gbif.api.vocabulary.IdentifierType;
import org.gbif.api.vocabulary.LifeStage;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.OccurrenceSchemaType;
import org.gbif.api.vocabulary.Sex;
import org.gbif.api.vocabulary.TypeStatus;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.hbase.util.ResultReader;
import org.gbif.occurrence.common.constants.FieldName;
import org.gbif.occurrence.common.converter.BasisOfRecordConverter;
import org.gbif.occurrence.persistence.OccurrenceResultReader;
import org.gbif.occurrence.persistence.api.Fragment;
import org.gbif.occurrence.persistence.constants.HBaseTableConstants;
import org.gbif.occurrence.persistence.hbase.HBaseFieldUtil;
import org.gbif.occurrence.persistence.hbase.HBaseHelper;

import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;
import javax.validation.ValidationException;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.KeyValue;
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
   *
   * @return the Fragment or null if the passed in Result is null
   *
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
    String unitQualifier = OccurrenceResultReader.getTermString(result, GbifTerm.unitQualifier);
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
      occ.setBasisOfRecord(HBaseHelper
        .nullSafeEnum(BasisOfRecord.class, OccurrenceResultReader.getString(row, FieldName.I_BASIS_OF_RECORD)));
      occ.setClassKey(OccurrenceResultReader.getInteger(row, FieldName.I_CLASS_KEY));
      occ.setClazz(OccurrenceResultReader.getString(row, FieldName.I_CLASS));
      occ.setDatasetKey(OccurrenceResultReader.getUuid(row, FieldName.DATASET_KEY));
      occ.setDepth(OccurrenceResultReader.getInteger(row, FieldName.I_DEPTH));
      occ.setFamily(OccurrenceResultReader.getString(row, FieldName.I_FAMILY));
      occ.setFamilyKey(OccurrenceResultReader.getInteger(row, FieldName.I_FAMILY_KEY));
      occ.setGenus(OccurrenceResultReader.getString(row, FieldName.I_GENUS));
      occ.setGenusKey(OccurrenceResultReader.getInteger(row, FieldName.I_GENUS_KEY));
      occ
        .setPublishingCountry(Country.fromIsoCode(OccurrenceResultReader.getString(row, FieldName.PUB_COUNTRY)));
      occ.setCountry(Country.fromIsoCode(OccurrenceResultReader.getString(row, FieldName.I_COUNTRY)));
      occ.setKingdom(OccurrenceResultReader.getString(row, FieldName.I_KINGDOM));
      occ.setKingdomKey(OccurrenceResultReader.getInteger(row, FieldName.I_KINGDOM_KEY));
      occ.setLatitude(OccurrenceResultReader.getDouble(row, FieldName.I_LATITUDE));
      occ.setLongitude(OccurrenceResultReader.getDouble(row, FieldName.I_LONGITUDE));
      occ.setModified(OccurrenceResultReader.getDate(row, FieldName.I_MODIFIED));
      occ.setMonth(OccurrenceResultReader.getInteger(row, FieldName.I_MONTH));
      occ.setTaxonKey(OccurrenceResultReader.getInteger(row, FieldName.I_TAXON_KEY));
      occ.setEventDate(OccurrenceResultReader.getDate(row, FieldName.I_EVENT_DATE));
      occ.setOrder(OccurrenceResultReader.getString(row, FieldName.I_ORDER));
      occ.setOrderKey(OccurrenceResultReader.getInteger(row, FieldName.I_ORDER_KEY));
      occ.setPublishingOrgKey(OccurrenceResultReader.getUuid(row, FieldName.PUB_ORG_KEY));
      occ.setPhylum(OccurrenceResultReader.getString(row, FieldName.I_PHYLUM));
      occ.setPhylumKey(OccurrenceResultReader.getInteger(row, FieldName.I_PHYLUM_KEY));
      occ.setProtocol(HBaseHelper.nullSafeEnum(EndpointType.class, OccurrenceResultReader.getString(row, FieldName.PROTOCOL)));
      occ.setScientificName(OccurrenceResultReader.getString(row, FieldName.I_SCIENTIFIC_NAME));
      occ.setSpecies(OccurrenceResultReader.getString(row, FieldName.I_SPECIES));
      occ.setSpeciesKey(OccurrenceResultReader.getInteger(row, FieldName.I_SPECIES_KEY));
      occ.setYear(OccurrenceResultReader.getInteger(row, FieldName.I_YEAR));
      occ.setStateProvince(OccurrenceResultReader.getString(row, FieldName.I_STATE_PROVINCE));
      occ.setContinent(HBaseHelper.nullSafeEnum(Continent.class, OccurrenceResultReader.getString(row, FieldName.I_CONTINENT)));
      occ.setDateIdentified(OccurrenceResultReader.getDate(row, FieldName.I_DATE_IDENTIFIED));

      // new for occurrence widening
      occ.setAltitudeAccuracy(OccurrenceResultReader.getInteger(row, FieldName.I_ALTITUDE_ACC));
      occ.setCoordinateAccuracy(OccurrenceResultReader.getDouble(row, FieldName.I_COORD_ACCURACY));
      occ.setDay(OccurrenceResultReader.getInteger(row, FieldName.I_DAY));
      occ.setDepthAccuracy(OccurrenceResultReader.getInteger(row, FieldName.I_DEPTH_ACC));
      occ.setEstablishmentMeans(HBaseHelper.nullSafeEnum(EstablishmentMeans.class, OccurrenceResultReader.getString(row, FieldName.I_ESTAB_MEANS)));
      occ.setGeodeticDatum(OccurrenceResultReader.getString(row, FieldName.I_GEODETIC_DATUM));
      occ.setIndividualCount(OccurrenceResultReader.getInteger(row, FieldName.I_INDIVIDUAL_COUNT));
      occ.setLastInterpreted(OccurrenceResultReader.getDate(row, FieldName.LAST_INTERPRETED));
      occ.setLifeStage(HBaseHelper.nullSafeEnum(LifeStage.class,OccurrenceResultReader.getString(row, FieldName.I_LIFE_STAGE)));
      occ.setSex(HBaseHelper.nullSafeEnum(Sex.class, OccurrenceResultReader.getString(row, FieldName.I_SEX)));
      occ.setStateProvince(OccurrenceResultReader.getString(row, FieldName.I_STATE_PROVINCE));
      occ.setWaterBody(OccurrenceResultReader.getString(row, FieldName.I_WATERBODY));
      occ.setSubgenus(OccurrenceResultReader.getString(row, FieldName.I_SUBGENUS));
      occ.setSubgenusKey(OccurrenceResultReader.getInteger(row, FieldName.I_SUBGENUS_KEY));
      occ.setTypeStatus(HBaseHelper.nullSafeEnum(TypeStatus.class,OccurrenceResultReader.getString(row, FieldName.I_TYPE_STATUS)));
      occ.setTypifiedName(OccurrenceResultReader.getString(row, FieldName.I_TYPIFIED_NAME));

      occ.setIdentifiers(extractIdentifiers(key, row, HBaseTableConstants.OCCURRENCE_COLUMN_FAMILY));
      occ.setIssues(extractIssues(row));

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
    }

    VerbatimOccurrence verb = new VerbatimOccurrence();
    verb.setKey(Bytes.toInt(row.getRow()));
    verb.setDatasetKey(OccurrenceResultReader.getUuid(row, FieldName.DATASET_KEY));
    verb.setPublishingOrgKey(OccurrenceResultReader.getUuid(row, FieldName.PUB_ORG_KEY));
    verb.setPublishingCountry(Country.fromIsoCode(OccurrenceResultReader.getString(row, FieldName.PUB_COUNTRY)));
    verb.setLastCrawled(OccurrenceResultReader.getDate(row, FieldName.HARVESTED_DATE));
    verb.setProtocol(EndpointType.fromString(OccurrenceResultReader.getString(row, FieldName.PROTOCOL)));

    // all Term fields in row are prefixed
    for (KeyValue kv : row.raw()) {
      Term term = HBaseFieldUtil.getTermFromColumn(kv.getQualifier());
      if (term != null) {
        verb.setField(term, Bytes.toString(kv.getValue()));
      }
    }

    return verb;
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

  private static Set<OccurrenceIssue> extractIssues(Result result) {
    Set<OccurrenceIssue> issues = EnumSet.noneOf(OccurrenceIssue.class);
    for (OccurrenceIssue issue : OccurrenceIssue.values()) {
      HBaseFieldUtil.HBaseColumn column = HBaseFieldUtil.getHBaseColumn(issue);
      byte[] val = result.getValue(Bytes.toBytes(column.getColumnFamilyName()), Bytes.toBytes(column.getColumnName()));
      if (val != null) {
        issues.add(issue);
      }
    }

    return issues;
  }


}
