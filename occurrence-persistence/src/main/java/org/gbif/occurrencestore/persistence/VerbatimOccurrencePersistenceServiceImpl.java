package org.gbif.occurrencestore.persistence;

import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.occurrencestore.common.model.constants.FieldName;
import org.gbif.occurrencestore.persistence.api.VerbatimOccurrence;
import org.gbif.occurrencestore.persistence.api.VerbatimOccurrencePersistenceService;
import org.gbif.occurrencestore.persistence.constants.HBaseTableConstants;
import org.gbif.occurrencestore.persistence.hbase.HBaseHelper;
import org.gbif.occurrencestore.persistence.util.OccurrenceBuilder;

import java.io.IOException;
import java.util.Date;
import javax.annotation.Nullable;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Reads and writes VerbatimOccurrences from/to HBase.
 */
@Singleton
public class VerbatimOccurrencePersistenceServiceImpl implements VerbatimOccurrencePersistenceService {

  private static final Logger LOG = LoggerFactory.getLogger(VerbatimOccurrencePersistenceServiceImpl.class);

  private final String occurrenceTableName;
  private final HTablePool tablePool;

  @Inject
  public VerbatimOccurrencePersistenceServiceImpl(@Named("table_name") String tableName, HTablePool tablePool) {
    this.occurrenceTableName = tableName;
    this.tablePool = tablePool;
  }

  @Override
  public VerbatimOccurrence get(@Nullable Integer key) {
    if (key == null) {
      return null;
    }
    VerbatimOccurrence occ = null;
    HTableInterface table = null;
    try {
      table = tablePool.getTable(occurrenceTableName);
      Get get = new Get(Bytes.toBytes(key));
      Result result = table.get(get);
      if (result == null || result.isEmpty()) {
        LOG.info("Couldn't find occurrence for key [{}], returning null", key);
        return null;
      }
      occ = OccurrenceBuilder.buildVerbatimOccurrence(result);
    } catch (IOException e) {
      throw new ServiceUnavailableException("Could not read from HBase", e);
    } finally {
      closeTable(table);
    }

    return occ;
  }

  @Override
  public void update(VerbatimOccurrence occ) {
    checkNotNull(occ, "occurrence can't be null");
    checkNotNull(occ.getKey(), "occurrence's key can't be null");

    HTableInterface occTable = null;
    try {
      occTable = tablePool.getTable(occurrenceTableName);
      byte[] cf = Bytes.toBytes(HBaseTableConstants.OCCURRENCE_COLUMN_FAMILY);
      occ.setModified(new Date().getTime());
      write(occTable, cf, occ, true);
    } catch (IOException e) {
      throw new ServiceUnavailableException("Could not access HBase", e);
    } finally {
      closeTable(occTable);
    }
  }

  private static VerbatimOccurrence write(HTableInterface occTable, byte[] cf, VerbatimOccurrence occ, boolean dn) throws IOException {
    Put put = new Put(Bytes.toBytes(occ.getKey()));
    Delete del = null;
    if (dn) {
      del = new Delete(Bytes.toBytes(occ.getKey()));
    }

    HBaseHelper.writeField(FieldName.AUTHOR,
      occ.getAuthor() == null ? null : Bytes.toBytes(occ.getAuthor()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.ALTITUDE_PRECISION,
      occ.getAltitudePrecision() == null ? null : Bytes.toBytes(occ.getAltitudePrecision()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.BASIS_OF_RECORD,
      occ.getBasisOfRecord() == null ? null : Bytes.toBytes(occ.getBasisOfRecord()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.CATALOG_NUMBER,
      occ.getCatalogNumber() == null ? null : Bytes.toBytes(occ.getCatalogNumber()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.COLLECTION_CODE,
      occ.getCollectionCode() == null ? null : Bytes.toBytes(occ.getCollectionCode()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.COLLECTOR_NAME,
      occ.getCollectorName() == null ? null : Bytes.toBytes(occ.getCollectorName()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.CONTINENT_OCEAN,
      occ.getContinentOrOcean() == null ? null : Bytes.toBytes(occ.getContinentOrOcean()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.COUNTRY,
      occ.getCountry() == null ? null : Bytes.toBytes(occ.getCountry()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.COUNTY,
      occ.getCounty() == null ? null : Bytes.toBytes(occ.getCounty()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.DATASET_KEY,
      occ.getDatasetKey() == null ? null : Bytes.toBytes(occ.getDatasetKey().toString()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.DATA_PROVIDER_ID,
      occ.getDataProviderId() == null ? null : Bytes.toBytes(occ.getDataProviderId()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.DATA_RESOURCE_ID,
      occ.getDataResourceId() == null ? null : Bytes.toBytes(occ.getDataResourceId()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.DATA_PROVIDER_ID,
      occ.getDataProviderId() == null ? null : Bytes.toBytes(occ.getDataProviderId()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.DAY,
      occ.getDay() == null ? null : Bytes.toBytes(occ.getDay()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.DEPTH_PRECISION,
      occ.getDepthPrecision() == null ? null : Bytes.toBytes(occ.getDepthPrecision()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.FAMILY,
      occ.getFamily() == null ? null : Bytes.toBytes(occ.getFamily()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.GENUS,
      occ.getGenus() == null ? null : Bytes.toBytes(occ.getGenus()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.IDENTIFIER_NAME,
      occ.getIdentifierName() == null ? null : Bytes.toBytes(occ.getIdentifierName()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.INSTITUTION_CODE,
      occ.getInstitutionCode() == null ? null : Bytes.toBytes(occ.getInstitutionCode()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.KINGDOM,
      occ.getKingdom() == null ? null : Bytes.toBytes(occ.getKingdom()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.CLASS,
      occ.getKlass() == null ? null : Bytes.toBytes(occ.getKlass()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.LATITUDE,
      occ.getLatitude() == null ? null : Bytes.toBytes(occ.getLatitude()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.LAT_LNG_PRECISION,
      occ.getLatLongPrecision() == null ? null : Bytes.toBytes(occ.getLatLongPrecision()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.LOCALITY,
      occ.getLocality() == null ? null : Bytes.toBytes(occ.getLocality()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.LONGITUDE,
      occ.getLongitude() == null ? null : Bytes.toBytes(occ.getLongitude()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.MAX_ALTITUDE,
      occ.getMaxAltitude() == null ? null : Bytes.toBytes(occ.getMaxAltitude()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.MAX_DEPTH,
      occ.getMaxDepth() == null ? null : Bytes.toBytes(occ.getMaxDepth()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.MIN_ALTITUDE,
      occ.getMinAltitude() == null ? null : Bytes.toBytes(occ.getMinAltitude()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.MIN_DEPTH,
      occ.getMinDepth() == null ? null : Bytes.toBytes(occ.getMinDepth()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.MODIFIED,
      occ.getModified() == null ? null : Bytes.toBytes(occ.getModified()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.MONTH,
      occ.getMonth() == null ? null : Bytes.toBytes(occ.getMonth()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.OCCURRENCE_DATE,
      occ.getOccurrenceDate() == null ? null : Bytes.toBytes(occ.getOccurrenceDate()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.ORDER,
      occ.getOrder() == null ? null : Bytes.toBytes(occ.getOrder()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.PHYLUM,
      occ.getPhylum() == null ? null : Bytes.toBytes(occ.getPhylum()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.PROTOCOL,
      occ.getProtocol() == null ? null : Bytes.toBytes(occ.getProtocol().toString()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.RANK,
      occ.getRank() == null ? null : Bytes.toBytes(occ.getRank()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.RESOURCE_ACCESS_POINT_ID,
      occ.getResourceAccessPointId() == null ? null : Bytes.toBytes(occ.getResourceAccessPointId()), dn, cf, put,del);
    HBaseHelper.writeField(FieldName.SCIENTIFIC_NAME,
      occ.getScientificName() == null ? null : Bytes.toBytes(occ.getScientificName()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.SPECIES,
      occ.getSpecies() == null ? null : Bytes.toBytes(occ.getSpecies()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.STATE_PROVINCE,
      occ.getStateOrProvince() == null ? null : Bytes.toBytes(occ.getStateOrProvince()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.SUBSPECIES,
      occ.getSubspecies() == null ? null : Bytes.toBytes(occ.getSubspecies()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.UNIT_QUALIFIER,
      occ.getUnitQualifier() == null ? null : Bytes.toBytes(occ.getUnitQualifier()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.YEAR,
      occ.getYear() == null ? null : Bytes.toBytes(occ.getYear()), dn, cf, put, del);

    occTable.put(put);
    if (dn && !del.isEmpty()) {
      occTable.delete(del);
    }
    occTable.flushCommits();

    return occ;
  }

  private static void closeTable(HTableInterface table) {
    if (table != null) {
      try {
        table.close();
      } catch (IOException e) {
        LOG.warn("Couldn't return table to pool - continuing with possible memory leak", e);
      }
    }
  }
}
