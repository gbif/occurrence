package org.gbif.occurrence.persistence;

import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.api.model.common.Identifier;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.occurrence.common.constants.FieldName;
import org.gbif.occurrence.persistence.api.OccurrencePersistenceService;
import org.gbif.occurrence.persistence.constants.HBaseTableConstants;
import org.gbif.occurrence.persistence.hbase.HBaseFieldUtil;
import org.gbif.occurrence.persistence.hbase.HBaseHelper;
import org.gbif.occurrence.persistence.util.OccurrenceBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;


/**
 * An implementation of OccurrenceService and OccurrenceWriter for persisting and retrieving Occurrence objects in
 * HBase.
 */
@Singleton
public class OccurrencePersistenceServiceImpl implements OccurrencePersistenceService {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrencePersistenceServiceImpl.class);
  private static final int SCANNER_BATCH_SIZE = 50;
  private static final int SCANNER_CACHE_SIZE = 50;

  private final String occurrenceTableName;
  private final HTablePool tablePool;

  @Inject
  public OccurrencePersistenceServiceImpl(@Named("table_name") String tableName, HTablePool tablePool) {
    this.occurrenceTableName = checkNotNull(tableName, "tableName can't be null");
    this.tablePool = checkNotNull(tablePool, "tablePool can't be null");
  }

  /**
   * Note that the returned fragment here is a String that holds the actual xml or json snippet for this occurrence,
   * and not the Fragment object that is used elsewhere.
   *
   * @param key that identifies an occurrence
   *
   * @return a String holding the original xml or json snippet for this occurrence
   */
  @Override
  public String getFragment(int key) {
    String fragment = null;
    HTableInterface table = null;
    try {
      table = tablePool.getTable(occurrenceTableName);
      Get get = new Get(Bytes.toBytes(key));
      Result result = table.get(get);
      if (result == null || result.isEmpty()) {
        LOG.info("Couldn't find occurrence for id [{}], returning null", key);
        return null;
      }
      byte[] rawFragment = OccurrenceResultReader.getBytes(result, FieldName.FRAGMENT);
      if (rawFragment != null) {
        fragment = Bytes.toString(rawFragment);
      }
    } catch (IOException e) {
      throw new ServiceUnavailableException("Could not read from HBase", e);
    } finally {
      closeTable(table);
    }

    return fragment;
  }

  @Nullable
  @Override
  public VerbatimOccurrence getVerbatim(@Nullable Integer key) {
    if (key == null) {
      return null;
    }
    VerbatimOccurrence verb = null;
    HTableInterface table = null;
    try {
      table = tablePool.getTable(occurrenceTableName);
      Get get = new Get(Bytes.toBytes(key));
      Result result = table.get(get);
      if (result == null || result.isEmpty()) {
        LOG.debug("Couldn't find occurrence for key [{}], returning null", key);
        return null;
      }
      verb = OccurrenceBuilder.buildVerbatimOccurrence(result);
    } catch (IOException e) {
      throw new ServiceUnavailableException("Could not read from HBase", e);
    } finally {
      closeTable(table);
    }

    return verb;
  }

  @Override
  public Occurrence get(@Nullable Integer key) {
    if (key == null) {
      return null;
    }
    Occurrence occ = null;
    HTableInterface table = null;
    try {
      table = tablePool.getTable(occurrenceTableName);
      Get get = new Get(Bytes.toBytes(key));
      Result result = table.get(get);
      if (result == null || result.isEmpty()) {
        LOG.debug("Couldn't find occurrence for key [{}], returning null", key);
        return null;
      }
      occ = OccurrenceBuilder.buildOccurrence(result);
    } catch (IOException e) {
      throw new ServiceUnavailableException("Could not read from HBase", e);
    } finally {
      closeTable(table);
    }

    return occ;
  }

  @Override
  public Iterator<Integer> getKeysByColumn(byte[] columnValue, FieldName columnName) {
    byte[] cf = Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(columnName).getColumnFamilyName());
    byte[] col = Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(columnName).getColumnName());
    Scan scan = new Scan();
    scan.setBatch(SCANNER_BATCH_SIZE);
    scan.setCaching(SCANNER_CACHE_SIZE);
    scan.addColumn(cf, col);
    SingleColumnValueFilter filter = new SingleColumnValueFilter(cf, col, CompareFilter.CompareOp.EQUAL, columnValue);
    scan.setFilter(filter);

    return new OccurrenceKeyIterator(tablePool, occurrenceTableName, scan);
  }

  @Override
  public void update(VerbatimOccurrence verb) {
    checkNotNull(verb, "verb can't be null");
    checkNotNull(verb.getKey(), "verb's key can't be null");

    HTableInterface occTable = null;
    try {
      occTable = tablePool.getTable(occurrenceTableName);
      byte[] cf = Bytes.toBytes(HBaseTableConstants.OCCURRENCE_COLUMN_FAMILY);
      writeVerbatim(occTable, cf, verb, true);
    } catch (IOException e) {
      throw new ServiceUnavailableException("Could not access HBase", e);
    } finally {
      closeTable(occTable);
    }
  }

  @Override
  public void update(Occurrence occ) {
    checkNotNull(occ, "occurrence can't be null");
    checkNotNull(occ.getKey(), "occurrence's key can't be null");

    HTableInterface occTable = null;
    try {
      occTable = tablePool.getTable(occurrenceTableName);
      byte[] cf = Bytes.toBytes(HBaseTableConstants.OCCURRENCE_COLUMN_FAMILY);
      writeOccurrence(occTable, cf, occ, true);
    } catch (IOException e) {
      throw new ServiceUnavailableException("Could not access HBase", e);
    } finally {
      closeTable(occTable);
    }
  }

  @Override
  public Occurrence delete(int occurrenceKey) {
    Occurrence occurrence = get(occurrenceKey);
    if (occurrence == null) {
      LOG.debug("Occurrence for key [{}] not found, ignoring delete request.", occurrenceKey);
    } else {
      delete(new ImmutableList.Builder<Integer>().add(occurrenceKey).build());
    }

    LOG.debug("<< delete [{}]", occurrenceKey);
    return occurrence;
  }

  @Override
  public void delete(List<Integer> occurrenceKeys) {
    checkNotNull(occurrenceKeys, "occurrenceKeys can't be null");

    HTableInterface occTable = null;
    try {
      occTable = tablePool.getTable(occurrenceTableName);
      List<Delete> deletes = Lists.newArrayList();
      for (Integer occurrenceKey : occurrenceKeys) {
        if (occurrenceKey != null) {
          deletes.add(new Delete(Bytes.toBytes(occurrenceKey)));
        }
      }
      LOG.debug("Deleting [{}] occurrences", occurrenceKeys.size());
      occTable.delete(deletes);
    } catch (IOException e) {
      throw new ServiceUnavailableException("Could not access HBase", e);
    } finally {
      closeTable(occTable);
    }
  }

  private static void writeVerbatim(HTableInterface occTable, byte[] cf, VerbatimOccurrence occ, boolean dn)
    throws IOException {
    byte[] key = Bytes.toBytes(occ.getKey());

    Put put = new Put(key);
    Delete del = new Delete(key);

    doVerbatimPutDelete(occTable, cf, put, del, occ, dn);
    HBaseHelper.writeField(FieldName.LAST_PARSED, HBaseHelper.nullSafeBytes(occ.getLastParsed()), dn, cf, put, del);

    occTable.put(put);
    if (dn && !del.isEmpty()) {
      occTable.delete(del);
    }
    occTable.flushCommits();
  }

  private static void doVerbatimPutDelete(HTableInterface occTable, byte[] cf, Put put, Delete del,
    VerbatimOccurrence occ, boolean dn) throws IOException {
    byte[] key = Bytes.toBytes(occ.getKey());
    if (dn) {
      // start by scheduling deletion of all terms not in the occ
      Get get = new Get(key);
      Result row = occTable.get(get);
      for (KeyValue kv : row.raw()) {
        String colName = Bytes.toString(kv.getQualifier());
        if (colName.startsWith(HBaseTableConstants.KNOWN_TERM_PREFIX)) {
          Term term =
            TermFactory.instance().findTerm(colName.substring(HBaseTableConstants.KNOWN_TERM_PREFIX.length()));
          if (occ.getVerbatimField(term) == null) {
            del.deleteColumns(cf, kv.getQualifier());
          }
        }
      }
    }

    for (Map.Entry<Term, String> entry : occ.getVerbatimFields().entrySet()) {
      if (entry.getValue() != null) {
        put.add(cf, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(entry.getKey()).getColumnName()),
          Bytes.toBytes(entry.getValue()));
      }
    }

    HBaseHelper.writeField(FieldName.DATASET_KEY, HBaseHelper.nullSafeBytes(occ.getDatasetKey()), dn, cf, put, del);
    HBaseHelper
      .writeField(FieldName.PUB_COUNTRY, HBaseHelper.nullSafeBytes(occ.getPublishingCountry()), dn, cf, put, del);
    HBaseHelper
      .writeField(FieldName.PUB_ORG_KEY, HBaseHelper.nullSafeBytes(occ.getPublishingOrgKey()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.PROTOCOL, HBaseHelper.nullSafeBytes(occ.getProtocol()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.LAST_CRAWLED, HBaseHelper.nullSafeBytes(occ.getLastCrawled()), dn, cf, put, del);
  }

  private static void writeOccurrence(HTableInterface occTable, byte[] cf, Occurrence occ, boolean dn)
    throws IOException {

    byte[] key = Bytes.toBytes(occ.getKey());
    Put put = new Put(key);
    Delete del = new Delete(key);

    doVerbatimPutDelete(occTable, cf, put, del, occ, dn);

    HBaseHelper.writeField(FieldName.I_ALTITUDE, HBaseHelper.nullSafeBytes(occ.getElevation()), dn, cf, put, del);
    HBaseHelper
      .writeField(FieldName.I_BASIS_OF_RECORD, HBaseHelper.nullSafeBytes(occ.getBasisOfRecord()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.I_CLASS_KEY, HBaseHelper.nullSafeBytes(occ.getClassKey()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.I_CLASS, HBaseHelper.nullSafeBytes(occ.getClazz()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.I_DEPTH, HBaseHelper.nullSafeBytes(occ.getDepth()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.I_FAMILY, HBaseHelper.nullSafeBytes(occ.getFamily()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.I_FAMILY_KEY, HBaseHelper.nullSafeBytes(occ.getFamilyKey()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.I_GENUS, HBaseHelper.nullSafeBytes(occ.getGenus()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.I_GENUS_KEY, HBaseHelper.nullSafeBytes(occ.getGenusKey()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.I_KINGDOM, HBaseHelper.nullSafeBytes(occ.getKingdom()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.I_KINGDOM_KEY, HBaseHelper.nullSafeBytes(occ.getKingdomKey()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.I_LATITUDE, HBaseHelper.nullSafeBytes(occ.getDecimalLatitude()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.I_LONGITUDE, HBaseHelper.nullSafeBytes(occ.getDecimalLongitude()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.I_COUNTRY, HBaseHelper.nullSafeBytes(occ.getCountry()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.I_MODIFIED, HBaseHelper.nullSafeBytes(occ.getModified()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.I_MONTH, HBaseHelper.nullSafeBytes(occ.getMonth()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.I_TAXON_KEY, HBaseHelper.nullSafeBytes(occ.getTaxonKey()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.I_EVENT_DATE, HBaseHelper.nullSafeBytes(occ.getEventDate()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.I_ORDER, HBaseHelper.nullSafeBytes(occ.getOrder()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.I_ORDER_KEY, HBaseHelper.nullSafeBytes(occ.getOrderKey()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.I_PHYLUM, HBaseHelper.nullSafeBytes(occ.getPhylum()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.I_PHYLUM_KEY, HBaseHelper.nullSafeBytes(occ.getPhylumKey()), dn, cf, put, del);
    HBaseHelper
      .writeField(FieldName.I_SCIENTIFIC_NAME, HBaseHelper.nullSafeBytes(occ.getScientificName()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.I_SPECIES, HBaseHelper.nullSafeBytes(occ.getSpecies()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.I_SPECIES_KEY, HBaseHelper.nullSafeBytes(occ.getSpeciesKey()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.I_YEAR, HBaseHelper.nullSafeBytes(occ.getYear()), dn, cf, put, del);

    HBaseHelper
      .writeField(FieldName.I_ALTITUDE_ACC, HBaseHelper.nullSafeBytes(occ.getElevationAccuracy()), dn, cf, put, del);
    HBaseHelper
      .writeField(FieldName.I_COORD_ACCURACY, HBaseHelper.nullSafeBytes(occ.getCoordinateAccuracy()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.I_CONTINENT, HBaseHelper.nullSafeBytes(occ.getContinent()), dn, cf, put, del);
    HBaseHelper
      .writeField(FieldName.I_DATE_IDENTIFIED, HBaseHelper.nullSafeBytes(occ.getDateIdentified()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.I_DAY, HBaseHelper.nullSafeBytes(occ.getDay()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.I_DEPTH_ACC, HBaseHelper.nullSafeBytes(occ.getDepthAccuracy()), dn, cf, put, del);
    HBaseHelper
      .writeField(FieldName.I_ESTAB_MEANS, HBaseHelper.nullSafeBytes(occ.getEstablishmentMeans()), dn, cf, put, del);
    HBaseHelper
      .writeField(FieldName.I_GEODETIC_DATUM, HBaseHelper.nullSafeBytes(occ.getGeodeticDatum()), dn, cf, put, del);
    HBaseHelper
      .writeField(FieldName.I_INDIVIDUAL_COUNT, HBaseHelper.nullSafeBytes(occ.getIndividualCount()), dn, cf, put, del);
    HBaseHelper
      .writeField(FieldName.LAST_INTERPRETED, HBaseHelper.nullSafeBytes(occ.getLastInterpreted()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.I_LIFE_STAGE, HBaseHelper.nullSafeBytes(occ.getLifeStage()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.I_SEX, HBaseHelper.nullSafeBytes(occ.getSex()), dn, cf, put, del);
    HBaseHelper
      .writeField(FieldName.I_STATE_PROVINCE, HBaseHelper.nullSafeBytes(occ.getStateProvince()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.I_WATERBODY, HBaseHelper.nullSafeBytes(occ.getWaterBody()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.I_SUBGENUS, HBaseHelper.nullSafeBytes(occ.getSubgenus()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.I_SUBGENUS_KEY, HBaseHelper.nullSafeBytes(occ.getSubgenusKey()), dn, cf, put, del);
    HBaseHelper.writeField(FieldName.I_TYPE_STATUS, HBaseHelper.nullSafeBytes(occ.getTypeStatus()), dn, cf, put, del);
    HBaseHelper
      .writeField(FieldName.I_TYPIFIED_NAME, HBaseHelper.nullSafeBytes(occ.getTypifiedName()), dn, cf, put, del);

    if (dn) {
      // Identifiers
      deleteOldIdentifiers(occTable, occ.getKey(), cf);
      // OccurrenceIssues
      for (OccurrenceIssue issue : OccurrenceIssue.values()) {
        if (!occ.getIssues().contains(issue)) {
          del.deleteColumns(cf, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(issue).getColumnName()));
        }
      }
    }

    if (occ.getIdentifiers() != null && !occ.getIdentifiers().isEmpty()) {
      addIdentifiersToPut(put, cf, occ.getIdentifiers());
    }

    for (OccurrenceIssue issue : occ.getIssues()) {
      put.add(cf, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(issue).getColumnName()), Bytes.toBytes(1));
    }

    occTable.put(put);
    if (dn && !del.isEmpty()) {
      occTable.delete(del);
    }
    occTable.flushCommits();
  }

  private static void addIdentifiersToPut(Put put, byte[] columnFamily, Collection<Identifier> records) {
    put.add(columnFamily, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.IDENTIFIER_COUNT).getColumnName()),
      Bytes.toBytes(records.size()));
    int count = -1;
    for (Identifier record : records) {
      count++;
      String idCol = HBaseTableConstants.IDENTIFIER_COLUMN + count;
      String idTypeCol = HBaseTableConstants.IDENTIFIER_TYPE_COLUMN + count;
      put.add(columnFamily, Bytes.toBytes(idCol), Bytes.toBytes(record.getIdentifier()));
      put.add(columnFamily, Bytes.toBytes(idTypeCol), Bytes.toBytes(record.getType().toString()));
    }
  }

  private static void deleteOldIdentifiers(HTableInterface occTable, int id, byte[] columnFamily) throws IOException {
    Get get = new Get(Bytes.toBytes(id));
    get.addColumn(columnFamily,
      Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.IDENTIFIER_COUNT).getColumnName()));
    Result result = occTable.get(get);
    Integer maxCount = OccurrenceResultReader.getInteger(result, FieldName.IDENTIFIER_COUNT);
    if (maxCount != null && maxCount > 0) {
      Delete delete = new Delete(Bytes.toBytes(id));
      for (int count = 0; count < maxCount; count++) {
        String idCol = HBaseTableConstants.IDENTIFIER_COLUMN + count;
        delete.deleteColumn(columnFamily, Bytes.toBytes(idCol));
        String idTypeCol = HBaseTableConstants.IDENTIFIER_TYPE_COLUMN + count;
        delete.deleteColumn(columnFamily, Bytes.toBytes(idTypeCol));
      }
      occTable.delete(delete);
      // set count to 0
      Put put = new Put(Bytes.toBytes(id));
      put.add(columnFamily, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(FieldName.IDENTIFIER_COUNT).getColumnName()),
        Bytes.toBytes(0));
      occTable.put(put);
    }
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
