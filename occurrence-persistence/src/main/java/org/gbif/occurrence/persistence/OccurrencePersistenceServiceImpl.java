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
  private static final TermFactory TERM_FACTORY = TermFactory.instance();
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
    byte[] cf = Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(columnName).getFamilyName());
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
    updateOcc(verb);
  }

  @Override
  public void update(Occurrence occ) {
    updateOcc(occ);
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


  private <T extends VerbatimOccurrence> void updateOcc(T occ) {
    checkNotNull(occ, "occurrence can't be null");
    checkNotNull(occ.getKey(), "occurrence's key can't be null");

    HTableInterface occTable = null;
    try {
      occTable = tablePool.getTable(occurrenceTableName);
      byte[] cf = Bytes.toBytes(HBaseTableConstants.OCCURRENCE_COLUMN_FAMILY);
      byte[] key = Bytes.toBytes(occ.getKey());

      Put put = new Put(key);
      Delete del = new Delete(key);

      populateVerbatimPutDelete(occTable, cf, key, put, del, occ, occ instanceof Occurrence);

      // add interpreted occurrence terms
      if (occ instanceof Occurrence) {
        populateInterpretedPutDelete(occTable, cf, key, put, del, (Occurrence) occ);
      }

      occTable.put(put);
      if (!del.isEmpty()) {
        occTable.delete(del);
      }
      occTable.flushCommits();

    } catch (IOException e) {
      throw new ServiceUnavailableException("Could not access HBase", e);
    } finally {
      closeTable(occTable);
    }
  }

  /**
   * Populates the put and delete for a verbatim record.
   * @param keepInterpretedVerbatimColumns if true does not delete any of the verbatim columns removed
   *                                       during interpretation
   * @throws IOException
   */
  private void populateVerbatimPutDelete(HTableInterface occTable, byte[] cf, byte[] key, Put put, Delete del,
                              VerbatimOccurrence occ, boolean keepInterpretedVerbatimColumns) throws IOException {

    // start by scheduling deletion of all terms not in the occ
    Get get = new Get(key);
    Result row = occTable.get(get);
    for (KeyValue kv : row.raw()) {
      String colName = Bytes.toString(kv.getQualifier());
      if (colName.startsWith(HBaseTableConstants.KNOWN_TERM_PREFIX)) {
        Term term = TERM_FACTORY.findTerm(colName.substring(HBaseTableConstants.KNOWN_TERM_PREFIX.length()));
        if (occ.getVerbatimField(term) == null &&
            // only remove the interpreted verbatim terms if explicitly requested
            (!keepInterpretedVerbatimColumns || !OccurrenceBuilder.INTERPRETED_TERMS.contains(term)) ) {
          del.deleteColumns(cf, kv.getQualifier());
        }
      }
    }

    // put all non null verbatim terms from the map
    for (Map.Entry<Term, String> entry : occ.getVerbatimFields().entrySet()) {
      if (entry.getValue() != null) {
        put.add(cf, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(entry.getKey()).getColumnName()),
              Bytes.toBytes(entry.getValue()));
      }
    }

    HBaseHelper.writeField(FieldName.DATASET_KEY, HBaseHelper.nullSafeBytes(occ.getDatasetKey()), cf, put, del);
    HBaseHelper.writeField(FieldName.PUB_COUNTRY_CODE,HBaseHelper.nullSafeBytes(occ.getPublishingCountry()),cf,put,del);
    HBaseHelper.writeField(FieldName.PUB_ORG_KEY,HBaseHelper.nullSafeBytes(occ.getPublishingOrgKey()),cf, put, del);
    HBaseHelper.writeField(FieldName.PROTOCOL, HBaseHelper.nullSafeBytes(occ.getProtocol()), cf, put, del);
    HBaseHelper.writeField(FieldName.LAST_CRAWLED, HBaseHelper.nullSafeBytes(occ.getLastCrawled()), cf, put, del);
    HBaseHelper.writeField(FieldName.LAST_PARSED, HBaseHelper.nullSafeBytes(occ.getLastParsed()), cf, put, del);
  }

  /**
   * Populates put and delete for the occurrence specific interpreted columns, leaving any verbatim columns untouched.
   * @throws IOException
   */
  private void populateInterpretedPutDelete(HTableInterface occTable, byte[] cf, byte[] key, Put put, Delete del,
                                            Occurrence occ) throws IOException {

    HBaseHelper.writeField(FieldName.I_BASIS_OF_RECORD,
                           HBaseHelper.nullSafeBytes(occ.getBasisOfRecord()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_CLASS_KEY, HBaseHelper.nullSafeBytes(occ.getClassKey()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_CLASS, HBaseHelper.nullSafeBytes(occ.getClazz()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_DEPTH, HBaseHelper.nullSafeBytes(occ.getDepth()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_ELEVATION, HBaseHelper.nullSafeBytes(occ.getElevation()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_FAMILY, HBaseHelper.nullSafeBytes(occ.getFamily()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_FAMILY_KEY, HBaseHelper.nullSafeBytes(occ.getFamilyKey()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_GENUS, HBaseHelper.nullSafeBytes(occ.getGenus()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_GENUS_KEY, HBaseHelper.nullSafeBytes(occ.getGenusKey()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_KINGDOM, HBaseHelper.nullSafeBytes(occ.getKingdom()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_KINGDOM_KEY, HBaseHelper.nullSafeBytes(occ.getKingdomKey()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_DECIMAL_LATITUDE,
                           HBaseHelper.nullSafeBytes(occ.getDecimalLatitude()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_DECIMAL_LONGITUDE,
                           HBaseHelper.nullSafeBytes(occ.getDecimalLongitude()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_COUNTRY, HBaseHelper.nullSafeBytes(occ.getCountry()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_MODIFIED, HBaseHelper.nullSafeBytes(occ.getModified()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_MONTH, HBaseHelper.nullSafeBytes(occ.getMonth()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_TAXON_KEY, HBaseHelper.nullSafeBytes(occ.getTaxonKey()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_EVENT_DATE, HBaseHelper.nullSafeBytes(occ.getEventDate()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_ORDER, HBaseHelper.nullSafeBytes(occ.getOrder()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_ORDER_KEY, HBaseHelper.nullSafeBytes(occ.getOrderKey()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_PHYLUM, HBaseHelper.nullSafeBytes(occ.getPhylum()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_PHYLUM_KEY, HBaseHelper.nullSafeBytes(occ.getPhylumKey()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_SCIENTIFIC_NAME,
                           HBaseHelper.nullSafeBytes(occ.getScientificName()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_SPECIES, HBaseHelper.nullSafeBytes(occ.getSpecies()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_SPECIES_KEY, HBaseHelper.nullSafeBytes(occ.getSpeciesKey()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_YEAR, HBaseHelper.nullSafeBytes(occ.getYear()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_ELEVATION_ACC,
                           HBaseHelper.nullSafeBytes(occ.getElevationAccuracy()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_COORD_ACCURACY,
                           HBaseHelper.nullSafeBytes(occ.getCoordinateAccuracy()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_CONTINENT, HBaseHelper.nullSafeBytes(occ.getContinent()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_DATE_IDENTIFIED,
                           HBaseHelper.nullSafeBytes(occ.getDateIdentified()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_DAY, HBaseHelper.nullSafeBytes(occ.getDay()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_DEPTH_ACC, HBaseHelper.nullSafeBytes(occ.getDepthAccuracy()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_ESTAB_MEANS,
                           HBaseHelper.nullSafeBytes(occ.getEstablishmentMeans()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_INDIVIDUAL_COUNT,
                           HBaseHelper.nullSafeBytes(occ.getIndividualCount()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_LIFE_STAGE, HBaseHelper.nullSafeBytes(occ.getLifeStage()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_SEX, HBaseHelper.nullSafeBytes(occ.getSex()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_STATE_PROVINCE, HBaseHelper.nullSafeBytes(occ.getStateProvince()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_WATERBODY, HBaseHelper.nullSafeBytes(occ.getWaterBody()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_SUBGENUS, HBaseHelper.nullSafeBytes(occ.getSubgenus()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_SUBGENUS_KEY, HBaseHelper.nullSafeBytes(occ.getSubgenusKey()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_TYPE_STATUS, HBaseHelper.nullSafeBytes(occ.getTypeStatus()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_TYPIFIED_NAME, HBaseHelper.nullSafeBytes(occ.getTypifiedName()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_SPECIFIC_EPITHET,
                           HBaseHelper.nullSafeBytes(occ.getSpecificEpithet()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_INFRASPECIFIC_EPITHET,
                           HBaseHelper.nullSafeBytes(occ.getInfraspecificEpithet()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_GENERIC_NAME, HBaseHelper.nullSafeBytes(occ.getGenericName()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_TAXON_RANK, HBaseHelper.nullSafeBytes(occ.getTaxonRank()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_DIST_ABOVE_SURFACE,
                           HBaseHelper.nullSafeBytes(occ.getDistanceAboveSurface()), cf, put, del);
    HBaseHelper.writeField(FieldName.I_DIST_ABOVE_SURFACE_ACC,
                           HBaseHelper.nullSafeBytes(occ.getDistanceAboveSurfaceAccuracy()), cf, put, del);
    HBaseHelper.writeField(FieldName.LAST_INTERPRETED,
                           HBaseHelper.nullSafeBytes(occ.getLastInterpreted()), cf, put, del);

    // Identifiers
    deleteOldIdentifiers(occTable, occ.getKey(), cf);
    // OccurrenceIssues
    for (OccurrenceIssue issue : OccurrenceIssue.values()) {
      if (!occ.getIssues().contains(issue)) {
        del.deleteColumns(cf, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(issue).getColumnName()));
      }
    }

    if (occ.getIdentifiers() != null && !occ.getIdentifiers().isEmpty()) {
      addIdentifiersToPut(put, cf, occ.getIdentifiers());
    }

    for (OccurrenceIssue issue : occ.getIssues()) {
      put.add(cf, Bytes.toBytes(HBaseFieldUtil.getHBaseColumn(issue).getColumnName()), Bytes.toBytes(1));
    }
  }

  private void addIdentifiersToPut(Put put, byte[] columnFamily, Collection<Identifier> records) {
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

  private void deleteOldIdentifiers(HTableInterface occTable, int id, byte[] columnFamily) throws IOException {
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

  private void closeTable(HTableInterface table) {
    if (table != null) {
      try {
        table.close();
      } catch (IOException e) {
        LOG.warn("Couldn't return table to pool - continuing with possible memory leak", e);
      }
    }
  }
}
