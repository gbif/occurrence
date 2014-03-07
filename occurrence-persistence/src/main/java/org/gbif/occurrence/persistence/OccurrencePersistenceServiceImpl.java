package org.gbif.occurrence.persistence;

import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.api.model.common.Identifier;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.util.ClassificationUtils;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.Rank;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;
import org.gbif.occurrence.persistence.api.OccurrencePersistenceService;
import org.gbif.occurrence.persistence.hbase.Columns;
import org.gbif.occurrence.persistence.hbase.ExtResultReader;
import org.gbif.occurrence.persistence.hbase.RowUpdate;
import org.gbif.occurrence.persistence.util.OccurrenceBuilder;

import java.io.IOException;
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
      byte[] rawFragment = ExtResultReader.getBytes(result,
                              Columns.column(GbifInternalTerm.fragment));
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
  public Iterator<Integer> getKeysByColumn(byte[] columnValue, String columnName) {
    byte[] col = Bytes.toBytes(columnName);
    Scan scan = new Scan();
    scan.setBatch(SCANNER_BATCH_SIZE);
    scan.setCaching(SCANNER_CACHE_SIZE);
    scan.addColumn(Columns.CF, col);
    scan.setFilter(new SingleColumnValueFilter(Columns.CF, col, CompareFilter.CompareOp.EQUAL, columnValue));

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
      RowUpdate upd = new RowUpdate(occ.getKey());

      populateVerbatimPutDelete(occTable, upd, occ, occ instanceof Occurrence);

      // add interpreted occurrence terms
      if (occ instanceof Occurrence) {
        populateInterpretedPutDelete(occTable, upd, (Occurrence) occ);
      }

      upd.execute(occTable);

    } catch (IOException e) {
      throw new ServiceUnavailableException("Could not access HBase", e);
    } finally {
      closeTable(occTable);
    }
  }

  /**
   * Populates the put and delete for a verbatim record.
   * @param deleteInterpretedVerbatimColumns if true deletes also the verbatim columns removed during interpretation
   * @throws IOException
   */
  private void populateVerbatimPutDelete(HTableInterface occTable, RowUpdate upd, VerbatimOccurrence occ,
                                         boolean deleteInterpretedVerbatimColumns) throws IOException {

    // start by scheduling deletion of all terms not in the occ
    Get get = new Get(upd.getKey());
    Result row = occTable.get(get);
    for (KeyValue kv : row.raw()) {
      Term term = Columns.termFromVerbatimColumn(kv.getQualifier());
      if (term != null) {
        if (occ.getVerbatimField(term) == null &&
            // only remove the interpreted verbatim terms if explicitly requested
            (deleteInterpretedVerbatimColumns || !TermUtils.isInterpretedSourceTerm(term)) ) {
          upd.deleteField(kv.getQualifier());
        }
      }
    }

    // put all non null verbatim terms, and any internal terms, from the map
    for (Map.Entry<Term, String> entry : occ.getVerbatimFields().entrySet()) {
      if (!(entry.getKey() instanceof GbifInternalTerm) && entry.getValue() != null) {
        upd.setVerbatimField(entry.getKey(), entry.getValue());
      }
    }

    upd.setInterpretedField(GbifTerm.datasetKey, occ.getDatasetKey());
    upd.setInterpretedField(GbifTerm.publishingCountry, occ.getPublishingCountry());
    upd.setInterpretedField(GbifInternalTerm.publishingOrgKey, occ.getPublishingOrgKey());
    upd.setInterpretedField(GbifTerm.protocol, occ.getProtocol());
    upd.setInterpretedField(GbifTerm.lastCrawled, occ.getLastCrawled());
    upd.setInterpretedField(GbifTerm.lastParsed, occ.getLastParsed());
  }

  /**
   * Populates put and delete for the occurrence specific interpreted columns, leaving any verbatim columns untouched.
   * TODO: use reflection to get values from the java properties now that we have corresponding terms?
   * @throws IOException
   */
  private void populateInterpretedPutDelete(HTableInterface occTable, RowUpdate upd, Occurrence occ) throws IOException {

    upd.setInterpretedField(DwcTerm.basisOfRecord, occ.getBasisOfRecord());
    upd.setInterpretedField(GbifTerm.taxonKey, occ.getTaxonKey());
    for (Rank r : Rank.DWC_RANKS) {
      upd.setInterpretedField(OccurrenceBuilder.rank2taxonTerm.get(r), ClassificationUtils.getHigherRank(occ, r));
      upd.setInterpretedField(OccurrenceBuilder.rank2KeyTerm.get(r), ClassificationUtils.getHigherRankKey(occ, r));
    }
    upd.setInterpretedField(GbifTerm.depth, occ.getDepth());
    upd.setInterpretedField(GbifTerm.depthAccuracy, occ.getDepthAccuracy());
    upd.setInterpretedField(GbifTerm.elevation, occ.getElevation());
    upd.setInterpretedField(GbifTerm.elevationAccuracy, occ.getElevationAccuracy());
    upd.setInterpretedField(DwcTerm.decimalLatitude, occ.getDecimalLatitude());
    upd.setInterpretedField(DwcTerm.decimalLongitude, occ.getDecimalLongitude());
    upd.setInterpretedField(DwcTerm.countryCode, occ.getCountry());
    upd.setInterpretedField(DcTerm.modified, occ.getModified());
    upd.setInterpretedField(DwcTerm.eventDate, occ.getEventDate());
    upd.setInterpretedField(DwcTerm.year, occ.getYear());
    upd.setInterpretedField(DwcTerm.month, occ.getMonth());
    upd.setInterpretedField(DwcTerm.day, occ.getDay());
    upd.setInterpretedField(DwcTerm.scientificName, occ.getScientificName());
    upd.setInterpretedField(DwcTerm.genericName, occ.getGenericName());
    upd.setInterpretedField(DwcTerm.specificEpithet, occ.getSpecificEpithet());
    upd.setInterpretedField(DwcTerm.infraspecificEpithet, occ.getInfraspecificEpithet());
    upd.setInterpretedField(DwcTerm.taxonRank, occ.getTaxonRank());
    upd.setInterpretedField(GbifTerm.coordinateAccuracy, occ.getCoordinateAccuracy());
    upd.setInterpretedField(DwcTerm.continent, occ.getContinent());
    upd.setInterpretedField(DwcTerm.dateIdentified, occ.getDateIdentified());
    upd.setInterpretedField(DwcTerm.establishmentMeans, occ.getEstablishmentMeans());
    upd.setInterpretedField(DwcTerm.individualCount, occ.getIndividualCount());
    upd.setInterpretedField(DwcTerm.lifeStage, occ.getLifeStage());
    upd.setInterpretedField(DwcTerm.sex, occ.getSex());
    upd.setInterpretedField(DwcTerm.stateProvince, occ.getStateProvince());
    upd.setInterpretedField(DwcTerm.waterBody, occ.getWaterBody());
    upd.setInterpretedField(DwcTerm.typeStatus, occ.getTypeStatus());
    upd.setInterpretedField(DwcTerm.typifiedName, occ.getTypifiedName());
    upd.setInterpretedField(GbifTerm.lastInterpreted, occ.getLastInterpreted());

    // Identifiers
    deleteOldIdentifiers(occTable, occ.getKey());
    if (occ.getIdentifiers() != null && !occ.getIdentifiers().isEmpty()) {
      upd.setInterpretedField(GbifInternalTerm.identifierCount, occ.getIdentifiers().size());
      int count = 0;
      for (Identifier record : occ.getIdentifiers()) {
        upd.setField(Columns.idColumn(count), Bytes.toBytes(record.getIdentifier()));
        upd.setField(Columns.idTypeColumn(count), Bytes.toBytes(record.getType().toString()));
        count++;
      }
    }

    // OccurrenceIssues
    for (OccurrenceIssue issue : OccurrenceIssue.values()) {
      upd.setField(Columns.column(issue), occ.getIssues().contains(issue) ? Bytes.toBytes(1) : null);
    }
  }

  /**
   * removes id columns and sets id count to zero
   */
  private void deleteOldIdentifiers(HTableInterface occTable, int id) throws IOException {
    final String idCountColumn = Columns.column(GbifInternalTerm.identifierCount);

    Get get = new Get(Bytes.toBytes(id));
    get.addColumn(Columns.CF, Bytes.toBytes(idCountColumn));
    Result result = occTable.get(get);
    Integer maxCount = ExtResultReader.getInteger(result, idCountColumn);
    if (maxCount != null && maxCount > 0) {
      Delete delete = new Delete(Bytes.toBytes(id));
      for (int count = 0; count < maxCount; count++) {
        delete.deleteColumn(Columns.CF, Bytes.toBytes(Columns.idColumn(count)));
        delete.deleteColumn(Columns.CF, Bytes.toBytes(Columns.idTypeColumn(count)));
      }
      occTable.delete(delete);
      // set count to 0
      Put put = new Put(Bytes.toBytes(id));
      put.add(Columns.CF, Bytes.toBytes(idCountColumn), Bytes.toBytes(0));
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
