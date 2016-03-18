package org.gbif.occurrence.persistence;

import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.util.ClassificationUtils;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.Rank;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.TermUtils;
import org.gbif.occurrence.common.config.OccHBaseConfiguration;
import org.gbif.occurrence.common.json.ExtensionSerDeserUtils;
import org.gbif.occurrence.common.json.MediaSerDeserUtils;
import org.gbif.occurrence.persistence.api.OccurrencePersistenceService;
import org.gbif.occurrence.persistence.hbase.Columns;
import org.gbif.occurrence.persistence.hbase.ExtResultReader;
import org.gbif.occurrence.persistence.hbase.RowUpdate;
import org.gbif.occurrence.persistence.util.OccurrenceBuilder;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.occurrence.persistence.util.ComparisonUtil.nullSafeEquals;

import static com.google.common.base.Preconditions.checkNotNull;


/**
 * An implementation of OccurrenceService and OccurrenceWriter for persisting and retrieving Occurrence objects in
 * HBase.
 */
@Singleton
public class OccurrencePersistenceServiceImpl implements OccurrencePersistenceService {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrencePersistenceServiceImpl.class);
  private static final int SCANNER_CACHE_SIZE = 50;
  private final String occurrenceTableName;
  private final HTablePool tablePool;

  @Inject
  public OccurrencePersistenceServiceImpl(OccHBaseConfiguration cfg, HTablePool tablePool) {
    this.occurrenceTableName = checkNotNull(cfg.occTable, "tableName can't be null");
    this.tablePool = checkNotNull(tablePool, "tablePool can't be null");
  }

  /**
   * Note that the returned fragment here is a String that holds the actual xml or json snippet for this occurrence,
   * and not the Fragment object that is used elsewhere.
   *
   * @param key that identifies an occurrence
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
      byte[] rawFragment = ExtResultReader.getBytes(result, Columns.column(GbifInternalTerm.fragment));
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

  <T extends VerbatimOccurrence> RowUpdate buildRowUpdate(T occ) {
    checkNotNull(occ, "occurrence can't be null");
    checkNotNull(occ.getKey(), "occurrence's key can't be null");

    HTableInterface occTable = null;
    RowUpdate upd = new RowUpdate(occ.getKey());
    try {
      occTable = tablePool.getTable(occurrenceTableName);
      if (occ instanceof Occurrence) {
        populateVerbatimPutDelete(occTable, upd, occ, false);
        populateInterpretedPutDelete(upd, (Occurrence) occ);
      } else {
        populateVerbatimPutDelete(occTable, upd, occ, true);
      }
    } catch (IOException e) {
      throw new ServiceUnavailableException("Could not access HBase", e);
    } finally {
      closeTable(occTable);
    }

    return upd;
  }

  private <T extends VerbatimOccurrence> void updateOcc(T occ) {
    checkNotNull(occ, "occurrence can't be null");
    checkNotNull(occ.getKey(), "occurrence's key can't be null");

    RowUpdate upd = buildRowUpdate(occ);

    HTableInterface occTable = null;
    try {
      occTable = tablePool.getTable(occurrenceTableName);
      upd.execute(occTable);
    } catch (IOException e) {
      throw new ServiceUnavailableException("Could not access HBase", e);
    } finally {
      closeTable(occTable);
    }
  }

  /**
   * Populates the put and delete for a verbatim record.
   *
   * @param deleteInterpretedVerbatimColumns if true deletes also the verbatim columns removed during interpretation
   *        (typically true when updating an Occurrence and false for
   *        VerbatimOccurrence)
   */
  private void populateVerbatimPutDelete(HTableInterface occTable, RowUpdate upd, VerbatimOccurrence occ,
    boolean deleteInterpretedVerbatimColumns) throws IOException {

    // adding the mutations to the HTable is quite expensive, hence worth all these comparisons
    VerbatimOccurrence oldVerb = getVerbatim(occ.getKey());

    // schedule delete of any fields that are on the oldVerb but not on the updated verb, but only if we've been
    // explicitly asked to delete empty term columns (deleteInterpretedVerbatimColumns) or if the column is one that is
    // used equally by the verbatim and interp occurrences (e.g. changes to catalogNumber on either of verb or interp
    // should be reflected here (it is not an InterpretedSourceTerm), but not something like verbatimLatitude
    // (which is an InterpretedSourceTerm)).
    //
    for (Term term : oldVerb.getVerbatimFields().keySet()) {
      if ((!occ.hasVerbatimField(term) || occ.getVerbatimField(term) == null)
        && (deleteInterpretedVerbatimColumns || !TermUtils.isInterpretedSourceTerm(term))) {
        upd.deleteVerbatimField(term);
      }
    }

    // schedule the updates for any verbatim field that has changed
    for (Map.Entry<Term, String> field : occ.getVerbatimFields().entrySet()) {
      String oldValue = oldVerb.getVerbatimField(field.getKey());
      String newValue = field.getValue();
      if (newValue != null && !newValue.equals(oldValue)) {
        upd.setVerbatimField(field.getKey(), field.getValue());
      }
    }

    if (!nullSafeEquals(oldVerb.getDatasetKey(), occ.getDatasetKey())) {
      upd.setInterpretedField(GbifTerm.datasetKey, occ.getDatasetKey());
    }
    if (!nullSafeEquals(oldVerb.getPublishingCountry(), occ.getPublishingCountry())) {
      upd.setInterpretedField(GbifTerm.publishingCountry, occ.getPublishingCountry());
    }
    if (!nullSafeEquals(oldVerb.getPublishingOrgKey(), occ.getPublishingOrgKey())) {
      upd.setInterpretedField(GbifInternalTerm.publishingOrgKey, occ.getPublishingOrgKey());
    }
    if (!nullSafeEquals(oldVerb.getProtocol(), occ.getProtocol())) {
      upd.setInterpretedField(GbifTerm.protocol, occ.getProtocol());
    }
    if (!nullSafeEquals(oldVerb.getLastCrawled(), occ.getLastCrawled())) {
      upd.setInterpretedField(GbifTerm.lastCrawled, occ.getLastCrawled());
    }
    if (!nullSafeEquals(oldVerb.getLastParsed(), occ.getLastParsed())) {
      upd.setInterpretedField(GbifTerm.lastParsed, occ.getLastParsed());
    }
    updateExtensions(oldVerb, occ, upd);
  }

  /**
   * Updates the extensions map of the newOcc object into the upd object.
   */
  private void updateExtensions(VerbatimOccurrence oldOcc, VerbatimOccurrence newOcc, RowUpdate upd)
    throws IOException {
    for (Extension extension : Extension.values()) {
      String newExtensions = getExtensionAsJson(newOcc, extension);
      if (!nullSafeEquals(getExtensionAsJson(oldOcc, extension), newExtensions)) {
        upd.setVerbatimExtension(extension, newExtensions);
      }
    }
  }

  /**
   * Returns the JSON object of verbatimOccurrence.getExtensions().get(extension).
   * If verbatimOccurrence is null or the requested extension doesn't exist returns null.
   */
  private String getExtensionAsJson(VerbatimOccurrence verbatimOccurrence, Extension extension) {
    String jsonExtensions = null;
    if (verbatimOccurrence.getExtensions() != null && verbatimOccurrence.getExtensions().containsKey(extension)) {
      jsonExtensions = ExtensionSerDeserUtils.toJson(verbatimOccurrence.getExtensions().get(extension));
    }
    return jsonExtensions;
  }

  /**
   * Populates put and delete for the occurrence specific interpreted columns, leaving any verbatim columns untouched.
   * TODO: use reflection to get values from the java properties now that we have corresponding terms?
   */
  private void populateInterpretedPutDelete(RowUpdate upd, Occurrence occ)
    throws IOException {

    Occurrence oldOcc = get(occ.getKey());

    if (!nullSafeEquals(oldOcc.getBasisOfRecord(), occ.getBasisOfRecord())) {
      upd.setInterpretedField(DwcTerm.basisOfRecord, occ.getBasisOfRecord());
    }
    if (!nullSafeEquals(oldOcc.getTaxonKey(), occ.getTaxonKey())) {
      upd.setInterpretedField(GbifTerm.taxonKey, occ.getTaxonKey());
    }
    if (!nullSafeEquals(oldOcc.getKingdom(), occ.getKingdom())) {
      updateRank(upd, occ, Rank.KINGDOM);
    }
    if (!nullSafeEquals(oldOcc.getPhylum(), occ.getPhylum())) {
      updateRank(upd, occ, Rank.PHYLUM);
    }
    if (!nullSafeEquals(oldOcc.getClazz(), occ.getClazz())) {
      updateRank(upd, occ, Rank.CLASS);
    }
    if (!nullSafeEquals(oldOcc.getOrder(), occ.getOrder())) {
      updateRank(upd, occ, Rank.ORDER);
    }
    if (!nullSafeEquals(oldOcc.getFamily(), occ.getFamily())) {
      updateRank(upd, occ, Rank.FAMILY);
    }
    if (!nullSafeEquals(oldOcc.getGenus(), occ.getGenus())) {
      updateRank(upd, occ, Rank.GENUS);
    }
    if (!nullSafeEquals(oldOcc.getSubgenus(), occ.getSubgenus())) {
      updateRank(upd, occ, Rank.SUBGENUS);
    }
    if (!nullSafeEquals(oldOcc.getSpecies(), occ.getSpecies())) {
      updateRank(upd, occ, Rank.SPECIES);
    }
    if (!nullSafeEquals(oldOcc.getDepth(), occ.getDepth())) {
      upd.setInterpretedField(GbifTerm.depth, occ.getDepth());
    }
    if (!nullSafeEquals(oldOcc.getDepthAccuracy(), occ.getDepthAccuracy())) {
      upd.setInterpretedField(GbifTerm.depthAccuracy, occ.getDepthAccuracy());
    }
    if (!nullSafeEquals(oldOcc.getElevation(), occ.getElevation())) {
      upd.setInterpretedField(GbifTerm.elevation, occ.getElevation());
    }
    if (!nullSafeEquals(oldOcc.getElevationAccuracy(), occ.getElevationAccuracy())) {
      upd.setInterpretedField(GbifTerm.elevationAccuracy, occ.getElevationAccuracy());
    }
    if (!nullSafeEquals(oldOcc.getDecimalLatitude(), occ.getDecimalLatitude())) {
      upd.setInterpretedField(DwcTerm.decimalLatitude, occ.getDecimalLatitude());
    }
    if (!nullSafeEquals(oldOcc.getDecimalLongitude(), occ.getDecimalLongitude())) {
      upd.setInterpretedField(DwcTerm.decimalLongitude, occ.getDecimalLongitude());
    }
    if (!nullSafeEquals(oldOcc.getCountry(), occ.getCountry())) {
      upd.setInterpretedField(DwcTerm.countryCode, occ.getCountry());
    }
    if (!nullSafeEquals(oldOcc.getModified(), occ.getModified())) {
      upd.setInterpretedField(DcTerm.modified, occ.getModified());
    }
    if (!nullSafeEquals(oldOcc.getEventDate(), occ.getEventDate())) {
      upd.setInterpretedField(DwcTerm.eventDate, occ.getEventDate());
    }
    if (!nullSafeEquals(oldOcc.getYear(), occ.getYear())) {
      upd.setInterpretedField(DwcTerm.year, occ.getYear());
    }
    if (!nullSafeEquals(oldOcc.getMonth(), occ.getMonth())) {
      upd.setInterpretedField(DwcTerm.month, occ.getMonth());
    }
    if (!nullSafeEquals(oldOcc.getDay(), occ.getDay())) {
      upd.setInterpretedField(DwcTerm.day, occ.getDay());
    }
    if (!nullSafeEquals(oldOcc.getScientificName(), occ.getScientificName())) {
      upd.setInterpretedField(DwcTerm.scientificName, occ.getScientificName());
    }
    if (!nullSafeEquals(oldOcc.getGenericName(), occ.getGenericName())) {
      upd.setInterpretedField(GbifTerm.genericName, occ.getGenericName());
    }
    if (!nullSafeEquals(oldOcc.getSpecificEpithet(), occ.getSpecificEpithet())) {
      upd.setInterpretedField(DwcTerm.specificEpithet, occ.getSpecificEpithet());
    }
    if (!nullSafeEquals(oldOcc.getInfraspecificEpithet(), occ.getInfraspecificEpithet())) {
      upd.setInterpretedField(DwcTerm.infraspecificEpithet, occ.getInfraspecificEpithet());
    }
    if (!nullSafeEquals(oldOcc.getTaxonRank(), occ.getTaxonRank())) {
      upd.setInterpretedField(DwcTerm.taxonRank, occ.getTaxonRank());
    }
    //we only store one digit for coordinateUncertaintyInMeters
    if (!nullSafeEquals(oldOcc.getCoordinateUncertaintyInMeters(), occ.getCoordinateUncertaintyInMeters())) {
      upd.setInterpretedField(DwcTerm.coordinateUncertaintyInMeters, nullSafeRoundHalfUp(occ.getCoordinateUncertaintyInMeters(), 1));
    }
    if (!nullSafeEquals(oldOcc.getCoordinatePrecision(), occ.getCoordinatePrecision())) {
      upd.setInterpretedField(DwcTerm.coordinatePrecision, occ.getCoordinatePrecision());
    }
    // should we remove it ?
    if (!nullSafeEquals(oldOcc.getCoordinateAccuracy(), occ.getCoordinateAccuracy())) {
      upd.setInterpretedField(GbifTerm.coordinateAccuracy, occ.getCoordinateAccuracy());
    }
    if (!nullSafeEquals(oldOcc.getContinent(), occ.getContinent())) {
      upd.setInterpretedField(DwcTerm.continent, occ.getContinent());
    }
    if (!nullSafeEquals(oldOcc.getDateIdentified(), occ.getDateIdentified())) {
      upd.setInterpretedField(DwcTerm.dateIdentified, occ.getDateIdentified());
    }
    if (!nullSafeEquals(oldOcc.getEstablishmentMeans(), occ.getEstablishmentMeans())) {
      upd.setInterpretedField(DwcTerm.establishmentMeans, occ.getEstablishmentMeans());
    }
    if (!nullSafeEquals(oldOcc.getIndividualCount(), occ.getIndividualCount())) {
      upd.setInterpretedField(DwcTerm.individualCount, occ.getIndividualCount());
    }
    if (!nullSafeEquals(oldOcc.getLifeStage(), occ.getLifeStage())) {
      upd.setInterpretedField(DwcTerm.lifeStage, occ.getLifeStage());
    }
    if (!nullSafeEquals(oldOcc.getSex(), occ.getSex())) {
      upd.setInterpretedField(DwcTerm.sex, occ.getSex());
    }
    if (!nullSafeEquals(oldOcc.getStateProvince(), occ.getStateProvince())) {
      upd.setInterpretedField(DwcTerm.stateProvince, occ.getStateProvince());
    }
    if (!nullSafeEquals(oldOcc.getWaterBody(), occ.getWaterBody())) {
      upd.setInterpretedField(DwcTerm.waterBody, occ.getWaterBody());
    }
    if (!nullSafeEquals(oldOcc.getTypeStatus(), occ.getTypeStatus())) {
      upd.setInterpretedField(DwcTerm.typeStatus, occ.getTypeStatus());
    }
    if (!nullSafeEquals(oldOcc.getTypifiedName(), occ.getTypifiedName())) {
      upd.setInterpretedField(GbifTerm.typifiedName, occ.getTypifiedName());
    }
    if (!nullSafeEquals(oldOcc.getLastInterpreted(), occ.getLastInterpreted())) {
      upd.setInterpretedField(GbifTerm.lastInterpreted, occ.getLastInterpreted());
    }
    if (!nullSafeEquals(oldOcc.getReferences(), occ.getReferences())) {
      upd.setInterpretedField(DcTerm.references, occ.getReferences());
    }

    // Multimedia extension
    String newMediaJson = MediaSerDeserUtils.toJson(occ.getMedia());
    if (!nullSafeEquals(MediaSerDeserUtils.toJson(oldOcc.getMedia()), newMediaJson)) {
      upd.setInterpretedExtension(Extension.MULTIMEDIA, newMediaJson);
    }

    // OccurrenceIssues
    for (OccurrenceIssue issue : oldOcc.getIssues()) {
      if (!occ.getIssues().contains(issue)) {
        upd.setField(Columns.column(issue), null);
      }
    }
    for (OccurrenceIssue issue : occ.getIssues()) {
      if (!oldOcc.getIssues().contains(issue)) {
        upd.setField(Columns.column(issue), Bytes.toBytes(1));
      }
    }
  }

  private void updateRank(RowUpdate upd, Occurrence occ, Rank r) throws IOException {
    upd.setInterpretedField(OccurrenceBuilder.rank2taxonTerm.get(r), ClassificationUtils.getHigherRank(occ, r));
    upd.setInterpretedField(OccurrenceBuilder.rank2KeyTerm.get(r), ClassificationUtils.getHigherRankKey(occ, r));
  }

  /**
   * Used to round (with half up) a BigDecimal to only keep a certain number of digit(s).
   * @param value
   * @param scale
   * @return
   */
  private BigDecimal nullSafeRoundHalfUp(BigDecimal value, int scale){
    if(value == null){
      return null;
    }
    return value.setScale(scale, BigDecimal.ROUND_HALF_UP);
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
