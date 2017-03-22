package org.gbif.occurrence.persistence;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.util.ClassificationUtils;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.Rank;
import org.gbif.dwc.terms.*;
import org.gbif.occurrence.common.TermUtils;
import org.gbif.occurrence.common.config.OccHBaseConfiguration;
import org.gbif.occurrence.common.json.ExtensionSerDeserUtils;
import org.gbif.occurrence.common.json.MediaSerDeserUtils;
import org.gbif.occurrence.persistence.api.OccurrencePersistenceService;
import org.gbif.occurrence.persistence.hbase.Columns;
import org.gbif.occurrence.persistence.hbase.ExtResultReader;
import org.gbif.occurrence.persistence.hbase.RowUpdate;
import org.gbif.occurrence.persistence.util.OccurrenceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
  private final Connection connection;

  @Inject
  public OccurrencePersistenceServiceImpl(OccHBaseConfiguration cfg, Connection connection) {
    occurrenceTableName = checkNotNull(cfg.occTable, "tableName can't be null");
    this.connection = checkNotNull(connection, "connection can't be null");
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
    try (Table table = connection.getTable(TableName.valueOf(occurrenceTableName))) {
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
    try (Table table = connection.getTable(TableName.valueOf(occurrenceTableName)))  {
      Get get = new Get(Bytes.toBytes(key));
      Result result = table.get(get);
      if (result == null || result.isEmpty()) {
        LOG.debug("Couldn't find occurrence for key [{}], returning null", key);
        return null;
      }
      verb = OccurrenceBuilder.buildVerbatimOccurrence(result);
    } catch (IOException e) {
      throw new ServiceUnavailableException("Could not read from HBase", e);
    }

    return verb;
  }

  @Override
  public Occurrence get(@Nullable Integer key) {
    if (key == null) {
      return null;
    }
    Occurrence occ = null;
    try (Table table = connection.getTable(TableName.valueOf(occurrenceTableName))) {
      Get get = new Get(Bytes.toBytes(key));
      Result result = table.get(get);
      if (result == null || result.isEmpty()) {
        LOG.debug("Couldn't find occurrence for key [{}], returning null", key);
        return null;
      }
      occ = OccurrenceBuilder.buildOccurrence(result);
    } catch (Exception e) {
      throw new ServiceUnavailableException("Could not read from HBase", e);
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

    return new OccurrenceKeyIterator(connection, occurrenceTableName, scan);
  }

  @Override
  public void update(VerbatimOccurrence occurrence) {
    updateOcc(occurrence);
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

    try (Table table = connection.getTable(TableName.valueOf(occurrenceTableName)))  {
      List<Delete> deletes = Lists.newArrayListWithExpectedSize(occurrenceKeys.size());
      for (Integer occurrenceKey : occurrenceKeys) {
        if (occurrenceKey != null) {
          deletes.add(new Delete(Bytes.toBytes(occurrenceKey)));
        }
      }
      LOG.debug("Deleting [{}] occurrences", occurrenceKeys.size());
      table.delete(deletes);
    } catch (IOException e) {
      throw new ServiceUnavailableException("Could not access HBase", e);
    }
  }

  <T extends VerbatimOccurrence> RowUpdate buildRowUpdate(T occ) {
    checkNotNull(occ, "occurrence can't be null");
    checkNotNull(occ.getKey(), "occurrence's key can't be null");

    RowUpdate upd = new RowUpdate(occ.getKey());
    try (Table table = connection.getTable(TableName.valueOf(occurrenceTableName))) {
      if (occ instanceof Occurrence) {
        populateVerbatimPutDelete(table, upd, occ, false);
        populateInterpretedPutDelete(upd, (Occurrence) occ);
      } else {
        populateVerbatimPutDelete(table, upd, occ, true);
      }
    } catch (IOException e) {
      throw new ServiceUnavailableException("Could not access HBase", e);
    }

    return upd;
  }

  private <T extends VerbatimOccurrence> void updateOcc(T occ) {
    checkNotNull(occ, "occurrence can't be null");
    checkNotNull(occ.getKey(), "occurrence's key can't be null");

    RowUpdate upd = buildRowUpdate(occ);

    try (Table table = connection.getTable(TableName.valueOf(occurrenceTableName))) {
      upd.execute(table);
    } catch (IOException e) {
      throw new ServiceUnavailableException("Could not access HBase", e);
    }
  }

  /**
   * Populates the put and delete for a verbatim record.
   *
   * @param deleteInterpretedVerbatimColumns if true deletes also the verbatim columns removed during interpretation
   *        (typically true when updating an Occurrence and false for
   *        VerbatimOccurrence)
   */
  private void populateVerbatimPutDelete(Table occTable, RowUpdate upd, VerbatimOccurrence occ,
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

    if (!Objects.equals(oldVerb.getDatasetKey(), occ.getDatasetKey())) {
      upd.setInterpretedField(GbifTerm.datasetKey, occ.getDatasetKey());
    }
    if (!Objects.equals(oldVerb.getPublishingCountry(), occ.getPublishingCountry())) {
      upd.setInterpretedField(GbifTerm.publishingCountry, occ.getPublishingCountry());
    }
    if (!Objects.equals(oldVerb.getPublishingOrgKey(), occ.getPublishingOrgKey())) {
      upd.setInterpretedField(GbifInternalTerm.publishingOrgKey, occ.getPublishingOrgKey());
    }
    if (!Objects.equals(oldVerb.getProtocol(), occ.getProtocol())) {
      upd.setInterpretedField(GbifTerm.protocol, occ.getProtocol());
    }
    if (!Objects.equals(oldVerb.getLastCrawled(), occ.getLastCrawled())) {
      upd.setInterpretedField(GbifTerm.lastCrawled, occ.getLastCrawled());
    }
    if (!Objects.equals(oldVerb.getLastParsed(), occ.getLastParsed())) {
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
      if (!Objects.equals(getExtensionAsJson(oldOcc, extension), newExtensions)) {
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
  private void populateInterpretedPutDelete(RowUpdate upd, Occurrence occ) throws IOException {

    Occurrence oldOcc = get(occ.getKey());

    if (!Objects.equals(oldOcc.getBasisOfRecord(), occ.getBasisOfRecord())) {
      upd.setInterpretedField(DwcTerm.basisOfRecord, occ.getBasisOfRecord());
    }
    if (!Objects.equals(oldOcc.getTaxonKey(), occ.getTaxonKey())) {
      upd.setInterpretedField(GbifTerm.taxonKey, occ.getTaxonKey());
    }
    for (Rank r : Rank.DWC_RANKS) {
      if (!Objects.equals(oldOcc.getHigherRankKey(r), occ.getHigherRankKey(r))) {
        upd.setInterpretedField(OccurrenceBuilder.rank2KeyTerm.get(r), ClassificationUtils.getHigherRankKey(occ, r));
      }
      if (!Objects.equals(oldOcc.getHigherRank(r), occ.getHigherRank(r))) {
        upd.setInterpretedField(OccurrenceBuilder.rank2taxonTerm.get(r), ClassificationUtils.getHigherRank(occ, r));
      }
    }
    if (!Objects.equals(oldOcc.getDepth(), occ.getDepth())) {
      upd.setInterpretedField(GbifTerm.depth, occ.getDepth());
    }
    if (!Objects.equals(oldOcc.getDepthAccuracy(), occ.getDepthAccuracy())) {
      upd.setInterpretedField(GbifTerm.depthAccuracy, occ.getDepthAccuracy());
    }
    if (!Objects.equals(oldOcc.getElevation(), occ.getElevation())) {
      upd.setInterpretedField(GbifTerm.elevation, occ.getElevation());
    }
    if (!Objects.equals(oldOcc.getElevationAccuracy(), occ.getElevationAccuracy())) {
      upd.setInterpretedField(GbifTerm.elevationAccuracy, occ.getElevationAccuracy());
    }
    if (!Objects.equals(oldOcc.getDecimalLatitude(), occ.getDecimalLatitude())) {
      upd.setInterpretedField(DwcTerm.decimalLatitude, occ.getDecimalLatitude());
    }
    if (!Objects.equals(oldOcc.getDecimalLongitude(), occ.getDecimalLongitude())) {
      upd.setInterpretedField(DwcTerm.decimalLongitude, occ.getDecimalLongitude());
    }
    if (!Objects.equals(oldOcc.getCountry(), occ.getCountry())) {
      upd.setInterpretedField(DwcTerm.countryCode, occ.getCountry());
    }
    if (!Objects.equals(oldOcc.getModified(), occ.getModified())) {
      upd.setInterpretedField(DcTerm.modified, occ.getModified());
    }
    if (!Objects.equals(oldOcc.getEventDate(), occ.getEventDate())) {
      upd.setInterpretedField(DwcTerm.eventDate, occ.getEventDate());
    }
    if (!Objects.equals(oldOcc.getYear(), occ.getYear())) {
      upd.setInterpretedField(DwcTerm.year, occ.getYear());
    }
    if (!Objects.equals(oldOcc.getMonth(), occ.getMonth())) {
      upd.setInterpretedField(DwcTerm.month, occ.getMonth());
    }
    if (!Objects.equals(oldOcc.getDay(), occ.getDay())) {
      upd.setInterpretedField(DwcTerm.day, occ.getDay());
    }
    if (!Objects.equals(oldOcc.getScientificName(), occ.getScientificName())) {
      upd.setInterpretedField(DwcTerm.scientificName, occ.getScientificName());
    }
    if (!Objects.equals(oldOcc.getGenericName(), occ.getGenericName())) {
      upd.setInterpretedField(GbifTerm.genericName, occ.getGenericName());
    }
    if (!Objects.equals(oldOcc.getSpecificEpithet(), occ.getSpecificEpithet())) {
      upd.setInterpretedField(DwcTerm.specificEpithet, occ.getSpecificEpithet());
    }
    if (!Objects.equals(oldOcc.getInfraspecificEpithet(), occ.getInfraspecificEpithet())) {
      upd.setInterpretedField(DwcTerm.infraspecificEpithet, occ.getInfraspecificEpithet());
    }
    if (!Objects.equals(oldOcc.getTaxonRank(), occ.getTaxonRank())) {
      upd.setInterpretedField(DwcTerm.taxonRank, occ.getTaxonRank());
    }
    if (!Objects.equals(oldOcc.getCoordinateUncertaintyInMeters(), occ.getCoordinateUncertaintyInMeters())) {
      upd.setInterpretedField(DwcTerm.coordinateUncertaintyInMeters, occ.getCoordinateUncertaintyInMeters());
    }
    if (!Objects.equals(oldOcc.getCoordinatePrecision(), occ.getCoordinatePrecision())) {
      upd.setInterpretedField(DwcTerm.coordinatePrecision, occ.getCoordinatePrecision());
    }
    if (!Objects.equals(oldOcc.getContinent(), occ.getContinent())) {
      upd.setInterpretedField(DwcTerm.continent, occ.getContinent());
    }
    if (!Objects.equals(oldOcc.getDateIdentified(), occ.getDateIdentified())) {
      upd.setInterpretedField(DwcTerm.dateIdentified, occ.getDateIdentified());
    }
    if (!Objects.equals(oldOcc.getEstablishmentMeans(), occ.getEstablishmentMeans())) {
      upd.setInterpretedField(DwcTerm.establishmentMeans, occ.getEstablishmentMeans());
    }
    if (!Objects.equals(oldOcc.getIndividualCount(), occ.getIndividualCount())) {
      upd.setInterpretedField(DwcTerm.individualCount, occ.getIndividualCount());
    }
    if (!Objects.equals(oldOcc.getLifeStage(), occ.getLifeStage())) {
      upd.setInterpretedField(DwcTerm.lifeStage, occ.getLifeStage());
    }
    if (!Objects.equals(oldOcc.getSex(), occ.getSex())) {
      upd.setInterpretedField(DwcTerm.sex, occ.getSex());
    }
    if (!Objects.equals(oldOcc.getStateProvince(), occ.getStateProvince())) {
      upd.setInterpretedField(DwcTerm.stateProvince, occ.getStateProvince());
    }
    if (!Objects.equals(oldOcc.getWaterBody(), occ.getWaterBody())) {
      upd.setInterpretedField(DwcTerm.waterBody, occ.getWaterBody());
    }
    if (!Objects.equals(oldOcc.getTypeStatus(), occ.getTypeStatus())) {
      upd.setInterpretedField(DwcTerm.typeStatus, occ.getTypeStatus());
    }
    if (!Objects.equals(oldOcc.getTypifiedName(), occ.getTypifiedName())) {
      upd.setInterpretedField(GbifTerm.typifiedName, occ.getTypifiedName());
    }
    if (!Objects.equals(oldOcc.getLastInterpreted(), occ.getLastInterpreted())) {
      upd.setInterpretedField(GbifTerm.lastInterpreted, occ.getLastInterpreted());
    }
    if (!Objects.equals(oldOcc.getReferences(), occ.getReferences())) {
      upd.setInterpretedField(DcTerm.references, occ.getReferences());
    }
    if (!Objects.equals(oldOcc.getLicense(), occ.getLicense())) {
      upd.setInterpretedField(DcTerm.license, occ.getLicense());
    }

    // Multimedia extension
    String newMediaJson = MediaSerDeserUtils.toJson(occ.getMedia());
    if (!Objects.equals(MediaSerDeserUtils.toJson(oldOcc.getMedia()), newMediaJson)) {
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

  /**
   * Used to round (with half up) a BigDecimal to only keep a certain number of digit(s).
   */
  private static BigDecimal nullSafeRoundHalfUp(BigDecimal value, int scale){
    if (value == null) {
      return null;
    }
    return value.setScale(scale, BigDecimal.ROUND_HALF_UP);
  }

}
