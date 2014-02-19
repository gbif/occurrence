package org.gbif.occurrence.persistence;

import org.gbif.dwc.terms.GbifTerm;
import org.gbif.occurrence.persistence.api.DatasetDeletionService;
import org.gbif.occurrence.persistence.api.OccurrenceKeyPersistenceService;
import org.gbif.occurrence.persistence.api.OccurrencePersistenceService;
import org.gbif.occurrence.persistence.hbase.ColumnUtil;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An implementation of the DatasetDeletionService for deletion of datasets in HBase.
 */
@Singleton
public class DatasetDeletionServiceImpl implements DatasetDeletionService {

  private static final Logger LOG = LoggerFactory.getLogger(DatasetDeletionServiceImpl.class);

  private final OccurrencePersistenceService occurrenceService;
  private final OccurrenceKeyPersistenceService occurrenceKeyService;

  @Inject
  public DatasetDeletionServiceImpl(OccurrencePersistenceService occurrenceService,
    OccurrenceKeyPersistenceService occurrenceKeyService) {
    this.occurrenceService = checkNotNull(occurrenceService, "occurrenceService can't be null");
    this.occurrenceKeyService = checkNotNull(occurrenceKeyService, "occurrenceKeyService can't be null");
  }

  @Override
  public void deleteDataset(UUID datasetKey) {
    checkNotNull(datasetKey, "datasetKey can't be null");
    LOG.debug("Deleting dataset for datasetKey [{}]", datasetKey);

    // lookup all occurrence ids from lookup table using dataset prefix

    //
    deleteByColumn(Bytes.toBytes(datasetKey.toString()), GbifTerm.datasetKey);
    LOG.debug("Completed deletion of dataset for datasetKey [{}]", datasetKey);
  }

  /**
   * Deletes both the secondary indexes ("lookups") as well as the occurrence proper for the scan that matches the
   * given column and value. Note any exceptions thrown during the deletions will cause this method to fail, leaving
   * deletions in an incomplete state.
   *
   * @param columnValue value to match
   * @param column interpreted column on which to match values
   */
  private void deleteByColumn(byte[] columnValue, GbifTerm column) {
    LOG.debug("Starting delete by column for [{}]", column);
    int deleteCount = 0;
    Iterator<Integer> keyIterator = occurrenceService.getKeysByColumn(columnValue, ColumnUtil.getColumn(column));
    List<Integer> keys = Lists.newArrayList();
    while (keyIterator.hasNext()) {
      int key = keyIterator.next();
      // TODO: this is critical, but causes extreme performance problems (full scan of lookups per deleted key)
      occurrenceKeyService.deleteKey(key, null);
      keys.add(key);
      if (keys.size() % 100000 == 0) {
        LOG.debug("Writing batch of [{}] deletes", keys.size());
        occurrenceService.delete(keys);
        deleteCount += keys.size();
        keys = Lists.newArrayList();
      }
    }
    LOG.debug("Writing batch of [{}] deletes", keys.size());
    occurrenceService.delete(keys);
    deleteCount += keys.size();
    LOG.debug("Finished delete by column for [{}] giving [{}] total rows deleted", column, deleteCount);
  }
}
