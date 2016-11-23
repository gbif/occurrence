package org.gbif.occurrence.cli.registry.sync;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Organization;
import org.gbif.common.messaging.api.messages.DeleteDatasetOccurrencesMessage;
import org.gbif.common.messaging.api.messages.OccurrenceDeletionReason;
import org.gbif.common.messaging.api.messages.OccurrenceMutatedMessage;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A mapreduce Mapper that synchronizes occurrences with the registry. It checks for changes detected by
 * {@link RegistryBasedOccurrenceMutator} and dataset deletions. For organization changes the new values are written
 * to the occurrence HBase table via occurrence persistence, and then an OccurrenceMutatedMessage is sent. For dataset
 * deletions a DeleteDatasetMessage is sent.
 */
public class OccurrenceScanMapper extends AbstractOccurrenceRegistryMapper {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceScanMapper.class);

  private static final Set<UUID> DEAD_DATASETS = Sets.newHashSet();
  private static final Set<UUID> UNCHANGED_DATASETS = Sets.newHashSet();
  private static final Map<UUID, Organization> DATASET_TO_OWNING_ORG = Maps.newHashMap();

  private int numRecords = 0;

  @Override
  public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException {
    UUID datasetKey = UUID.fromString(Bytes.toString(values.getValue(SyncCommon.OCC_CF, SyncCommon.DK_COL)));
    if (DEAD_DATASETS.contains(datasetKey) || UNCHANGED_DATASETS.contains(datasetKey)) {
      return;
    }

    Dataset dataset = datasetService.get(datasetKey);
    if (dataset.getDeleted() != null) {
      DEAD_DATASETS.add(datasetKey);
      try {
        LOG.info("Sending delete dataset message for dataset [{}]", datasetKey);
        messagePublisher.send(new DeleteDatasetOccurrencesMessage(datasetKey, OccurrenceDeletionReason.DATASET_MANUAL));
      } catch (IOException e) {
        LOG.warn("Failed to send update message", e);
      }
      return;
    }

    // dataset exists, now compare with values we have on the occurrence
    Organization publishingOrg;

    boolean needsUpdate;
    if (DATASET_TO_OWNING_ORG.containsKey(datasetKey)) {
      // seen it before, no need to do comparisons - record needs updating
      publishingOrg = DATASET_TO_OWNING_ORG.get(datasetKey);
      needsUpdate = true;
    } else {
      publishingOrg = orgService.get(dataset.getPublishingOrganizationKey());
      if (occurrenceMutator.requiresUpdate(dataset, publishingOrg, values)) {
        needsUpdate = true;
        DATASET_TO_OWNING_ORG.put(datasetKey, publishingOrg);
      } else {
        needsUpdate = false;
        UNCHANGED_DATASETS.add(datasetKey);
      }
    }

    if (needsUpdate) {
      Occurrence origOcc = occurrencePersistenceService.get(Bytes.toInt(row.get()));
      // we have no clone or other easy copy method
      Occurrence updatedOcc = occurrencePersistenceService.get(Bytes.toInt(row.get()));
      occurrenceMutator.mutateOccurrence(updatedOcc, dataset, publishingOrg);
      occurrencePersistenceService.update(updatedOcc);

      int crawlId = Bytes.toInt(values.getValue(SyncCommon.OCC_CF, SyncCommon.CI_COL));
      OccurrenceMutatedMessage msg =
        OccurrenceMutatedMessage.buildUpdateMessage(datasetKey, origOcc, updatedOcc, crawlId);

      try {
        LOG.info(
          "Sending update for key [{}], publishing org changed from [{}] to [{}] and host country from [{}] to [{}]",
          datasetKey, origOcc.getPublishingOrgKey(), updatedOcc.getPublishingOrgKey(), origOcc.getPublishingCountry(),
          updatedOcc.getPublishingCountry());
        messagePublisher.send(msg);
      } catch (IOException e) {
        LOG.warn("Failed to send update message", e);
      }
    }
    numRecords++;
    if (numRecords % 10000 == 0) {
      context.setStatus("mapper processed " + numRecords + " records so far");
    }
  }
}
