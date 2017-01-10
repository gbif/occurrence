package org.gbif.occurrence.cli.registry.sync;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Organization;
import org.gbif.common.messaging.api.messages.DeleteDatasetOccurrencesMessage;
import org.gbif.common.messaging.api.messages.OccurrenceDeletionReason;
import org.gbif.common.messaging.api.messages.OccurrenceMutatedMessage;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A MapReduce Mapper that synchronizes all occurrences (called with this mapper) with the registry.
 */
public class OccurrenceRegistryMapper extends AbstractOccurrenceRegistryMapper {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceRegistryMapper.class);

  private static final int MAX_DATASET_CACHE = 1000;
  private static final int MAX_ORGANIZATION_CACHE = 1000;

  private LoadingCache<UUID, Dataset> datasetCache;
  private LoadingCache<UUID, Organization> organizationCache;

  private static final Set<UUID> DELETED_DATASETS = Sets.newHashSet();

  private int numRecords = 0;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    datasetCache = CacheBuilder.newBuilder()
            .maximumSize(MAX_DATASET_CACHE)
            .build(
                    new CacheLoader<UUID, Dataset>() {
                      public Dataset load(UUID datasetKey) {
                        return datasetService.get(datasetKey);
                      }
                    });

    organizationCache = CacheBuilder.newBuilder()
            .maximumSize(MAX_ORGANIZATION_CACHE)
            .build(
                    new CacheLoader<UUID, Organization>() {
                      public Organization load(UUID key) {
                        return orgService.get(key);
                      }
                    });
  }

  @Override
  public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException {
    UUID datasetKey = UUID.fromString(Bytes.toString(values.getValue(SyncCommon.OCC_CF, SyncCommon.DK_COL)));

    if (DELETED_DATASETS.contains(datasetKey)) {
      return;
    }

    try {
      Dataset dataset = datasetCache.get(datasetKey);
      if (dataset.getDeleted() != null) {
        DELETED_DATASETS.add(datasetKey);
        try {
          LOG.info("Sending delete dataset message for dataset [{}]", datasetKey);
          messagePublisher.send(new DeleteDatasetOccurrencesMessage(datasetKey, OccurrenceDeletionReason.DATASET_MANUAL));
        } catch (IOException e) {
          LOG.warn("Failed to send update message", e);
        }
        return;
      }

      Organization publishingOrg = organizationCache.get(dataset.getPublishingOrganizationKey());
      if (occurrenceMutator.requiresUpdate(dataset, publishingOrg, values)) {
        Occurrence origOcc = occurrencePersistenceService.get(Bytes.toInt(row.get()));
        // we have no clone or other easy copy method
        Occurrence updatedOcc = occurrencePersistenceService.get(Bytes.toInt(row.get()));
        occurrenceMutator.mutateOccurrence(updatedOcc, dataset, publishingOrg);

        //FIX ME
        if (numRecords % 10000 == 0) {
          occurrencePersistenceService.update(updatedOcc);

          int crawlId = Bytes.toInt(values.getValue(SyncCommon.OCC_CF, SyncCommon.CI_COL));
          OccurrenceMutatedMessage msg =
                  OccurrenceMutatedMessage.buildUpdateMessage(datasetKey, origOcc, updatedOcc, crawlId);
          try {
            //TODO use generateUpdateMessage
            LOG.debug(
                    "Sending update for key [{}], publishing org changed from [{}] to [{}] and host country from [{}] to [{}]",
                    datasetKey, origOcc.getPublishingOrgKey(), updatedOcc.getPublishingOrgKey(), origOcc.getPublishingCountry(),
                    updatedOcc.getPublishingCountry());
            messagePublisher.send(msg);
          } catch (IOException e) {
            LOG.warn("Failed to send update message", e);
          }
        }
      }
      numRecords++;
      if (numRecords % 10000 == 0) {
        context.setStatus("mapper processed " + numRecords + " records so far");
      }
    } catch (ExecutionException e) {
      LOG.warn("Failed to get Dataset/Organization data for datasetKey {}", datasetKey, e);
    }
  }
}
