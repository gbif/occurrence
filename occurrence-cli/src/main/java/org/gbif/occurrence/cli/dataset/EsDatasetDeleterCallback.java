package org.gbif.occurrence.cli.dataset;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.messages.DeleteDatasetOccurrencesMessage;
import org.gbif.common.messaging.api.messages.OccurrenceDeletionReason;
import org.gbif.occurrence.cli.common.EsHelper;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/** Callback that is called when the {@link DeleteDatasetOccurrencesMessage} is received. */
public class EsDatasetDeleterCallback
    extends AbstractMessageCallback<DeleteDatasetOccurrencesMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(EsDatasetDeleterCallback.class);

  private final RestHighLevelClient esClient;
  private final String[] esIndex;

  private final Timer processTimerDeleteByQuery =
      Metrics.newTimer(
          EsDatasetDeleterCallback.class,
          "ES dataset delete by query time",
          TimeUnit.MILLISECONDS,
          TimeUnit.SECONDS);

  private final Timer processTimerDeleteIndex =
      Metrics.newTimer(
          EsDatasetDeleterCallback.class,
          "ES dataset delete index time",
          TimeUnit.MILLISECONDS,
          TimeUnit.SECONDS);

  public EsDatasetDeleterCallback(RestHighLevelClient esClient, String[] esIndex) {
    this.esClient = esClient;
    this.esIndex = esIndex;
  }

  @Override
  public void handleMessage(DeleteDatasetOccurrencesMessage message) {
    MDC.put("datasetKey", message.getDatasetUuid().toString());

    if (OccurrenceDeletionReason.DATASET_MANUAL != message.getDeletionReason()) {
      LOG.warn("In Pipelines we only support DATASET_MANUAL deletion events");
      return;
    }

    final String datasetKey = message.getDatasetUuid().toString();
    // find the indexes where the dataset is indexed
    Set<String> datasetIndexes =
        EsHelper.findExistingIndexesInAliases(esClient, datasetKey, esIndex);

    if (datasetIndexes == null || datasetIndexes.isEmpty()) {
      LOG.info("No indexes found in aliases {} for dataset {}", esIndex, datasetKey);
      return;
    }

    final TimerContext contextDeleteIndex = processTimerDeleteIndex.time();
    // remove independent indexes for this dataset
    datasetIndexes.stream()
        .filter(i -> i.startsWith(datasetKey))
        .forEach(
            idx -> {
              LOG.info("Deleting ES index {}", idx);
              EsHelper.deleteIndex(esClient, idx);
            });
    contextDeleteIndex.stop();

    final TimerContext contextDeleteByQuery = processTimerDeleteByQuery.time();
    // delete documents of this dataset in non-independent indexes
    datasetIndexes.stream()
        .filter(i -> !i.startsWith(datasetKey))
        .forEach(
            idx -> {
              LOG.info("Deleting all documents of dataset {} from ES index {}", datasetKey, idx);
              EsHelper.deleteByDatasetKey(esClient, datasetKey, idx);
            });
    contextDeleteByQuery.stop();
  }
}
