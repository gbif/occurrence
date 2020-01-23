package org.gbif.occurrence.cli.dataset;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.messages.DeleteDatasetOccurrencesMessage;
import org.gbif.common.messaging.api.messages.OccurrenceDeletionReason;
import org.gbif.occurrence.cli.common.EsHelper;
import org.gbif.pipelines.common.PipelinesVariables.Pipeline.HdfsView;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

/** Callback that is called when the {@link DeleteDatasetOccurrencesMessage} is received. */
public class EsDatasetDeleterCallback
  extends AbstractMessageCallback<DeleteDatasetOccurrencesMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(EsDatasetDeleterCallback.class);

  private final RestHighLevelClient esClient;
  private final EsDatasetDeleterConfiguration config;
  private final FileSystem fs;

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

  public EsDatasetDeleterCallback(RestHighLevelClient esClient, FileSystem fs, EsDatasetDeleterConfiguration config) {
    this.esClient = esClient;
    this.config = config;
    this.fs = fs;
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
      EsHelper.findExistingIndexesInAliases(esClient, datasetKey, config.esIndex);

    if (datasetIndexes == null || datasetIndexes.isEmpty()) {
      LOG.info("No indexes found in aliases {} for dataset {}", config.esIndex, datasetKey);
      return;
    }

    final TimerContext contextDeleteIndex = processTimerDeleteIndex.time();
    // remove independent indexes for this dataset
    datasetIndexes.stream()
      .filter(i -> i.startsWith(datasetKey))
      .forEach(idx -> EsHelper.deleteIndex(esClient, idx));
    contextDeleteIndex.stop();

    final TimerContext contextDeleteByQuery = processTimerDeleteByQuery.time();
    // delete documents of this dataset in non-independent indexes
    datasetIndexes.stream()
      .filter(i -> !i.startsWith(datasetKey))
      .forEach(idx -> EsHelper.deleteByDatasetKey(esClient, datasetKey, idx));
    contextDeleteByQuery.stop();

    // Delete dataset from ingest folder
    String deleteIngestPath = String.join(Path.SEPARATOR, config.ingestDirPath, datasetKey);
    deleteByPattern(fs, deleteIngestPath);

    // Delete dataset from hdfs view directory
    String viewFileName = HdfsView.VIEW_OCCURRENCE + "_" + datasetKey + "_*";
    String deleteHdfsPath = String.join(Path.SEPARATOR, config.hdfsViewDirPath, viewFileName);
    deleteByPattern(fs, deleteHdfsPath);
  }

  /**
   * Deletes a list files that match against a glob filter into a target directory.
   *
   * @param globFilter filter used to filter files and paths
   */
  public static void deleteByPattern(FileSystem fs, String globFilter) {
    try {
      FileStatus[] status = fs.globStatus(new Path(globFilter));
      Path[] paths = FileUtil.stat2Paths(status);
      for (Path path : paths) {
        LOG.info("Deleting the path - {}", path);
        fs.delete(path, Boolean.TRUE);
      }

    } catch (IOException e) {
      LOG.warn("Can't delete files using filter - {}", globFilter);
      throw new RuntimeException(e);
    }
  }

}
