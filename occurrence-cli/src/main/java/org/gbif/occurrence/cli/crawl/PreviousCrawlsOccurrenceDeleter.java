package org.gbif.occurrence.cli.crawl;

import org.gbif.api.model.registry.Comment;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DeleteOccurrenceMessage;
import org.gbif.common.messaging.api.messages.OccurrenceDeletionReason;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that can emits delete message for all gbifid linked to previous crawls.
 * Previous crawls gbifid are received by a SQL query executed on a JDBC connection (e.g. Hive JDBC).
 *
 * Note: this class will issue a Thread.sleep the value of config.deleteMessageBatchIntervalMs on each
 * config.deleteMessageBatchSize emitted.
 */
class PreviousCrawlsOccurrenceDeleter {

  private static final Logger LOG = LoggerFactory.getLogger(PreviousCrawlsOccurrenceDeleter.class);

  public static final String REGISTRY_COMMENT_CREATED_BY = "PreviousCrawls Occurrence Deleter";
  private static final String REGISTRY_COMMENT = "Auto-deletion of stale records: %d occurrence(s) flagged for deletion. %d was identified as the last normal crawl attempt.";

  private PreviousCrawlsManagerConfiguration config;
  private MessagePublisher publisher;
  private DatasetService datasetService;

  // we use a smaller than (<) instead of not equal (<>) to avoid deleting records of a potential new crawl
  // that started
  private static final String SQL_QUERY_GET_OTHER_CRAWL_ID = "SELECT gbifId, crawlId FROM " +
          " %s WHERE datasetKey = ? AND crawlId < ?";

  private static final Function<String, String> getSqlCommand = (tableName) ->
          String.format(SQL_QUERY_GET_OTHER_CRAWL_ID, tableName);

  private static final String GBIF_ID_LBL = "gbifId";
  private static final String CRAWL_ID_LBL = "crawlId";

  private static final int DATASET_KEY_STMT_IDX = 1;
  private static final int CRAWL_ID_STMT_IDX = 2;

  /**
   * @param config
   * @param publisher caller is responsible to close the provided {@link MessagePublisher}
   * @param datasetService used to log comments to the registry of what was deleted (this is not essential and could be removed
   *                       once proven stable)
   */
  @Inject
  PreviousCrawlsOccurrenceDeleter(PreviousCrawlsManagerConfiguration config, MessagePublisher publisher,
                                  DatasetService datasetService) {
    Preconditions.checkArgument(publisher != null || !(config.delete || config.forceDelete),
            "MessagePublisher shall be provided if configuration indicates deletion should be applied.");
    this.config = config;
    this.publisher = publisher;
    this.datasetService = datasetService;
  }

  /**
   * Send a delete message (NOT_SEEN_IN_LAST_CRAWL) for an occurrence key.
   * @param occurrenceKey
   * @param lastSeenInCrawlId last crawl we saw the provided occurrenceKey
   * @param lastSuccessfulCrawlId current crawl identified as the lastSuccessfulCrawl
   * @throws IOException
   */
  private void sendDeleteMessage(long occurrenceKey, int lastSeenInCrawlId, int lastSuccessfulCrawlId) throws IOException {
    this.publisher.send(new DeleteOccurrenceMessage(occurrenceKey, OccurrenceDeletionReason.NOT_SEEN_IN_LAST_CRAWL,
            lastSeenInCrawlId, lastSuccessfulCrawlId));
  }

  /**
   * Record a message in the registry to keep a trace of automatic deletion.
   * Eventually we might want to only record deletion when the numberOfMessageEmitted is > than a certain threshold.
   * @param datasetKey
   * @param lastSuccessfulCrawl
   * @param numberOfMessageEmitted
   */
  private void addRegistryComment(UUID datasetKey, int lastSuccessfulCrawl, int numberOfMessageEmitted) {
    Comment comment = new Comment();
    comment.setContent(String.format(REGISTRY_COMMENT, numberOfMessageEmitted, lastSuccessfulCrawl));
    comment.setCreatedBy(REGISTRY_COMMENT_CREATED_BY);
    datasetService.addComment(datasetKey, comment);
  }

  /**
   * Sends delete message for all occurrence records that are coming from a crawl before lastSuccessfulCrawl.
   *
   * @param datasetKey
   * @param lastSuccessfulCrawl
   *
   * @return the number of delete message emitted.
   */
  public int deleteOccurrenceInPreviousCrawls(UUID datasetKey, int lastSuccessfulCrawl) {
    int numberOfMessageEmitted = 0;
    try (Connection conn = config.hive.buildHiveConnection();
         PreparedStatement stmt = conn.prepareStatement(getSqlCommand.apply(config.hiveOccurrenceTable))) {
      stmt.setString(DATASET_KEY_STMT_IDX, datasetKey.toString());
      stmt.setInt(CRAWL_ID_STMT_IDX, lastSuccessfulCrawl);

      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          sendDeleteMessage(rs.getLong(GBIF_ID_LBL), rs.getInt(CRAWL_ID_LBL), lastSuccessfulCrawl);
          numberOfMessageEmitted++;

          if (numberOfMessageEmitted % config.deleteMessageBatchSize == 0) {
            Thread.sleep(config.deleteMessageBatchIntervalMs);
          }
        }
      } catch (IOException | InterruptedException e) {
        LOG.error("Error while deleting records for dataset " + datasetKey, e);
      }
    } catch (SQLException e) {
      LOG.error("Error while deleting records for dataset " + datasetKey, e);
    }

    //FIXME eventually change this to only log if config says force-delete since a regular delete
    //will become the normal behavior after we cleaned the current backlog
    addRegistryComment(datasetKey, lastSuccessfulCrawl, numberOfMessageEmitted);
    return numberOfMessageEmitted;
  }
}
