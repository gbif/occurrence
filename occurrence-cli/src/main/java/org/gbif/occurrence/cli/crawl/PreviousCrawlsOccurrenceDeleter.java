package org.gbif.occurrence.cli.crawl;

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

  private PreviousCrawlsManagerConfiguration config;
  private MessagePublisher publisher;

  // we use a smaller than (<) instead of not equals (<>) to avoid deleting records of a potential new crawl
  // that started
  private static final String SQL_QUERY_GET_OTHER_CRAWL_ID = "SELECT gbifid FROM " +
          " %s WHERE datasetkey = ? AND crawlid < ?";

  private static final Function<String, String> getSqlCommand = (tableName) ->
          String.format(SQL_QUERY_GET_OTHER_CRAWL_ID, tableName);
  private static final int GBIF_ID_SELECT_IDX = 1;
  private static final int DATASET_KEY_STMT_IDX = 1;
  private static final int CRAWL_ID_STMT_IDX = 2;

  /**
   * @param config
   * @param publisher caller is responsible to close the provided {@link MessagePublisher}
   */
  PreviousCrawlsOccurrenceDeleter(PreviousCrawlsManagerConfiguration config, MessagePublisher publisher) {
    Preconditions.checkArgument((config.delete || config.forceDelete) && publisher != null, "MessagePublisher " +
            "shall be provided if configuration indicates deletion should be applied.");
    this.config = config;
    this.publisher = publisher;
  }

  private void sendDeleteMessage(int occurrenceKey) throws IOException {
    //Maybe it should use OccurrenceDeletionReason.NOT_SEEN_IN_LAST_CRAWL but it seems OccurrenceDeletionReason is
    //never used
    this.publisher.send(new DeleteOccurrenceMessage(occurrenceKey, OccurrenceDeletionReason.OCCURRENCE_MANUAL, null, null));
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
          sendDeleteMessage(rs.getInt(GBIF_ID_SELECT_IDX));
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
    return numberOfMessageEmitted;
  }
}
