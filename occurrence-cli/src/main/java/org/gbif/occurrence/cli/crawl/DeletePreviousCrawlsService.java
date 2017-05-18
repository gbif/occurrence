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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service that can emits delete message for all gbifid linked to previous crawls.
 */
class DeletePreviousCrawlsService {

  private static final Logger LOG = LoggerFactory.getLogger(DeletePreviousCrawlsService.class);

  private PreviousCrawlsManagerConfiguration config;
  private MessagePublisher publisher;

  // we use a smaller than (<) instead of not equals (<>) to avoid deleting records of a potential new crawl
  // that started
  private static final String SQL_QUERY_GET_OTHER_CRAWL_ID = "SELECT gbifid FROM " +
          " %s WHERE datasetkey = ? AND crawlid < ?";

  private static final Function<String, String> getSqlCommand = (tableName) ->
          String.format(SQL_QUERY_GET_OTHER_CRAWL_ID, tableName);
  private static final int DATASET_KEY_IDX = 1;
  private static final int CRAWL_ID_IDX = 2;

  DeletePreviousCrawlsService(PreviousCrawlsManagerConfiguration config, MessagePublisher publisher) {
    this.config = config;
    this.publisher = publisher;
  }

  private void sendDeleteMessage(int occurrenceKey) throws IOException {
    //Maybe it should use OccurrenceDeletionReason.NOT_SEEN_IN_LAST_CRAWL but it seems OccurrenceDeletionReason is
    //never used
    this.publisher.send(new DeleteOccurrenceMessage(occurrenceKey, OccurrenceDeletionReason.OCCURRENCE_MANUAL, null, null));
  }

  public void close() {
    publisher.close();
  }

  /**
   * Sends delete message for all occurrence records that are coming from a crawl before lastSuccessfulCrawl.
   * @param datasetKey
   * @param lastSuccessfulCrawl
   * @return the number of delete message emitted.
   */
  public int deleteOccurrenceInPreviousCrawls(UUID datasetKey, int lastSuccessfulCrawl) {
    int numberOfMessageEmitted = 0;
    try (Connection conn = config.hive.buildHiveConnection();
         PreparedStatement stmt = conn.prepareStatement(getSqlCommand.apply(config.hiveOccurrenceTable))) {
      stmt.setString(DATASET_KEY_IDX, datasetKey.toString());
      stmt.setInt(CRAWL_ID_IDX, lastSuccessfulCrawl);

      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          sendDeleteMessage(rs.getInt(1));
          numberOfMessageEmitted++;

          if(numberOfMessageEmitted % config.deleteMessageBatchSize == 0){
            Thread.sleep(config.deleteMessageBatchIntervalMs);
          }
        }
      } catch (IOException | InterruptedException e) {
        LOG.error("Error while deleting records for dataset " + datasetKey , e);
      }
    } catch (SQLException e) {
      LOG.error("Error while deleting records for dataset " + datasetKey , e);
    }
    return numberOfMessageEmitted;
  }
}
