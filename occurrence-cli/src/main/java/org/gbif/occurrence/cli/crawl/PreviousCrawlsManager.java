package org.gbif.occurrence.cli.crawl;

import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.crawler.DatasetProcessStatus;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.gbif.api.service.registry.DatasetProcessStatusService;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manager that checks for previous crawls and send delete messages if predefined conditions are met.
 */
public class PreviousCrawlsManager {

  private static final Logger LOG = LoggerFactory.getLogger(PreviousCrawlsManager.class);

  private static final String SQL_WITH_CLAUSE =
          "WITH t1 AS (" +
                  " SELECT datasetKey, count(DISTINCT crawlID) crawlCount" +
                  " FROM %s" +
                  " WHERE protocol = 'DWC_ARCHIVE'" +
                  " GROUP BY datasetKey" +
                  " HAVING crawlCount > 1 )";

  private static final String SQL_QUERY =
          " SELECT " +
                  " o.datasetKey, o.crawlId, count(*) AS crawlCount" +
                  " FROM " +
                  " %s o JOIN t1 ON o.datasetKey = t1.datasetKey" +
                  " GROUP BY " +
                  "  o.datasetKey, o.crawlId" +
                  " ORDER BY " +
                  "  o.datasetKey, o.crawlId";

  private static final String SQL_QUERY_SINGLE_DATASET = "SELECT datasetkey, crawlid, count(*) AS crawlCount FROM " +
          " %s WHERE datasetkey = ? GROUP BY datasetkey, crawlid";

  private static final int DATASET_KEY_IDX = 1;
  private static final int CRAWL_ID_IDX = 2;
  private static final int CRAWL_COUNT_IDX = 3;

  private static final Function<String, String> getSqlCommand = (tableName) ->
          String.format(SQL_WITH_CLAUSE, tableName) + String.format(SQL_QUERY, tableName);

  private static final Function<String, String> getSqlCommandSingleDataset = (tableName) ->
          String.format(SQL_QUERY_SINGLE_DATASET, tableName);

  private final PreviousCrawlsManagerConfiguration config;
  private final DatasetProcessStatusService datasetProcessStatusService;
  private final OccurrenceSearchService occurrenceSearchService;
  private final PreviousCrawlsOccurrenceDeleter deletePreviousCrawlsService;

  public PreviousCrawlsManager(PreviousCrawlsManagerConfiguration config, DatasetProcessStatusService datasetProcessStatusService,
                               OccurrenceSearchService occurrenceSearchService, PreviousCrawlsOccurrenceDeleter deletePreviousCrawlsService) {
    this.config = config;
    this.datasetProcessStatusService = datasetProcessStatusService;
    this.occurrenceSearchService = occurrenceSearchService;
    this.deletePreviousCrawlsService = deletePreviousCrawlsService;
  }

  /**
   * Starts the service.
   *
   * @param resultHandler handler used to serialize the results
   */
  public void execute(Consumer<Object> resultHandler) {
    Object report;
    if (config.datasetKey == null) {
      report = manageDatasetWithMoreThanOneCrawl();
    } else {
      report = manageSingleDataset(UUID.fromString(config.datasetKey));
    }
    resultHandler.accept(report);
  }

  private DatasetRecordCountInfo manageSingleDataset(UUID datasetKey) {
    DatasetRecordCountInfo datasetRecordCountInfo = getDatasetCrawlInfo(datasetKey);
    if (shouldRunAutomaticDeletion(datasetRecordCountInfo)) {
      int numberOfMessageEmitted = deletePreviousCrawlsService.deleteOccurrenceInPreviousCrawls(datasetKey,
              datasetRecordCountInfo.getLastCrawlId());
      LOG.info("Number Of Delete message emitted: " + numberOfMessageEmitted);
    }
    return datasetRecordCountInfo;
  }

  /**
   *
   * @return
   */
  private Map<UUID, DatasetRecordCountInfo> manageDatasetWithMoreThanOneCrawl() {

    //we do not support forceDelete on all datasets
    if (config.forceDelete) {
      LOG.error("forceDelete is only support for a single dataset");
      return Collections.EMPTY_MAP;
    }

    Map<UUID, DatasetRecordCountInfo> allDatasetWithMoreThanOneCrawl =
            getAllDatasetWithMoreThanOneCrawl();

    if (config.delete) {
      allDatasetWithMoreThanOneCrawl.entrySet()
              .stream()
              .map(Map.Entry::getValue)
              .filter(this::shouldRunAutomaticDeletion)
              .limit(config.datasetAutodeletionLimit)
              .forEach(drci -> {
                int numberOfMessageEmitted = deletePreviousCrawlsService.deleteOccurrenceInPreviousCrawls(
                        drci.getDatasetKey(), drci.getLastCrawlId());
                LOG.info("Number Of Delete message emitted for dataset " + drci.getDatasetKey() +
                        ": " + numberOfMessageEmitted);
              });
    }
    return allDatasetWithMoreThanOneCrawl;
  }

  /**
   * Based on {@link PreviousCrawlsManagerConfiguration} and {@link DatasetRecordCountInfo}, decides if we should
   * trigger deletion of occurrence records that belong to previous crawl(s).
   *
   * @param datasetRecordCountInfo
   *
   * @return
   */
  @VisibleForTesting
  protected boolean shouldRunAutomaticDeletion(DatasetRecordCountInfo datasetRecordCountInfo) {

    if (config.forceDelete) {
      return true;
    }

    if (!config.delete) {
      return false;
    }

    if(datasetRecordCountInfo.getLastCrawlCount() != datasetRecordCountInfo.getLastCrawlFragmentProcessCount()) {
      LOG.info("Dataset " + datasetRecordCountInfo.getDatasetKey() +
              " -> No automatic deletion. Crawl lastCrawlCount != lastCrawlFragmentProcessCount which may indicate an " +
              " incomplete or bad crawl. lastCrawlCount:" + datasetRecordCountInfo.getLastCrawlCount() +
              ", lastCrawlFragmentProcessCount" + datasetRecordCountInfo.getLastCrawlFragmentProcessCount());
      return false;
    }

    if (datasetRecordCountInfo.getPercentagePreviousCrawls() > config.automaticRecordDeletionThreshold) {
      LOG.info("Dataset " + datasetRecordCountInfo.getDatasetKey() +
              "-> No automatic deletion. Percentage of records to remove (" + datasetRecordCountInfo.getPercentagePreviousCrawls() +
              "% ) higher than the configured threshold (" + config.automaticRecordDeletionThreshold + "%).");
      return false;
    }
    return true;
  }

  /**
   * Get {@link DatasetCrawlInfo} for a single Dataset.
   * @param datasetKey
   * @return
   */
  private DatasetRecordCountInfo getDatasetCrawlInfo(UUID datasetKey) {
    DatasetRecordCountInfo datasetRecordCountInfo  = new DatasetRecordCountInfo();
    datasetRecordCountInfo.setDatasetKey(datasetKey);
    List<DatasetCrawlInfo> datasetCrawlInfoList = new ArrayList<>();

    try (Connection conn = config.hive.buildHiveConnection();
         PreparedStatement stmt = conn.prepareStatement(getSqlCommandSingleDataset.apply(config.hiveOccurrenceTable))) {
      stmt.setString(1, datasetKey.toString());
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          datasetCrawlInfoList.add(new DatasetCrawlInfo(rs.getInt(2), rs.getInt(3)));
        }
      }
      datasetRecordCountInfo.setCrawlInfo(datasetCrawlInfoList);
      populateSolrAndRegistryData(datasetRecordCountInfo);
    } catch (SQLException e) {
      LOG.error("Error while getting crawl information for dataset " + datasetKey , e);
    }
    return datasetRecordCountInfo;
  }

  /**
   * Get {@link DatasetRecordCountInfo} for each datasets that has records coming to more than one crawl.
   * @return
   */
  private Map<UUID, DatasetRecordCountInfo> getAllDatasetWithMoreThanOneCrawl() {
    String sql = getSqlCommand.apply(config.hiveOccurrenceTable);

    Map<UUID, DatasetRecordCountInfo> crawlInfo = new HashMap<>();
    try (Connection conn = config.hive.buildHiveConnection();
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {

      UUID currentDatasetKey = null;
      DatasetRecordCountInfo currentDatasetRecordCountInfo;
      List<DatasetCrawlInfo> currentDatasetCrawlInfoList = new ArrayList<>();

      while (rs.next()) {

        if(!UUID.fromString(rs.getString(DATASET_KEY_IDX)).equals(currentDatasetKey)) {
          //manage previous list
          if(currentDatasetKey != null) {
            currentDatasetRecordCountInfo = new DatasetRecordCountInfo();
            currentDatasetRecordCountInfo.setDatasetKey(currentDatasetKey);
            currentDatasetRecordCountInfo.setCrawlInfo(currentDatasetCrawlInfoList);
            populateSolrAndRegistryData(currentDatasetRecordCountInfo);
            crawlInfo.put(currentDatasetKey, currentDatasetRecordCountInfo);
          }
          currentDatasetKey = UUID.fromString(rs.getString(DATASET_KEY_IDX));
          currentDatasetCrawlInfoList = new ArrayList<>();
        }
        currentDatasetCrawlInfoList.add(new DatasetCrawlInfo(rs.getInt(CRAWL_ID_IDX), rs.getInt(CRAWL_COUNT_IDX)));
      }
    } catch (SQLException e) {
      LOG.error("Error while generating the crawls report", e);
    }
    return crawlInfo;
  }

  /**
   * Populate the given {@link DatasetRecordCountInfo} with Solr count and last crawls information from the registry.
   *
   * @return the provided {@link DatasetRecordCountInfo} populated
   */
  private DatasetRecordCountInfo populateSolrAndRegistryData(DatasetRecordCountInfo datasetRecordCountInfo) {

    //Get the count from Solr
    OccurrenceSearchRequest osReq = new OccurrenceSearchRequest();
    osReq.addDatasetKeyFilter(datasetRecordCountInfo.getDatasetKey());
    osReq.setLimit(1);
    SearchResponse<Occurrence, OccurrenceSearchParameter> occResponse = occurrenceSearchService.search(osReq);
    datasetRecordCountInfo.setCurrentSolrCount(occResponse.getCount() != null ? occResponse.getCount() : 0);

    //Get crawl status of the last crawl
    DatasetProcessStatus lastCompletedCrawl = datasetProcessStatusService.getDatasetProcessStatus(datasetRecordCountInfo.getDatasetKey(),
            datasetRecordCountInfo.getLastCrawlId());
    if (lastCompletedCrawl != null) {
      datasetRecordCountInfo.setLastCrawlFragmentProcessCount(lastCompletedCrawl.getFragmentsProcessed());
    }
    return datasetRecordCountInfo;
  }

}
