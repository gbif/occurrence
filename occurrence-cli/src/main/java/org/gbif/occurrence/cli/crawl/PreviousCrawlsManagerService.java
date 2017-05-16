package org.gbif.occurrence.cli.crawl;

import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.crawler.DatasetProcessStatus;
import org.gbif.api.model.crawler.FinishReason;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.gbif.api.service.registry.DatasetProcessStatusService;
import org.gbif.occurrence.ws.client.OccurrenceWsClientModule;
import org.gbif.registry.ws.client.guice.RegistryWsClientModule;
import org.gbif.ws.client.guice.AnonymousAuthModule;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service that checks for previous crawls and send delete messages if predefined conditions are met.
 */
public class PreviousCrawlsManagerService {

  private static final Logger LOG = LoggerFactory.getLogger(PreviousCrawlsManagerService.class);

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

  private static final Function<String, String> getSqlCommand = (tableName) ->
          String.format(SQL_WITH_CLAUSE, tableName) + String.format(SQL_QUERY, tableName);

  private static final Function<String, String> getSqlCommandSingleDataset = (tableName) ->
          String.format(SQL_QUERY_SINGLE_DATASET, tableName);

  private final PreviousCrawlsManagerConfiguration config;

  private DatasetProcessStatusService datasetProcessStatusService;
  private OccurrenceSearchService occurrenceSearchService;
  private final DeletePreviousCrawlsService deletePreviousCrawlsService;

  public PreviousCrawlsManagerService(PreviousCrawlsManagerConfiguration config, DeletePreviousCrawlsService deletePreviousCrawlsService) {
    this.config = config;
    this.deletePreviousCrawlsService = deletePreviousCrawlsService;
  }

  private void prepare() {
    // Create Registry WS Client
    Properties properties = new Properties();
    properties.setProperty("registry.ws.url", config.registryWsUrl);
    properties.setProperty("occurrence.ws.url", config.registryWsUrl);
    properties.setProperty("httpTimeout", "30000");

    Injector injector = Guice.createInjector(new RegistryWsClientModule(properties), new AnonymousAuthModule(),
            new OccurrenceWsClientModule(properties));
    datasetProcessStatusService = injector.getInstance(DatasetProcessStatusService.class);
    occurrenceSearchService = injector.getInstance(OccurrenceSearchService.class);
  }

  /**
   * Starts the service.
   * @param resultHandler handler used to serialize the results
   */
  public void start(Consumer<Object> resultHandler) {
    prepare();

    Object report;
    //
    if (config.datasetKey != null) {
      report = runOnSingleDataset(UUID.fromString(config.datasetKey));
    } else {
      Map<UUID, DatasetRecordCountInfo> allDatasetWithMoreThanOneCrawl =
              getAllDatasetWithMoreThanOneCrawl();
      report = allDatasetWithMoreThanOneCrawl;
    }
   // analyseReport(allDatasetWithMoreThanOneCrawl);

    resultHandler.accept(report);
  }


  private DatasetRecordCountInfo runOnSingleDataset(UUID datasetKey) {
    DatasetRecordCountInfo datasetRecordCountInfo = getDatasetCrawlInfo(datasetKey);
    if (shouldRunDeletion(datasetRecordCountInfo)) {
      int numberOfMessageEmitted = deletePreviousCrawlsService.deleteOccurrenceInPreviousCrawls(datasetKey,
              datasetRecordCountInfo.getLastCompleteCrawlId());
      LOG.info("Number Of Delete message emitted: " + numberOfMessageEmitted);
    }
    return datasetRecordCountInfo;
  }

  /**
   * Based on {@link PreviousCrawlsManagerConfiguration} and {@link DatasetRecordCountInfo}, decides if we should
   * trigger deletion of occurrence records that belong to previous crawl(s).
   *
   * @param datasetRecordCountInfo
   *
   * @return
   */
  private boolean shouldRunDeletion(DatasetRecordCountInfo datasetRecordCountInfo) {
    if (!config.delete) {
      return false;
    }

    if (config.automaticRecordDeletionThreshold > datasetRecordCountInfo.getDiffSolrLastCrawlPercentage()) {
      LOG.info("No automatic deletion. Percentage of records to remove (" + datasetRecordCountInfo.getDiffSolrLastCrawlPercentage() +
              ") higher than the configured threshold (" + config.automaticRecordDeletionThreshold + ").");
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
    DatasetRecordCountInfo datasetRecordCountInfo  = getDatasetRecordCountInfo(datasetKey);
    List<DatasetCrawlInfo> datasetCrawlInfoList = new ArrayList<>();
    datasetRecordCountInfo.setCrawlInfo(datasetCrawlInfoList);
    try (Connection conn = config.hive.buildHiveConnection();
         PreparedStatement stmt = conn.prepareStatement(getSqlCommandSingleDataset.apply(config.hiveOccurrenceTable))) {
      stmt.setString(1, datasetKey.toString());
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          datasetCrawlInfoList.add(new DatasetCrawlInfo(rs.getInt(2), rs.getInt(3)));
        }
      }
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
      DatasetRecordCountInfo currentDatasetRecordCountInfo = null;
      List<DatasetCrawlInfo> currentDatasetCrawlInfoList = new ArrayList<>();
      while (rs.next()) {

        if(!UUID.fromString(rs.getString(1)).equals(currentDatasetKey)) {
          currentDatasetKey = UUID.fromString(rs.getString(1));
          currentDatasetCrawlInfoList = new ArrayList<>();
          currentDatasetRecordCountInfo = getDatasetRecordCountInfo(currentDatasetKey);
          currentDatasetRecordCountInfo.setCrawlInfo(currentDatasetCrawlInfoList);
          crawlInfo.put(currentDatasetKey, currentDatasetRecordCountInfo);
        }
        currentDatasetCrawlInfoList.add(new DatasetCrawlInfo(rs.getInt(2), rs.getInt(3)));
      }
    } catch (SQLException e) {
      LOG.error("Error while generating the crawls report", e);
    }
    return crawlInfo;
  }

  /**
   * Load information related to Solr count and last crawls .
   *
   * @param datasetKey
   *
   * @return
   */
  private DatasetRecordCountInfo getDatasetRecordCountInfo(UUID datasetKey) {

    DatasetRecordCountInfo datasetRecordCountInfo = new DatasetRecordCountInfo(datasetKey);

    //Get the count from Solr
    OccurrenceSearchRequest osReq = new OccurrenceSearchRequest();
    osReq.addDatasetKeyFilter(datasetKey);
    osReq.setLimit(1);
    SearchResponse<Occurrence, OccurrenceSearchParameter> occResponse = occurrenceSearchService.search(osReq);
    datasetRecordCountInfo.setSolrCount(occResponse.getCount() != null ? occResponse.getCount() : 0);

    //Check crawl status, try to get the latest successful crawl
    PagingResponse<DatasetProcessStatus> processStatus = datasetProcessStatusService.listDatasetProcessStatus(datasetKey, null);
    Optional<DatasetProcessStatus> lastCompletedCrawl = processStatus.getResults()
            .stream()
            .filter(dps -> FinishReason.NORMAL == dps.getFinishReason() && dps.getPagesFragmentedSuccessful() > 0)
            .findFirst();

    if (lastCompletedCrawl.isPresent()) {
      DatasetProcessStatus datasetProcessStatus = lastCompletedCrawl.get();
      datasetRecordCountInfo.setLastCompleteCrawlId(datasetProcessStatus.getCrawlJob().getAttempt());
      datasetRecordCountInfo.setFragmentEmittedCount(datasetProcessStatus.getFragmentsEmitted());
      datasetRecordCountInfo.setFragmentProcessCount(datasetProcessStatus.getFragmentsProcessed());
    }
    return datasetRecordCountInfo;
  }

  private static class DatasetRecordCountInfo {
    private UUID datasetKey;
    private int lastCompleteCrawlId;
    private long fragmentEmittedCount;
    private long fragmentProcessCount;
    private long solrCount;
    private double diffSolrLastCrawlPercentage;
    private List<DatasetCrawlInfo> crawlInfo;

    public DatasetRecordCountInfo(){}

    public List<DatasetCrawlInfo> getCrawlInfo() {
      return crawlInfo;
    }

    public void setCrawlInfo(List<DatasetCrawlInfo> crawlInfo) {
      this.crawlInfo = crawlInfo;
    }

    public DatasetRecordCountInfo(UUID datasetKey) {
      this.datasetKey = datasetKey;
    }

    public UUID getDatasetKey() {
      return datasetKey;
    }

    public void setDatasetKey(UUID datasetKey) {
      this.datasetKey = datasetKey;
    }

    public int getLastCompleteCrawlId() {
      return lastCompleteCrawlId;
    }

    public void setLastCompleteCrawlId(int lastCompleteCrawlId) {
      this.lastCompleteCrawlId = lastCompleteCrawlId;
    }

    public long getFragmentEmittedCount() {
      return fragmentEmittedCount;
    }

    public void setFragmentEmittedCount(long fragmentEmittedCount) {
      this.fragmentEmittedCount = fragmentEmittedCount;
    }

    public long getFragmentProcessCount() {
      return fragmentProcessCount;
    }

    public void setFragmentProcessCount(long fragmentProcessCount) {
      this.fragmentProcessCount = fragmentProcessCount;
    }

    public long getSolrCount() {
      return solrCount;
    }

    public void setSolrCount(long solrCount) {
      this.solrCount = solrCount;
    }

    public long getDiffSolrLastCrawl() {
      return solrCount - fragmentProcessCount;
    }

    public long getSumAllPreviousCrawl() {
      if(crawlInfo == null || crawlInfo.isEmpty()) {
        return 0;
      }
      return crawlInfo.stream()
              .filter( dci -> dci.getCrawlId() != lastCompleteCrawlId)
              .mapToLong( dci -> dci.getCount())
              .sum();
    }

    public double getDiffSolrLastCrawlPercentage() {
      if(solrCount == 0) {
        return 0;
      }
      return (double)getDiffSolrLastCrawl()/(double)solrCount*100d;
    }

  }

}
