package org.gbif.occurrence.cli.crawl;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service that generates a crawls report as JSON using Hive on JDBC.
 */
public class CrawlReportGeneratorService {

  private static final Logger LOG = LoggerFactory.getLogger(CrawlReportGeneratorService.class);
  private CrawlsReportGeneratorConfiguration config;

  private static final String SQL = "SELECT datasetkey, crawlid, count(*) FROM %s GROUP BY datasetkey, " +
          "crawlid ORDER BY datasetkey, crawlid";
  private static final int DATASET_KEY_IDX = 1;
  private static final int CRAWL_ID_IDX = 2;
  private static final int COUNT_IDX = 3;

  public CrawlReportGeneratorService(CrawlsReportGeneratorConfiguration config) {
    this.config = config;
  }

  public void start() {
    String sql = String.format(SQL, config.hiveOccurrenceTable);

    Map<String, List<DatasetCrawlInfo>> crawlInfo = new HashMap<>();
    try (Connection conn = config.hive.buildHiveConnection();
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {

      while (rs.next()) {
        crawlInfo.putIfAbsent(rs.getString(DATASET_KEY_IDX), new ArrayList<>());
        crawlInfo.get(rs.getString(DATASET_KEY_IDX)).add(new DatasetCrawlInfo(rs.getInt(CRAWL_ID_IDX), rs.getInt(COUNT_IDX)));
      }

      ObjectMapper om = new ObjectMapper();
      om.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
      om.writeValue(Paths.get(config.reportLocation).toFile(), crawlInfo);
    } catch (SQLException | IOException e) {
      LOG.error("Error while generating the crawls report", e);
    }
  }
}
