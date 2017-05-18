package org.gbif.occurrence.cli.crawl;

import org.gbif.cli.BaseCommand;
import org.gbif.cli.Command;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.api.MessagePublisher;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.type.TypeReference;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toList;

/**
 * Commands to manage occurrence record from previous crawls.
 */
@MetaInfServices(Command.class)
public class PreviousCrawlsManagerCommand extends BaseCommand {

  private static final Logger LOG = LoggerFactory.getLogger(PreviousCrawlsManagerCommand.class);
  private final PreviousCrawlsManagerConfiguration config = new PreviousCrawlsManagerConfiguration();

  public PreviousCrawlsManagerCommand() {
    super("previous-crawls-manager");
  }

  @Override
  protected Object getConfigurationObject() {
    return config;
  }

  @Override
  protected void doRun() {
    MessagePublisher messagePublisher = buildMessagePublisher();
    DeletePreviousCrawlsService deletePreviousCrawlsService = new DeletePreviousCrawlsService(config, messagePublisher);

    PreviousCrawlsManagerService checkPreviousCrawlsService = new PreviousCrawlsManagerService(config, deletePreviousCrawlsService);
    checkPreviousCrawlsService.start(this::printReportToJson);
  }

  private MessagePublisher buildMessagePublisher() {
    MessagePublisher publisher = null;
    try {
      publisher = new DefaultMessagePublisher(config.messaging.getConnectionParameters());
    } catch (IOException e) {
      LOG.error("Error while building DefaultMessagePublisher", e);
    }
    return publisher;
  }

  private void printReportToJson(Object report) {
    ObjectMapper om = new ObjectMapper();
    om.configure(SerializationConfig.Feature.INDENT_OUTPUT, true);
    try {
      om.writeValue(Paths.get(config.reportLocation).toFile(), report);

      if(config.displayReport) {
        System.out.print(om.writeValueAsString(report));
      }
    } catch (IOException e) {
      LOG.error("Failed to write report.", e);
    }
  }

  public static void main(String[] args) {
    PreviousCrawlsManagerCommand pcmc = new PreviousCrawlsManagerCommand();
    pcmc.startFromDisk("/Users/cgendreau/Documents/SourceCode/occurrence/deletion-status-reports/extended_report_may18.json");
  }

  public void startFromDisk(String reportLocation) {
    ObjectMapper om = new ObjectMapper();
    om.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    try {
      Map<UUID, PreviousCrawlsManagerService.DatasetRecordCountInfo> allDatasetWithMoreThanOneCrawl =
              om.readValue(new File(reportLocation), new TypeReference<Map<UUID, PreviousCrawlsManagerService.DatasetRecordCountInfo>>() {});
      analyseReport(allDatasetWithMoreThanOneCrawl);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  public void analyseReport(Map<UUID, PreviousCrawlsManagerService.DatasetRecordCountInfo> allDatasetWithMoreThanOneCrawl) {
    long allRecordsToDelete = allDatasetWithMoreThanOneCrawl.entrySet()
            .stream()
            .filter( e -> e.getValue().getDiffSolrLastCrawlPercentage() < config.automaticRecordDeletionThreshold)
            .mapToLong( e-> e.getValue().getSumAllPreviousCrawl())
            .sum();

    List<PreviousCrawlsManagerService.DatasetRecordCountInfo> test = allDatasetWithMoreThanOneCrawl.entrySet()
            .stream()
            .map( t -> t.getValue())
            .filter( e -> e.getFragmentEmittedCount() != e.getFragmentProcessCount())
            .collect(toList());

    long allRecordsToDeleteNotAutomatic = allDatasetWithMoreThanOneCrawl.entrySet()
            .stream()
            .filter( e -> e.getValue().getDiffSolrLastCrawlPercentage() >= config.automaticRecordDeletionThreshold)
            .mapToLong( e-> e.getValue().getSumAllPreviousCrawl())
            .sum();

    long datasetsInvolded = allDatasetWithMoreThanOneCrawl.entrySet()
            .stream()
            .filter( e -> e.getValue().getDiffSolrLastCrawlPercentage() < config.automaticRecordDeletionThreshold)
            .count();

    long datasetsTooHigh = allDatasetWithMoreThanOneCrawl.entrySet()
            .stream()
            .filter( e -> e.getValue().getDiffSolrLastCrawlPercentage() >= config.automaticRecordDeletionThreshold)
            .count();

    long highestDiff = allDatasetWithMoreThanOneCrawl.entrySet()
            .stream()
            .mapToLong( e-> e.getValue().getDiffSolrLastCrawl())
            .max().getAsLong();

    System.out.println("ALL Datasets count (available for autodeletion or not): " + allDatasetWithMoreThanOneCrawl.keySet().size());
    System.out.println("all records available for autodeletion: " + allRecordsToDelete);
    System.out.println("all records NOT available for autodeletion: " + allRecordsToDeleteNotAutomatic);
    System.out.println("number of dataset with records available for autodeletion: " + datasetsInvolded);
    System.out.println("number of dataset with records NOT available for autodeletion: " + datasetsTooHigh);
    System.out.println("highestDiff: " + highestDiff);
  }

}





