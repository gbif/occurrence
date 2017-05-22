package org.gbif.occurrence.cli.crawl;

import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.gbif.api.service.registry.DatasetProcessStatusService;
import org.gbif.cli.BaseCommand;
import org.gbif.cli.Command;
import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.occurrence.ws.client.OccurrenceWsClientModule;
import org.gbif.registry.ws.client.guice.RegistryWsClientModule;
import org.gbif.ws.client.guice.AnonymousAuthModule;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.type.TypeReference;
import org.kohsuke.MetaInfServices;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    if(!checkConfiguration()) {
      return;
    }

    PreviousCrawlsOccurrenceDeleter deletePreviousCrawlsService = null;
    MessagePublisher messagePublisher = null;

    if(config.delete || config.forceDelete) {
      messagePublisher = buildMessagePublisher();
      deletePreviousCrawlsService = new PreviousCrawlsOccurrenceDeleter(config,
              messagePublisher);
    }

    // Create WS Clients
    Properties properties = new Properties();
    properties.setProperty("registry.ws.url", config.registryWsUrl);
    properties.setProperty("occurrence.ws.url", config.registryWsUrl);
    properties.setProperty("httpTimeout", "30000");

    Injector injector = Guice.createInjector(new RegistryWsClientModule(properties),
            new AnonymousAuthModule(),
            new OccurrenceWsClientModule(properties));

    PreviousCrawlsManager previousCrawlsManager = new PreviousCrawlsManager(config,
            injector.getInstance(DatasetProcessStatusService.class),
            injector.getInstance(OccurrenceSearchService.class),
            deletePreviousCrawlsService);
    previousCrawlsManager.execute(this::printReportToJson);

    if(messagePublisher != null) {
      messagePublisher.close();
    }
  }

  /**
   * Makes configuration check on multiple parameters.
   *
   * @return
   */
  private boolean checkConfiguration() {
    if (config.delete && config.forceDelete) {
      System.err.println("--delete and --force-delete flags can not be used together");
      return false;
    }

    if (!config.displayReport && StringUtils.isBlank(config.reportLocation)) {
      System.err.println("--displayReport or --report-location flag must be specified");
      return false;
    }

    return true;
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

      if (StringUtils.isNotBlank(config.reportLocation)) {
        om.writeValue(Paths.get(config.reportLocation).toFile(), report);
      }

      if(config.displayReport) {
        System.out.print(om.writeValueAsString(report));
      }
    } catch (IOException e) {
      LOG.error("Failed to write report.", e);
    }
  }

  public static void main(String[] args) {
    PreviousCrawlsManagerCommand pcmc = new PreviousCrawlsManagerCommand();
    pcmc.startFromDisk("/Users/cgendreau/Documents/SourceCode/occurrence/deletion-status-reports/extended_report_may19.json");
  }

  public void startFromDisk(String reportLocation) {
    ObjectMapper om = new ObjectMapper();
    om.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    try {
      Map<UUID, DatasetRecordCountInfo> allDatasetWithMoreThanOneCrawl =
              om.readValue(new File(reportLocation), new TypeReference<Map<UUID, DatasetRecordCountInfo>>() {});
      analyseReport(allDatasetWithMoreThanOneCrawl);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void analyseReport(Map<UUID, DatasetRecordCountInfo> allDatasetWithMoreThanOneCrawl) {
//    long allRecordsToDelete = allDatasetWithMoreThanOneCrawl.entrySet()
//            .stream()
//            .filter( e -> e.getValue().getDiffSolrLastCrawlPercentage() < config.automaticRecordDeletionThreshold)
//            .mapToLong( e-> e.getValue().getSumAllPreviousCrawl())
//            .sum();
//
//    List<DatasetRecordCountInfo> test = allDatasetWithMoreThanOneCrawl.entrySet()
//            .stream()
//            .map( t -> t.getValue())
//            .filter( e -> e.getFragmentEmittedCount() != e.getFragmentProcessCount())
//            .collect(toList());
//
//    long allRecordsToDeleteNotAutomatic = allDatasetWithMoreThanOneCrawl.entrySet()
//            .stream()
//            .filter( e -> e.getValue().getDiffSolrLastCrawlPercentage() >= config.automaticRecordDeletionThreshold)
//            .mapToLong( e-> e.getValue().getSumAllPreviousCrawl())
//            .sum();
//
//    long datasetsInvolded = allDatasetWithMoreThanOneCrawl.entrySet()
//            .stream()
//            .filter( e -> e.getValue().getDiffSolrLastCrawlPercentage() < config.automaticRecordDeletionThreshold)
//            .count();
//
//    long datasetsTooHigh = allDatasetWithMoreThanOneCrawl.entrySet()
//            .stream()
//            .filter( e -> e.getValue().getDiffSolrLastCrawlPercentage() >= config.automaticRecordDeletionThreshold)
//            .count();
//
//    long highestDiff = allDatasetWithMoreThanOneCrawl.entrySet()
//            .stream()
//            .mapToLong( e-> e.getValue().getDiffSolrLastCrawl())
//            .max().getAsLong();
//
//    System.out.println("ALL Datasets count (available for autodeletion or not): " + allDatasetWithMoreThanOneCrawl.keySet().size());
//    System.out.println("all records available for autodeletion: " + allRecordsToDelete);
//    System.out.println("all records NOT available for autodeletion: " + allRecordsToDeleteNotAutomatic);
//    System.out.println("number of dataset with records available for autodeletion: " + datasetsInvolded);
//    System.out.println("number of dataset with records NOT available for autodeletion: " + datasetsTooHigh);
//    System.out.println("highestDiff: " + highestDiff);
  }

}





