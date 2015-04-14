package org.gbif.occurrence.download.oozie;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
import org.gbif.occurrence.download.query.HiveQueryVisitor;
import org.gbif.occurrence.download.query.QueryBuildingException;
import org.gbif.occurrence.download.query.SolrQueryVisitor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Properties;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.name.Named;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * This class is being used at very first steps of the workflow to determine if the requested download is either "big"
 * or "small" download.
 * To calculate the download size contacts a SolrServer a executes a the Solr query that should be provided as jvm
 * argument.
 */
public class DownloadPrepareStep {

  private static final Logger LOG = LoggerFactory.getLogger(DownloadPrepareStep.class);

  // arbitrary record count that represents and error counting the records of the input query
  private static final int ERROR_COUNT = -1;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  private static final String OOZIE_ACTION_OUTPUT_PROPERTIES = "oozie.action.output.properties";

  private static final String IS_SMALL_DOWNLOAD = "is_small_download";

  private static final String SOLR_QUERY = "solr_query";

  private static final String HIVE_QUERY = "hive_query";

  private final SolrServer solrServer;

  // Holds the value of the maximum number of records that a small download can have.
  private final int smallDownloadLimit;

  private final OccurrenceDownloadService occurrenceDownloadService;

  /**
   * Default/injectable constructor.
   */
  @Inject
  public DownloadPrepareStep(
    SolrServer solrServer,
    @Named(DownloadWorkflowModule.DefaultSettings.MAX_RECORDS_KEY) int smallDownloadLimit,
    OccurrenceDownloadService occurrenceDownloadService
  ) {
    this.solrServer = solrServer;
    this.smallDownloadLimit = smallDownloadLimit;
    this.occurrenceDownloadService = occurrenceDownloadService;
  }

  /**
   * Entry point: receives as argument the Solr query.
   */
  public static void main(String[] args) throws Exception {
    checkArgument(args.length > 0 || Strings.isNullOrEmpty(args[0]), "The solr query argument hasn't been specified");
    DownloadPrepareStep occurrenceCount = getInjector().getInstance(DownloadPrepareStep.class);
    occurrenceCount.updateDownloadData(args[0], args[1]);
  }

  /**
   * Utility method that creates a instance of a Guice Injector containing the OccurrenceSearchCountModule.
   */
  private static Injector getInjector() {
    try {
      return Guice.createInjector(new DownloadWorkflowModule());
    } catch (IllegalArgumentException e) {
      LOG.error("Error creating Guice module", e);
      Throwables.propagate(e);
    }
    throw new IllegalStateException("Error initializing occurrence count guice module");
  }


  /**
   * Method that determines if the Solr Query produces a "small" download file.
   */
  public Boolean isSmallDownloadCount(long recordCount) {
    return recordCount != ERROR_COUNT && recordCount <= smallDownloadLimit;
  }


  /**
   * Executes the Solr query and returns the number of records found.
   * If an error occurs 'ERROR_COUNT' is returned.
   */
  public long getRecordCount(String solrQuery) {
    try {
      QueryResponse response = solrServer.query(new SolrQuery(solrQuery));
      return response.getResults().getNumFound();
    } catch (Exception e) {
      LOG.error("Error getting the records count", e);
      return ERROR_COUNT;
    }
  }


  /**
   * Update the oozie workflow data/parameters and persists the record of the occurrence download.
   *
   * @param rawPredicate to be executed
   * @param workflowId oozie workflow id
   * @throws java.io.IOException in case of error reading or writing the 'oozie.action.output.properties' file
   */
  public void updateDownloadData(String rawPredicate, String workflowId) throws IOException, QueryBuildingException {
    Predicate predicate = OBJECT_MAPPER.readValue(rawPredicate,Predicate.class);
    String solrQuery = new SolrQueryVisitor().getQuery(predicate);
    final long recordCount = getRecordCount(solrQuery);
    String oozieProp = System.getProperty(OOZIE_ACTION_OUTPUT_PROPERTIES);
    if (oozieProp != null) {
      File propFile = new File(oozieProp);
      Properties props = new Properties();
      try (OutputStream os = new FileOutputStream(propFile)) {
        props.setProperty(IS_SMALL_DOWNLOAD, isSmallDownloadCount(recordCount).toString());
        props.setProperty(SOLR_QUERY,solrQuery);
        props.setProperty(HIVE_QUERY,StringEscapeUtils.escapeXml10(new HiveQueryVisitor().getHiveQuery(predicate)));
        props.store(os, "");
      } catch (FileNotFoundException e) {
        LOG.error("Error reading properties file", e);
        Throwables.propagate(e);
      }
    } else {
      throw new IllegalStateException(OOZIE_ACTION_OUTPUT_PROPERTIES + " System property not defined");
    }
    if (recordCount >= 0) {
      updateTotalRecordsCount(workflowId, recordCount);
    }
  }

  /**
   * Updates the record count of the download entity.
   */
  private void updateTotalRecordsCount(String workflowId, long recordCount) {
    try {
      String downloadId = DownloadUtils.workflowToDownloadId(workflowId);
      LOG.info("Updating record count({}) of download {}", recordCount, downloadId);
      Download download = occurrenceDownloadService.get(downloadId);
      if (download == null) {
        LOG.error("Download {} was not found!", downloadId);
      } else {
        download.setTotalRecords(recordCount);
        occurrenceDownloadService.update(download);
      }
    } catch (Exception ex) {
      LOG.error(String.format("Error updating record count for download worflow %s, reported count is %,d",
        workflowId, recordCount), ex);
    }
  }

}
