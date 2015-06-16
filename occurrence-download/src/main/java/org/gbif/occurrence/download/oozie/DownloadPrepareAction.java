package org.gbif.occurrence.download.oozie;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
import org.gbif.occurrence.download.query.HiveQueryVisitor;
import org.gbif.occurrence.download.query.QueryBuildingException;
import org.gbif.occurrence.download.query.SolrQueryVisitor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
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
 * This class sets the following parameters required by the download workflow:
 * - is_small_download: define if the occurrence download must be processed as a small(Solr) or a big (Hive) download.\
 * This parameter is calculated by executing a Solr query that counts the number of records.
 * - solr_query: query to process small download, it's a translation of the predicate filter.
 * - hive_query: query to process big download, it's a translation of the predicate filter.
 * - hive_db: this parameter is read from a properties file.
 * - download_key: download primary key, it's generated from the Oozie workflow id.
 * - download_table_name: base name to use when creating hive tables and files, it's the download_key, but the '-'
 * it's replaced by '_'.
 */
public class DownloadPrepareAction {

  private static final Logger LOG = LoggerFactory.getLogger(DownloadPrepareAction.class);

  // arbitrary record count that represents and error counting the records of the input query
  private static final int ERROR_COUNT = -1;

  private static final ObjectMapper OBJECT_MAPPER =
    new ObjectMapper().configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  private static final String OOZIE_ACTION_OUTPUT_PROPERTIES = "oozie.action.output.properties";

  private static final String IS_SMALL_DOWNLOAD = "is_small_download";

  private static final String SOLR_QUERY = "solr_query";

  private static final String HIVE_DB = "hive_db";

  private static final String HIVE_QUERY = "hive_query";

  private static final String DOWNLOAD_KEY = "download_key";

  //'-' is not allowed in a Hive table name.
  // This value will hold the same value as the DOWNLOAD_KEY but the - is replaced by an '_'.
  private static final String DOWNLOAD_TABLE_NAME = "download_table_name";

  private final SolrServer solrServer;

  // Holds the value of the maximum number of records that a small download can have.
  private final int smallDownloadLimit;

  private final OccurrenceDownloadService occurrenceDownloadService;

  private final WorkflowConfiguration workflowConfiguration;

  /**
   * Entry point: receives as argument the predicate filter and the Oozie workflow id.
   */
  public static void main(String[] args) throws Exception {
    checkArgument(args.length > 0 || Strings.isNullOrEmpty(args[0]), "The solr query argument hasn't been specified");
    DownloadPrepareAction occurrenceCount = getInjector().getInstance(DownloadPrepareAction.class);
    occurrenceCount.updateDownloadData(args[0], DownloadUtils.workflowToDownloadId(args[1]));
  }

  /**
   * Utility method that creates a instance of a Guice Injector containing the OccurrenceSearchCountModule.
   */
  private static Injector getInjector() {
    try {
      return Guice.createInjector(new DownloadWorkflowModule(new WorkflowConfiguration()));
    } catch (IllegalArgumentException e) {
      LOG.error("Error creating Guice module", e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Default/injectable constructor.
   */
  @Inject
  public DownloadPrepareAction(
    SolrServer solrServer,
    @Named(DownloadWorkflowModule.DefaultSettings.MAX_RECORDS_KEY) int smallDownloadLimit,
    OccurrenceDownloadService occurrenceDownloadService,
    WorkflowConfiguration workflowConfiguration
  ) {
    this.solrServer = solrServer;
    this.smallDownloadLimit = smallDownloadLimit;
    this.occurrenceDownloadService = occurrenceDownloadService;
    this.workflowConfiguration = workflowConfiguration;
  }

  /**
   * Method that determines if the Solr Query produces a "small" download file.
   */
  public Boolean isSmallDownloadCount(long recordCount) {
    return recordCount != ERROR_COUNT && recordCount <= smallDownloadLimit;
  }

  /**
   * Update the oozie workflow data/parameters and persists the record of the occurrence download.
   *
   * @param rawPredicate to be executed
   * @param downloadKey  workflow id
   *
   * @throws java.io.IOException in case of error reading or writing the 'oozie.action.output.properties' file
   */
  public void updateDownloadData(String rawPredicate, String downloadKey) throws IOException, QueryBuildingException {
    Predicate predicate = OBJECT_MAPPER.readValue(rawPredicate, Predicate.class);
    String solrQuery = new SolrQueryVisitor().getQuery(predicate);
    final long recordCount = getRecordCount(solrQuery);
    String oozieProp = System.getProperty(OOZIE_ACTION_OUTPUT_PROPERTIES);
    if (oozieProp != null) {
      File propFile = new File(oozieProp);
      Properties props = new Properties();
      try (OutputStream os = new FileOutputStream(propFile)) {
        props.setProperty(IS_SMALL_DOWNLOAD, isSmallDownloadCount(recordCount).toString());
        props.setProperty(SOLR_QUERY, solrQuery);
        props.setProperty(HIVE_QUERY, StringEscapeUtils.escapeXml10(new HiveQueryVisitor().getHiveQuery(predicate)));
        props.setProperty(DOWNLOAD_KEY, downloadKey);
        // '-' is replaced by '_' because it's not allowed in hive table names
        props.setProperty(DOWNLOAD_TABLE_NAME, downloadKey.replaceAll("-", "_"));
        props.setProperty(HIVE_DB, workflowConfiguration.getHiveDb());
        props.store(os, "");
      } catch (FileNotFoundException e) {
        LOG.error("Error reading properties file", e);
        throw Throwables.propagate(e);
      }
    } else {
      throw new IllegalStateException(OOZIE_ACTION_OUTPUT_PROPERTIES + " System property not defined");
    }
    if (recordCount >= 0) {
      updateTotalRecordsCount(downloadKey, recordCount);
    }
  }

  /**
   * Executes the Solr query and returns the number of records found.
   * If an error occurs 'ERROR_COUNT' is returned.
   */
  private long getRecordCount(String solrQuery) {
    try {
      QueryResponse response = solrServer.query(new SolrQuery(solrQuery));
      return response.getResults().getNumFound();
    } catch (Exception e) {
      LOG.error("Error getting the records count", e);
      return ERROR_COUNT;
    }
  }

  /**
   * Updates the record count of the download entity.
   */
  private void updateTotalRecordsCount(String downloadKey, long recordCount) {
    try {
      LOG.info("Updating record count({}) of download {}", recordCount, downloadKey);
      Download download = occurrenceDownloadService.get(downloadKey);
      if (download == null) {
        LOG.error("Download {} was not found!", downloadKey);
      } else {
        download.setTotalRecords(recordCount);
        occurrenceDownloadService.update(download);
      }
    } catch (Exception ex) {
      LOG.error(String.format("Error updating record count for download workflow %s, reported count is %,d",
                              downloadKey,
                              recordCount), ex);
    }
  }

}
