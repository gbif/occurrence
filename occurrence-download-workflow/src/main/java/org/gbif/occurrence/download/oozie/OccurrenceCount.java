package org.gbif.occurrence.download.oozie;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.common.search.inject.SolrModule;
import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.download.util.RegistryClientUtil;
import org.gbif.service.guice.PrivateServiceModule;
import org.gbif.utils.file.properties.PropertiesUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.io.Closer;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * This class is being used at very first steps of the workflow to determine if the requested download is either "big"
 * or "small" download.
 * To calculate the download size contacts a SolrServer a executes a the Solr query that should be provided as jvm
 * argument.
 */
public class OccurrenceCount {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceSearchCountModule.class);

  // arbitrary record count that represents and error counting the records of the input query
  private static int ERROR_COUNT = -1;

  /**
   * Inner Guice Module: installs a SolrModule and only exposes instances of the OccurrenceCount class.
   * The properties file accepted should contain the prefix 'occurrence.download.' for the expected Solr configuration
   * settings and the
   * 'file.max_records' configuration.
   */
  private static final class OccurrenceSearchCountModule extends PrivateServiceModule {


    private static final String PREFIX = "occurrence.download.";
    private static final String REGISTRY_URL = "registry.ws.url";
    private final String regUrl;


    public OccurrenceSearchCountModule(Properties properties) {
      super(PREFIX, properties);
      regUrl = properties.getProperty(REGISTRY_URL);
    }

    @Override
    protected void configureService() {
      install(new SolrModule());
      bind(OccurrenceCount.class);
      expose(OccurrenceCount.class);
      expose(OccurrenceDownloadService.class);
    }

    @Provides
    @Singleton
    OccurrenceDownloadService provideOccurrenceDownloadService() {
      RegistryClientUtil registryClientUtil = new RegistryClientUtil(this.getVerbatimProperties());
      return registryClientUtil.setupOccurrenceDownloadService(regUrl);
    }

  }

  private static final String OOZIE_ACTION_OUTPUT_PROPERTIES = "oozie.action.output.properties";

  private static final String IS_SMALL_DOWNLOAD = "is_small_download";

  private static final String IS_SMALL_DOWNLOAD_KEY = "file.max_records";

  private final SolrServer solrServer;

  // Holds the value of the maximum number of records that a small download can have.
  private final int smallDownloadLimit;

  private final OccurrenceDownloadService occurrenceDownloadService;

  /**
   * Default/injectable constructor.
   */
  @Inject
  public OccurrenceCount(SolrServer solrServer, @Named(IS_SMALL_DOWNLOAD_KEY) int smallDownloadLimit,
    OccurrenceDownloadService occurrenceDownloadService) {
    this.solrServer = solrServer;
    this.smallDownloadLimit = smallDownloadLimit;
    this.occurrenceDownloadService = occurrenceDownloadService;
  }

  /**
   * Entry point: receives as argument the Solr query.
   */
  public static void main(String[] args) throws IOException {
    checkArgument(args.length > 0 || Strings.isNullOrEmpty(args[0]), "The solr query argument hasn't been specified");
    OccurrenceCount occurrenceCount = getInjector().getInstance(OccurrenceCount.class);
    occurrenceCount.updateDownloadData(args[0], args[1]);
  }

  /**
   * Utility method that creates a instance of a Guice Injector containing the OccurrenceSearchCountModule.
   */
  private static Injector getInjector() {
    try {
      return Guice.createInjector(new OccurrenceSearchCountModule(PropertiesUtil
        .loadProperties(RegistryClientUtil.OCC_PROPERTIES)));
    } catch (IllegalArgumentException e) {
      LOG.error("Error creating Guice module", e);
      Throwables.propagate(e);
    } catch (IOException e) {
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
   * @param solrQuery to be executed
   * @param workflowId oozie workflow id
   * @throws IOException in case of error reading or writing the 'oozie.action.output.properties' file
   */
  public void updateDownloadData(String solrQuery, String workflowId) throws IOException {
    final long recordCount = getRecordCount(solrQuery);
    updateWorkflowDownloadParameters(recordCount);
    if (recordCount >= 0) {
      updateTotalRecordsCount(workflowId, recordCount);
    }
  }


  /**
   * Sets the oozie action parameter 'is_small_download'.
   * 
   * @param solrQuery to be executed
   * @throws IOException in case of error reading or writing the 'oozie.action.output.properties' file
   */
  public void updateWorkflowDownloadParameters(long recordCount) throws IOException {
    String oozieProp = System.getProperty(OOZIE_ACTION_OUTPUT_PROPERTIES);
    Closer closer = Closer.create();
    if (oozieProp != null) {
      OutputStream os = null;
      File propFile = new File(oozieProp);
      Properties props = new Properties();
      try {
        os = closer.register(new FileOutputStream(propFile));
        props.setProperty(IS_SMALL_DOWNLOAD, isSmallDownloadCount(recordCount).toString());
        props.store(os, "");
      } catch (FileNotFoundException e) {
        LOG.error("Error reading properties file", e);
        closer.rethrow(e);
      } finally {
        closer.close();
      }
    } else {
      throw new IllegalStateException(OOZIE_ACTION_OUTPUT_PROPERTIES + " System property not defined");
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
