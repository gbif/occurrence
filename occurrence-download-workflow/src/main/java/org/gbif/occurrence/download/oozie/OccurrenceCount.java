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

import static com.google.common.base.Preconditions.checkArgument;

/**
 * This class is being used at very first steps of the workflow to determine if the requested download is either "big"
 * or "small" download.
 * To calculate the download size contacts a SolrServer a executes a the Solr query that should be provided as jvm
 * argument.
 */
public class OccurrenceCount {

  /**
   * Inner Guice Module: installs a SolrModule and only exposes instances of the OccurrenceCount class.
   * The properties file accepted should contain the prefix 'occurrence-download.' for the expected Solr configuration
   * settings and the
   * 'file.max_records' configuration.
   */
  private static final class OccurrenceSearchCountModule extends PrivateServiceModule {


    private static final String PREFIX = "occurrence-download.";


    public OccurrenceSearchCountModule(Properties properties) {
      super(PREFIX, properties);
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
    OccurrenceDownloadService provideOccurrenceDownloadService(
      @Named("registry.ws.url") String registryWsUri) {
      RegistryClientUtil registryClientUtil = new RegistryClientUtil(this.getVerbatimProperties());
      return registryClientUtil.setupOccurrenceDownloadService(registryWsUri);
    }

  }

  private static final String OOZIE_ACTION_OUTPUT_PROPERTIES = "oozie.action.output.properties";

  private static final String IS_SMALL_DOWNLOAD = "is_small_download";

  private static final String OCC_PROPERTIES = "occurrence-download.properties";

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
    occurrenceCount.setDownloadLimitWorkflowFlag(args[0], args[1]);
  }

  /**
   * Utility method that creates a instance of a Guice Injector containing the OccurrenceSearchCountModule.
   */
  private static Injector getInjector() {
    try {
      return Guice.createInjector(new OccurrenceSearchCountModule(PropertiesUtil.loadProperties(OCC_PROPERTIES)));
    } catch (IllegalArgumentException e) {
      System.err.println("Error creating Guice module");
      e.printStackTrace();
      Throwables.propagate(e);
    } catch (IOException e) {
      System.err.println("Error creating Guice module");
      e.printStackTrace();
      Throwables.propagate(e);
    }
    throw new IllegalStateException("Error initializing occurrence count guice module");
  }


  /**
   * Method that determines if the Solr Query produces a "small" download file.
   */
  public Boolean isSmallDownloadCount(String solrQuery, String workflowId) {
    try {
      QueryResponse response = solrServer.query(new SolrQuery(solrQuery));
      long recordCount = response.getResults().getNumFound();
      updateTotalRecordsCount(workflowId, recordCount);
      return recordCount <= smallDownloadLimit;
    } catch (Exception e) {
      System.err.println("Error getting the records count");
      e.printStackTrace();
      return false;
    }
  }

  /**
   * Sets the oozie action parameter 'is_small_download'.
   *
   * @param solrQuery to be executed
   * @throws IOException in case of error reading or writing the 'oozie.action.output.properties' file
   */
  public void setDownloadLimitWorkflowFlag(String solrQuery, String workflowId) throws IOException {
    String oozieProp = System.getProperty(OOZIE_ACTION_OUTPUT_PROPERTIES);
    Closer closer = Closer.create();
    if (oozieProp != null) {
      OutputStream os = null;
      File propFile = new File(oozieProp);
      Properties props = new Properties();
      try {
        os = closer.register(new FileOutputStream(propFile));
        props.setProperty(IS_SMALL_DOWNLOAD, isSmallDownloadCount(solrQuery, workflowId).toString());
        props.store(os, "");
      } catch (FileNotFoundException e) {
        System.err.println("Error reading properties file");
        e.printStackTrace();
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
      System.out.println(String.format("Updating record count(%,d) of download %s", recordCount, downloadId));
      Download download = occurrenceDownloadService.get(downloadId);
      if (download == null) {
        System.err.println(String.format("Download %s was not found!", downloadId));
      } else {
        download.setTotalRecords(recordCount);
        occurrenceDownloadService.update(download);
      }
    } catch (Exception ex) {
      System.err.println(String.format("Error updating record count for download worflow %s, reported count is %,d",
        workflowId, recordCount));
      ex.printStackTrace();
    }
  }

}
