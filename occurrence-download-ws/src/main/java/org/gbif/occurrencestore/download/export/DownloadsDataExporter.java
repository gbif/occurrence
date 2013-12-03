package org.gbif.occurrencestore.download.export;

import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.common.search.util.SolrConstants;
import org.gbif.occurrence.download.util.DownloadUtils;
import org.gbif.occurrencestore.download.service.Constants;
import org.gbif.occurrencestore.download.service.QueryBuildingException;
import org.gbif.occurrencestore.download.service.SolrQueryVisitor;
import org.gbif.occurrencestore.search.solr.OccurrenceSolrField;
import org.gbif.utils.file.FileUtils;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.List;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.codehaus.jackson.map.DeserializationConfig.Feature;
import org.codehaus.jackson.map.ObjectMapper;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

/**
 * Utility class that exports the downloads information from Oozie and Solr.
 * 2 tab separated file are generated: one containing the downloads information and second one with dataset usages.
 * The resulting files can be imported into PostgresSQL by using the command COPY, for example:
 * COPY occurrence_download FROM 'downloads.txt';
 * COPY dataset_occurrence_download FROM 'datasetusages.txt';
 */
public class DownloadsDataExporter {

  /**
   * Simple Guice module that creates the clients required.
   */
  private static class DownloadMetadataImporterModule extends AbstractModule {

    private final String oozieUrl;
    private final String solrUrl;

    public DownloadMetadataImporterModule(String oozieUrl, String solrUrl) {
      this.oozieUrl = oozieUrl;
      this.solrUrl = solrUrl;
    }

    @Override
    protected void configure() {
      bind(SolrServer.class).toInstance(new HttpSolrServer(solrUrl));
      bind(OozieClient.class).toInstance(new OozieClient(oozieUrl));
    }
  }

  private static final String OOZIE_FILTER = "name=" + Constants.OOZIE_USER;
  private static final String DT_FMT = "yyyy-MM-dd HH:mm:ss.SSSSSZ";
  private static final int DEFAULT_LIMIT = 30;
  private static final String ZIP_EXT = ".zip";

  private static final ObjectMapper objectMapper = new ObjectMapper().configure(Feature.FAIL_ON_UNKNOWN_PROPERTIES,
    false);

  /**
   * Map to provide conversions from oozie.Job.Status to Download.Status.
   */
  private static final ImmutableMap<WorkflowJob.Status, Download.Status> STATUSES_MAP =
    new ImmutableMap.Builder<WorkflowJob.Status, Download.Status>()
      .put(WorkflowJob.Status.PREP, Download.Status.PREPARING)
      .put(WorkflowJob.Status.RUNNING, Download.Status.RUNNING)
      .put(WorkflowJob.Status.KILLED, Download.Status.KILLED)
      .put(WorkflowJob.Status.FAILED, Download.Status.FAILED)
      .put(WorkflowJob.Status.SUCCEEDED, Download.Status.SUCCEEDED)
      .put(WorkflowJob.Status.SUSPENDED, Download.Status.SUSPENDED).build();


  /**
   * Executable method that creates the files.
   */
  public static void export(String oozieUrl, String solrUrl, String downloadsFile, String datasetUsageFile)
    throws IOException {
    Injector injector = Guice.createInjector(new DownloadMetadataImporterModule(oozieUrl, solrUrl));
    createDownloadsFile(injector.getInstance(OozieClient.class), injector.getInstance(SolrServer.class),
      downloadsFile, datasetUsageFile);
  }

  /**
   * Exports the occurrence downlods information from oozie into a tab separated file.
   */
  public static void main(String[] args) throws Exception {
    export(args[0], args[1], args[2], args[3]);
  }

  /**
   * Creates the downloads file.
   */
  private static void createDownloadsFile(OozieClient oozieClient, SolrServer solrServer, String downloadsFileName,
    String datasetUsageFile) throws IOException {
    int offset = 1;
    int limit = DEFAULT_LIMIT;
    Closer closer = Closer.create();
    try {
      Writer downloadsWriter =
        new BufferedWriter(new OutputStreamWriter(new FileOutputStream(downloadsFileName), FileUtils.UTF8));
      Writer datasetUsageWriter =
        new BufferedWriter(new OutputStreamWriter(new FileOutputStream(datasetUsageFile), FileUtils.UTF8));
      closer.register(downloadsWriter);
      closer.register(datasetUsageWriter);
      List<WorkflowJob> jobs = oozieClient.getJobsInfo(OOZIE_FILTER, offset, limit);
      while (jobs.size() > 0) {
        for (WorkflowJob job : jobs) {
          // The job is retrieved again form Oozie to get the full configuration data
          Download download = fromJob(oozieClient.getJobInfo(job.getId()));
          downloadsWriter.write(toFileLine(download));
          writeDatasetUsage(solrServer, download, datasetUsageFile, datasetUsageWriter);
        }
        offset += Math.min(limit, jobs.size());
        jobs = oozieClient.getJobsInfo(OOZIE_FILTER, offset, limit);
      }
    } catch (Exception e) {
      throw new IllegalStateException(e);
    } finally {
      closer.close();
    }
  }


  /**
   * Executes a search using a predicate.
   */
  private static QueryResponse executePredicateQuery(SolrServer solrServer, Predicate predicate) {
    try {
      SolrQueryVisitor solrQueryVisitor = new SolrQueryVisitor();
      String query = solrQueryVisitor.getQuery(predicate);
      SolrQuery solrQuery =
        new SolrQuery()
          .setQuery(SolrConstants.DEFAULT_QUERY).setRows(0)
          .setFacet(true).setFacetMinCount(1).setFacetMissing(false).setFacetLimit(-1)
          .addFacetField(OccurrenceSolrField.DATASET_KEY.getFieldName());
      if (!Strings.isNullOrEmpty(query)) {
        solrQuery.addFilterQuery(query);
      }
      QueryResponse queryResponse = solrServer.query(solrQuery);
      return queryResponse;
    } catch (SolrServerException e) {
      throw new IllegalStateException(e);
    } catch (QueryBuildingException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Builds a download object from an oozie job.
   */
  private static Download fromJob(WorkflowJob job) {
    try {
      String downloadKey = DownloadUtils.workflowToDownloadId(job.getId());
      Download download = new Download();
      download.setRequest(parseDownloadRequest(job));
      download.setCreated(job.getCreatedTime());
      download.setDownloadLink(downloadKey + ZIP_EXT);
      download.setModified(job.getLastModifiedTime());
      download.setKey(downloadKey);
      download.setStatus(STATUSES_MAP.get(job.getStatus()));
      return download;
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to deserialize download filter for job " + job.getId(), e);
    }
  }

  /**
   * Builds a {@link DownloadRequest} object by parsing the job xml configuration.
   */
  private static DownloadRequest parseDownloadRequest(WorkflowJob job) {
    try {
      SAXParserFactory saxFactory = SAXParserFactory.newInstance();
      saxFactory.setNamespaceAware(true);
      saxFactory.setValidating(false);
      OozieJobSaxHandler handler = new OozieJobSaxHandler();
      SAXParser parser = saxFactory.newSAXParser();
      XMLReader xmlReader = parser.getXMLReader();
      xmlReader.setContentHandler(handler);
      xmlReader.parse(new InputSource(new StringReader(job.getConf())));
      return handler.buildDownload();
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to deserialize download filter for job " + job.getId(), e);
    }
  }


  /**
   * Creates a String with the the downloads fields separated by the tab character.
   */
  private static String toFileLine(Download download) throws Exception {
    SimpleDateFormat dateFormat = new SimpleDateFormat(DT_FMT);
    return download.getKey() + '\t'
      + objectMapper.writeValueAsString(download.getRequest().getPredicate())
      + '\t' + download.getStatus() + '\t' + download.getDownloadLink()
      + '\t' + download.getRequest().getNotificationAddressesAsString() + '\t' + download.getRequest().getCreator()
      + '\t' + dateFormat.format(download.getCreated()) + '\t' + dateFormat.format(download.getModified()) + '\n';
  }

  /**
   * Executes the a Solr query using the download.request.predicate and collects the dataset usages for each download.
   * For each Solr facet a line is added to the file.
   */
  private static void writeDatasetUsage(SolrServer solrServer, Download download, String fileName,
    Writer datasetUsageWriter)
    throws IOException {
    try {
      QueryResponse response = executePredicateQuery(solrServer, download.getRequest().getPredicate());
      final FacetField facetField = response.getFacetField(OccurrenceSolrField.DATASET_KEY.getFieldName());
      if (facetField != null) {
        for (Count count : facetField.getValues()) {
          datasetUsageWriter.write(download.getKey() + '\t' + count.getName() + '\t' + count.getCount() + '\n');
        }
      }
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

}
