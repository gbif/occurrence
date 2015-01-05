/*
 * Copyright 2012 Global Biodiversity Information Facility (GBIF)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.download.service;

import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.HiveColumnsUtils;
import org.gbif.occurrence.common.TermUtils;
import org.gbif.occurrence.common.download.DownloadUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import com.google.common.base.Enums;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.sun.jersey.api.NotFoundException;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.occurrence.common.download.DownloadUtils.downloadLink;
import static org.gbif.occurrence.download.service.Constants.NOTIFY_ADMIN;

@Singleton
public class DownloadRequestServiceImpl implements DownloadRequestService, CallbackService {

  public static final EnumSet<Download.Status> RUNNING_STATUSES = EnumSet.of(Download.Status.PREPARING,
    Download.Status.RUNNING, Download.Status.SUSPENDED);

  /**
   * Map to provide conversions from oozie.Job.Status to Download.Status.
   */
  @VisibleForTesting
  protected static final ImmutableMap<Job.Status, Download.Status> STATUSES_MAP =
    new ImmutableMap.Builder<Job.Status, Download.Status>()
      .put(Job.Status.PREP, Download.Status.PREPARING)
      .put(Job.Status.PREPPAUSED, Download.Status.PREPARING)
      .put(Job.Status.PREMATER, Download.Status.PREPARING)
      .put(Job.Status.PREPSUSPENDED, Download.Status.SUSPENDED)
      .put(Job.Status.RUNNING, Download.Status.RUNNING)
      .put(Job.Status.KILLED, Download.Status.KILLED)
      .put(Job.Status.RUNNINGWITHERROR, Download.Status.RUNNING)
      .put(Job.Status.DONEWITHERROR, Download.Status.FAILED)
      .put(Job.Status.FAILED, Download.Status.FAILED)
      .put(Job.Status.PAUSED, Download.Status.RUNNING)
      .put(Job.Status.PAUSEDWITHERROR, Download.Status.RUNNING)
      .put(Job.Status.SUCCEEDED, Download.Status.SUCCEEDED)
      .put(Job.Status.SUSPENDED, Download.Status.SUSPENDED)
      .put(Job.Status.SUSPENDEDWITHERROR, Download.Status.SUSPENDED).build();

  private static final Logger LOG = LoggerFactory.getLogger(DownloadRequestServiceImpl.class);
  private static final Joiner EMAIL_JOINER = Joiner.on(';').skipNulls();
  private static final CharMatcher REPL_INVALID_HIVE_CHARS = CharMatcher.inRange('a', 'z')
    .or(CharMatcher.inRange('A', 'Z'))
    .or(CharMatcher.DIGIT)
    .or(CharMatcher.is('_'))
    .negate();

  private static final String HIVE_SELECT_INTERPRETED;
  private static final String HIVE_SELECT_VERBATIM;
  private static final String JOIN_ARRAY_FMT = "if(%1$s IS NULL,'',join_array(%1$s,';')) AS %1$s";

  static {
    List<String> columns = Lists.newArrayList();
    for (Term term : TermUtils.interpretedTerms()) {
      final String iCol = HiveColumnsUtils.getHiveColumn(term);
      if (TermUtils.isInterpretedDate(term)) {
        columns.add("toISO8601(" + iCol + ") AS " + iCol);
      } else if (TermUtils.isInterpretedNumerical(term) || TermUtils.isInterpretedDouble(term)) {
        columns.add("cleanNull(" + iCol + ") AS " + iCol);
      } else if (TermUtils.isInterpretedBoolean(term)) {
        columns.add(iCol);
      } else if (term == GbifTerm.issue) {
        // OccurrnceIssues are exposed as an String separate by ;
        columns.add(String.format(JOIN_ARRAY_FMT, iCol));
      } else if (term == GbifTerm.mediaType) {
        // OccurrnceIssues are exposed as an String separate by ;
        columns.add(String.format(JOIN_ARRAY_FMT, iCol));
      } else if (!TermUtils.isComplexType(term)) {
        // complex type fields are not added to the select statement
        columns.add("cleanDelimiters(" + iCol + ") AS " + iCol);
      }
    }
    HIVE_SELECT_INTERPRETED = Joiner.on(',').join(columns);


    columns = Lists.newArrayList();
    // manually add the GBIF occ key as first column
    columns.add(HiveColumnsUtils.getHiveColumn(GbifTerm.gbifID));
    for (Term term : TermUtils.verbatimTerms()) {
      if (GbifTerm.gbifID != term) {
        String colName = HiveColumnsUtils.getHiveColumn(term);
        columns.add("cleanDelimiters(v_" + colName + ") AS v_" + colName);
      }
    }
    HIVE_SELECT_VERBATIM = Joiner.on(',').join(columns);
  }

  private static final Counter SUCCESSFUL_DOWNLOADS = Metrics.newCounter(CallbackService.class, "successful_downloads");
  private static final Counter FAILED_DOWNLOADS = Metrics.newCounter(CallbackService.class, "failed_downloads");
  private static final Counter CANCELLED_DOWNLOADS = Metrics.newCounter(CallbackService.class, "cancelled_downloads");

  // ObjectMappers are thread safe if not reconfigured in code
  private final ObjectMapper mapper = new ObjectMapper();
  private final OozieClient client;
  private final Map<String, String> defaultProperties;
  private final String wsUrl;
  private final File downloadMount;
  private final OccurrenceDownloadService occurrenceDownloadService;
  private final DownloadEmailUtils downloadEmailUtils;


  @Inject
  public DownloadRequestServiceImpl(OozieClient client,
    @Named("oozie.default_properties") Map<String, String> defaultProperties,
    @Named("ws.url") String wsUrl,
    @Named("ws.mount") String wsMountDir,
    OccurrenceDownloadService occurrenceDownloadService,
    DownloadEmailUtils downloadEmailUtils) {

    this.client = client;
    this.defaultProperties = defaultProperties;
    this.wsUrl = wsUrl;
    this.downloadMount = new File(wsMountDir);
    this.occurrenceDownloadService = occurrenceDownloadService;
    this.downloadEmailUtils = downloadEmailUtils;

  }

  @Override
  public void cancel(String downloadKey) {
    try {
      Download download = occurrenceDownloadService.get(downloadKey);
      if (download != null) {
        if (RUNNING_STATUSES.contains(download.getStatus())) {
          updateDownloadStatus(download, Download.Status.CANCELLED);
          client.kill(DownloadUtils.downloadToWorkflowId(downloadKey));
          LOG.info("Download {} canceled", downloadKey);
        }
      } else {
        throw new NotFoundException(String.format("Download %s not found", downloadKey));
      }
    } catch (OozieClientException e) {
      throw new ServiceUnavailableException("Failed to cancel download " + downloadKey, e);
    }
  }

  @Override
  public String create(DownloadRequest request) {
    LOG.debug("Trying to create download from request [{}]", request);
    Preconditions.checkNotNull(request);

    HiveQueryVisitor hiveVisitor = new HiveQueryVisitor();
    SolrQueryVisitor solrVisitor = new SolrQueryVisitor();
    String hiveQuery;
    String solrQuery;
    try {
      hiveQuery = StringEscapeUtils.escapeXml(hiveVisitor.getHiveQuery(request.getPredicate()));
      solrQuery = solrVisitor.getQuery(request.getPredicate());
    } catch (QueryBuildingException e) {
      throw new ServiceUnavailableException("Error building the hive query, attempting to continue", e);
    }
    LOG.debug("Attempting to download with hive query [{}]", hiveQuery);

    final String uniqueId =
      REPL_INVALID_HIVE_CHARS.removeFrom(request.getCreator() + '_' + UUID.randomUUID().toString());
    final String tmpTable = "download_tmp_" + uniqueId;
    final String citationTable = "download_tmp_citation_" + uniqueId;

    Properties jobProps = new Properties();
    jobProps.putAll(defaultProperties);
    jobProps.setProperty("select_interpreted", HIVE_SELECT_INTERPRETED);
    jobProps.setProperty("select_verbatim", HIVE_SELECT_VERBATIM);
    jobProps.setProperty("query", hiveQuery);
    jobProps.setProperty("solr_query", solrQuery);
    jobProps.setProperty("query_result_table", tmpTable);
    jobProps.setProperty("citation_table", citationTable);
    // we dont have a downloadId yet, submit a placeholder
    jobProps.setProperty("download_link", downloadLink(wsUrl, DownloadUtils.DOWNLOAD_ID_PLACEHOLDER));
    jobProps.setProperty(Constants.USER_PROPERTY, request.getCreator());
    if (request.getNotificationAddresses() != null && !request.getNotificationAddresses().isEmpty()) {
      jobProps.setProperty(Constants.NOTIFICATION_PROPERTY, EMAIL_JOINER.join(request.getNotificationAddresses()));
    }
    // serialize the predicate filter into json
    StringWriter writer = new StringWriter();
    try {
      mapper.writeValue(writer, request.getPredicate());
      writer.flush();
      jobProps.setProperty(Constants.FILTER_PROPERTY, writer.toString());
    } catch (IOException e) {
      throw new ServiceUnavailableException("Failed to serialize download filter " + request.getPredicate(), e);
    }

    LOG.debug("job properties: {}", jobProps);

    try {
      final String jobId = client.run(jobProps);
      LOG.debug("oozie job id is: [{}], with tmpTable [{}]", jobId, tmpTable);
      String downloadId = DownloadUtils.workflowToDownloadId(jobId);
      persistDownload(request, downloadId);
      return downloadId;
    } catch (OozieClientException e) {
      throw new ServiceUnavailableException("Failed to create download job", e);
    }
  }


  @Override
  public InputStream getResult(String downloadKey) {
    Download d = occurrenceDownloadService.get(downloadKey);
    if (!d.isAvailable()) {
      throw new NotFoundException("Download " + downloadKey + " is not ready yet");
    }

    File localFile = new File(downloadMount, downloadKey + ".zip");
    try {
      return new FileInputStream(localFile);

    } catch (IOException e) {
      throw new IllegalStateException(
        "Failed to read download " + downloadKey + " from " + localFile.getAbsolutePath(), e);
    }
  }

  /**
   * Processes a callback from Oozie which update the download status.
   */
  public void processCallback(String jobId, String status) {
    Preconditions.checkNotNull(Strings.isNullOrEmpty(jobId), "<jobId> may not be null or empty");
    Preconditions.checkNotNull(Strings.isNullOrEmpty(status), "<status> may not be null or empty");
    final Optional<Job.Status> opStatus = Enums.getIfPresent(Job.Status.class, status.toUpperCase());
    Preconditions.checkArgument(opStatus.isPresent(), "<status> the requested status is not valid");
    String downloadId = DownloadUtils.workflowToDownloadId(jobId);

    LOG.debug("Processing callback for jobId [{}] with status [{}]", jobId, status);

    Download download = occurrenceDownloadService.get(downloadId);
    if (download == null) {
      // Download can be null if the oozie reports status before the download is persisted
      LOG.error(String.format("Download [%s] not found", downloadId));
      return;
    }

    Download.Status newStatus = STATUSES_MAP.get(opStatus.get());
    switch (newStatus) {
      case KILLED:
        // Keep a manually cancelled download status as opposed to a killed one
        if (download.getStatus() == Download.Status.CANCELLED) {
          CANCELLED_DOWNLOADS.inc();
          return;
        }
      case FAILED:
        LOG.error(NOTIFY_ADMIN, "Got callback for failed query. JobId [{}], Status [{}]", jobId, status);
        downloadEmailUtils.sendErrorNotificationMail(download);
        FAILED_DOWNLOADS.inc();
        break;

      case SUCCEEDED:
        SUCCESSFUL_DOWNLOADS.inc();
        // notify about download
        if (download.getRequest().getSendNotification()) {
          downloadEmailUtils.sendSuccessNotificationMail(download);
        }
        break;

      default:
        break;
    }

    updateDownloadStatus(download, newStatus);
  }

  /**
   * Returns the download size in bytes.
   */
  private Long getDownloadSize(String downloadKey) {
    File downloadFile = new File(downloadMount, downloadKey + ".zip");
    if (downloadFile.exists()) {
      return downloadFile.length();
    }
    return 0L;
  }

  /**
   * Persists the download information.
   */
  private void persistDownload(DownloadRequest request, String downloadId) {
    Download download = new Download();
    download.setKey(downloadId);
    download.setRequest(request);
    download.setStatus(Download.Status.PREPARING);
    download.setDownloadLink(downloadLink(wsUrl, downloadId));
    occurrenceDownloadService.create(download);
  }


  /**
   * Updates the download status and file size.
   */
  private void updateDownloadStatus(Download download, Download.Status newStatus) {
    download.setStatus(newStatus);
    download.setSize(getDownloadSize(download.getKey()));
    occurrenceDownloadService.update(download);
  }
}
