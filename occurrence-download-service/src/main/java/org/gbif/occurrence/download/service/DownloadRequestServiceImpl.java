/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.download.service;

import static org.gbif.occurrence.common.download.DownloadUtils.downloadLink;
import static org.gbif.occurrence.download.service.Constants.NOTIFY_ADMIN;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Enums;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.EnumSet;
import java.util.Set;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.api.messages.DownloadCancelMessage;
import org.gbif.common.messaging.api.messages.DownloadLauncherMessage;
import org.gbif.occurrence.mail.BaseEmailModel;
import org.gbif.occurrence.mail.EmailSender;
import org.gbif.occurrence.mail.OccurrenceEmailManager;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

@Component
@Slf4j
public class DownloadRequestServiceImpl implements DownloadRequestService, CallbackService {

  private static final Logger LOG = LoggerFactory.getLogger(DownloadRequestServiceImpl.class);
  // magic prefix for download keys to indicate these aren't real download files
  private static final String NON_DOWNLOAD_PREFIX = "dwca-";

  protected static final Set<Download.Status> RUNNING_STATUSES =
      EnumSet.of(Download.Status.PREPARING, Download.Status.RUNNING, Download.Status.SUSPENDED);

  /** Map to provide conversions from oozie.Job.Status to Download.Status. */
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
          .put(Job.Status.SUSPENDEDWITHERROR, Download.Status.SUSPENDED)
          .put(Job.Status.IGNORED, Download.Status.FAILED)
          .build();

  private static final Counter SUCCESSFUL_DOWNLOADS =
      Metrics.newCounter(CallbackService.class, "successful_downloads");
  private static final Counter FAILED_DOWNLOADS =
      Metrics.newCounter(CallbackService.class, "failed_downloads");
  private static final Counter CANCELLED_DOWNLOADS =
      Metrics.newCounter(CallbackService.class, "cancelled_downloads");

  private final OozieClient client;
  private final DownloadIdService downloadIdService;
  private final String portalUrl;
  private final String wsUrl;
  private final File downloadMount;
  private final OccurrenceDownloadService occurrenceDownloadService;
  private final OccurrenceEmailManager emailManager;
  private final EmailSender emailSender;

  private final DownloadLimitsService downloadLimitsService;
  private final MessagePublisher messagePublisher;

  @Autowired
  public DownloadRequestServiceImpl(
      OozieClient client,
      @Value("${occurrence.download.portal.url}") String portalUrl,
      @Value("${occurrence.download.ws.url}") String wsUrl,
      @Value("${occurrence.download.ws.mount}") String wsMountDir,
      OccurrenceDownloadService occurrenceDownloadService,
      DownloadLimitsService downloadLimitsService,
      OccurrenceEmailManager emailManager,
      EmailSender emailSender,
      MessagePublisher messagePublisher) {
    this.downloadIdService = new DownloadIdService();
    this.client = client;
    this.portalUrl = portalUrl;
    this.wsUrl = wsUrl;
    this.downloadMount = new File(wsMountDir);
    this.occurrenceDownloadService = occurrenceDownloadService;
    this.downloadLimitsService = downloadLimitsService;
    this.emailManager = emailManager;
    this.emailSender = emailSender;
    this.messagePublisher = messagePublisher;
  }

  @Override
  public void cancel(String downloadKey) {
    try {
      Download download = occurrenceDownloadService.get(downloadKey);
      if (download != null) {
        if (RUNNING_STATUSES.contains(download.getStatus())) {
          updateDownloadStatus(download, Download.Status.CANCELLED);

          messagePublisher.send(new DownloadCancelMessage(downloadKey));

          log.info("Download {} cancelled", downloadKey);
        }
      } else {
        throw new ResponseStatusException(
            HttpStatus.NOT_FOUND, String.format("Download %s not found", downloadKey));
      }
    } catch (Exception e) {
      throw new ServiceUnavailableException("Failed to cancel download " + downloadKey, e);
    }
  }

  @Override
  public String create(DownloadRequest request, String source) {
    LOG.debug("Trying to create download from request [{}]", request);
    Preconditions.checkNotNull(request);
    if (request instanceof PredicateDownloadRequest) {
      PredicateValidator.validate(((PredicateDownloadRequest) request).getPredicate());
    }

    String exceedComplexityLimit = null;
    try {
      exceedComplexityLimit = downloadLimitsService.exceedsDownloadComplexity(request);
    } catch (Exception e) {
      throw new ServiceUnavailableException(
          "Failed to create download job while checking download complexity", e);
    }
    if (exceedComplexityLimit != null) {
      LOG.info("Download request refused as it would exceed complexity limits");
      throw new ResponseStatusException(
          HttpStatus.PAYLOAD_TOO_LARGE,
          "A download limitation is exceeded:\n" + exceedComplexityLimit + "\n");
    }

    String exceedSimultaneousLimit = null;
    try {
      exceedSimultaneousLimit =
          downloadLimitsService.exceedsSimultaneousDownloadLimit(request.getCreator());
    } catch (Exception e) {
      throw new ServiceUnavailableException(
          "Failed to create download job while checking simultaneous download limit", e);
    }
    if (exceedSimultaneousLimit != null) {
      LOG.info("Download request refused as it would exceed simultaneous limits");
      // Keep HTTP 420 ("Enhance your calm") here.
      throw new ResponseStatusException(
          HttpStatus.METHOD_FAILURE,
          "A download limitation is exceeded:\n" + exceedSimultaneousLimit + "\n");
    }

    try {
      String downloadId = downloadIdService.generateId();
      log.debug("Download id is: [{}]", downloadId);
      persistDownload(request, downloadId, source);

      log.debug("Send message to the download launcher queue");
      messagePublisher.send(new DownloadLauncherMessage(downloadId, request));

      return downloadId;
    } catch (Exception e) {
      throw new ServiceUnavailableException("Failed to create download job", e);
    }
  }

  @Nullable
  @Override
  public File getResultFile(String downloadKey) {
    String filename;

    // avoid check for download in the registry if we have secret non download files with a magic
    // prefix!
    if (downloadKey == null || !downloadKey.toLowerCase().startsWith(NON_DOWNLOAD_PREFIX)) {
      Download d = occurrenceDownloadService.get(downloadKey);

      if (d == null) {
        throw new ResponseStatusException(
            HttpStatus.NOT_FOUND, "Download " + downloadKey + " doesn't exist");
      }

      checkDownloadPreConditions(downloadKey, d);

      filename = getDownloadFilename(d);
    } else {
      filename = downloadKey + ".zip";
    }

    return getDownloadFile(filename, downloadKey);
  }

  @Override
  public File getResultFile(Download download) {
    String filename;

    if (download == null || download.getKey() == null) {
      throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Download can't be null");
    }

    String downloadKey = download.getKey();

    // avoid check for download in the registry if we have secret non download files with a magic
    // prefix!
    if (!downloadKey.toLowerCase().startsWith(NON_DOWNLOAD_PREFIX)) {
      checkDownloadPreConditions(downloadKey, download);

      filename = getDownloadFilename(download);
    } else {
      filename = downloadKey + ".zip";
    }

    return getDownloadFile(filename, downloadKey);
  }

  private static void checkDownloadPreConditions(String downloadKey, Download d) {
    if (d.getStatus() == Download.Status.FILE_ERASED) {
      throw new ResponseStatusException(
          HttpStatus.GONE, "Download " + downloadKey + " has been erased\n");
    }

    if (!d.isAvailable()) {
      throw new ResponseStatusException(
          HttpStatus.NOT_FOUND, "Download " + downloadKey + " is not ready yet");
    }
  }

  @NotNull
  private File getDownloadFile(String filename, String downloadKey) {
    File localFile = new File(downloadMount, filename);
    if (localFile.canRead()) {
      return localFile;
    } else {
      throw new IllegalStateException(
          "Unable to read download " + downloadKey + " from " + localFile.getAbsolutePath());
    }
  }

  @Nullable
  @Override
  public InputStream getResult(String downloadKey) {
    File localFile = getResultFile(downloadKey);
    try {
      return Files.newInputStream(localFile.toPath());
    } catch (IOException e) {
      throw new IllegalStateException(
          "Failed to read download " + downloadKey + " from " + localFile.getAbsolutePath(), e);
    }
  }

  /** Processes a callback from Oozie which update the download status. */
  @Override
  public void processCallback(String jobId, String status) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(jobId), "<jobId> may not be null or empty");
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(status), "<status> may not be null or empty");
    Optional<Job.Status> opStatus = Enums.getIfPresent(Job.Status.class, status.toUpperCase());
    Preconditions.checkArgument(opStatus.isPresent(), "<status> the requested status is not valid");

    String downloadId;
    try {
      downloadId = client.getJobInfo(jobId).getExternalId();
    } catch (OozieClientException e) {
      throw new RuntimeException(e);
    }

    LOG.debug("Processing callback for downloadId [{}] with status [{}]", downloadId, status);

    Download download = occurrenceDownloadService.get(downloadId);
    if (download == null) {
      // Download can be null if the oozie reports status before the download is persisted
      LOG.info(
          "Download {} not found. [Oozie may be issuing callback before download persisted.]",
          downloadId);
      return;
    }

    if (Download.Status.SUCCEEDED.equals(download.getStatus())
        || Download.Status.FAILED.equals(download.getStatus())
        || Download.Status.KILLED.equals(download.getStatus())) {
      // Download has already completed, so perhaps callbacks in rapid succession have been
      // processed out-of-order
      LOG.warn(
          "Download {} has finished, but Oozie has sent a RUNNING callback. Ignoring it.",
          downloadId);
      return;
    }

    BaseEmailModel emailModel;
    Download.Status newStatus = STATUSES_MAP.get(opStatus.get());
    switch (newStatus) {
      case KILLED:
        // Keep a manually cancelled download status as opposed to a killed one
        if (download.getStatus() == Download.Status.CANCELLED) {
          CANCELLED_DOWNLOADS.inc();
          return;
        }

      case FAILED:
        LOG.error(
            NOTIFY_ADMIN,
            "Got callback for failed query. downloadId [{}], Status [{}]",
            downloadId,
            status);
        updateDownloadStatus(download, newStatus);
        emailModel = emailManager.generateFailedDownloadEmailModel(download, portalUrl);
        emailSender.send(emailModel);
        FAILED_DOWNLOADS.inc();
        break;

      case SUCCEEDED:
        SUCCESSFUL_DOWNLOADS.inc();
        updateDownloadStatus(download, newStatus, getDownloadSize(download));
        // notify about download
        if (download.getRequest().getSendNotification()) {
          emailModel = emailManager.generateSuccessfulDownloadEmailModel(download, portalUrl);
          emailSender.send(emailModel);
        }
        break;

      default:
        updateDownloadStatus(download, newStatus);
        break;
    }
  }

  /**
   * Tries to get the size of a download file. Logs as warning if the size is 0 or if the file can't
   * be read.
   */
  private Long getDownloadSize(Download download) {
    File file = new File(downloadMount, getDownloadFilename(download));
    if (file.canRead()) {
      long size = file.length();
      if (size == 0) {
        LOG.warn(
            "Download file {} size not read accurately, 0 length returned", file.getAbsolutePath());
      }
      return size;
    } else {
      LOG.warn("Can't read download file {}", file.getAbsolutePath());
    }
    return 0L;
  }

  /** Persists the download information. */
  private void persistDownload(DownloadRequest request, String downloadId, String source) {
    Download download = new Download();
    download.setKey(downloadId);
    download.setStatus(Download.Status.PREPARING);
    download.setEraseAfter(Date.from(OffsetDateTime.now(ZoneOffset.UTC).plusMonths(6).toInstant()));
    download.setDownloadLink(
        downloadLink(wsUrl, downloadId, request.getType(), request.getFormat().getExtension()));
    download.setRequest(request);
    download.setSource(source);
    occurrenceDownloadService.create(download);
  }

  /** Updates the download status and file size. */
  private void updateDownloadStatus(Download download, Download.Status newStatus) {
    if (download.getStatus() != newStatus) {
      download.setStatus(newStatus);
      occurrenceDownloadService.update(download);
    }
  }

  private void updateDownloadStatus(Download download, Download.Status newStatus, long size) {
    download.setStatus(newStatus);
    download.setSize(size);
    occurrenceDownloadService.update(download);
  }

  /** The download filename with extension. */
  private String getDownloadFilename(Download download) {
    return download.getKey() + download.getRequest().getFormat().getExtension();
  }
}
