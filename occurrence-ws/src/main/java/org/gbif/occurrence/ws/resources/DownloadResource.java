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
package org.gbif.occurrence.ws.resources;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.api.util.VocabularyUtils;
import org.gbif.occurrence.download.service.CallbackService;
import org.gbif.occurrence.download.service.PredicateFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.annotation.Secured;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.File;
import java.net.URI;
import java.security.Principal;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.gbif.api.model.occurrence.Download.Status.PREPARING;
import static org.gbif.api.model.occurrence.Download.Status.RUNNING;
import static org.gbif.api.model.occurrence.Download.Status.SUCCEEDED;
import static org.gbif.occurrence.download.service.DownloadSecurityUtil.assertLoginMatches;
import static org.gbif.occurrence.download.service.DownloadSecurityUtil.assertMonthlyDownloadBypass;
import static org.gbif.occurrence.download.service.DownloadSecurityUtil.assertUserAuthenticated;

@RestController
@Validated
@RequestMapping(produces = {MediaType.APPLICATION_JSON_VALUE,
  "application/x-javascript"}, value = "occurrence/download/request")
public class DownloadResource {

  private static final String USER_ROLE = "USER";
  private static final String ZIP_EXT = ".zip";
  private static final String AVRO_EXT = ".avro";

  // low quality of source to default to JSON
  private static final String OCT_STREAM_QS = ";qs=0.5";

  private static final String APPLICATION_OCTET_STREAM_QS_VALUE =
    MediaType.APPLICATION_OCTET_STREAM_VALUE + OCT_STREAM_QS;

  private static final Logger LOG = LoggerFactory.getLogger(DownloadResource.class);

  private static final Splitter COMMA_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

  private final DownloadRequestService requestService;

  private final OccurrenceDownloadService occurrenceDownloadService;

  private final CallbackService callbackService;

  private final String archiveServerUrl;

  @Autowired
  public DownloadResource(
    @Value("${occurrence.download.archive_server.url}") String archiveServerUrl,
    DownloadRequestService service,
    CallbackService callbackService,
    OccurrenceDownloadService occurrenceDownloadService
  ) {
    this.archiveServerUrl = archiveServerUrl;
    this.requestService = service;
    this.callbackService = callbackService;
    this.occurrenceDownloadService = occurrenceDownloadService;
  }

  @DeleteMapping("{key}")
  public void delDownload(@PathVariable("key") String jobId, @Autowired Principal principal) {
    // service.get returns a download or throws NotFoundException
    Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
    assertLoginMatches(occurrenceDownloadService.get(jobId).getRequest(), authentication, principal);
    LOG.info("Delete download: [{}]", jobId);
    requestService.cancel(jobId);
  }

  /**
   * Single file download.
   * <p>
   * This redirects to the download server, so redeploys of this webservice don't affect long-running
   * downloads, and to take advantage of Apache's full implementation of HTTP (Range requests etc).
   * <p>
   * (The commit introducing this comment removed an implementation of Range requests.)
   */
  @GetMapping(value = "{key}", produces = {APPLICATION_OCTET_STREAM_QS_VALUE, MediaType.APPLICATION_JSON_VALUE,
    "application/x-javascript"})
  public ResponseEntity<String> getResult(@PathVariable("key") String downloadKey) {

    // if key contains avro or zip suffix remove it as we intend to work with the pure key
    downloadKey = StringUtils.removeEndIgnoreCase(downloadKey, AVRO_EXT);
    downloadKey = StringUtils.removeEndIgnoreCase(downloadKey, ZIP_EXT);

    String extension = Optional.ofNullable(occurrenceDownloadService.get(downloadKey))
      .map(download -> download.getRequest().getFormat().getExtension())
      .orElse(ZIP_EXT);

    LOG.debug("Get download data: [{}]", downloadKey);
    File download = requestService.getResultFile(downloadKey);

    String location = archiveServerUrl + downloadKey + extension;
    return ResponseEntity.status(HttpStatus.FOUND)
      .header(HttpHeaders.LAST_MODIFIED, new SimpleDateFormat().format(new Date(download.lastModified())))
      .location(URI.create(location))
      .body(location + "\n");
  }

  @GetMapping("callback")
  public ResponseEntity oozieCallback(@RequestParam("job_id") String jobId, @RequestParam("status") String status) {
    LOG.debug("Received callback from Oozie for Job [{}] with status [{}]", jobId, status);
    callbackService.processCallback(jobId, status);
    return ResponseEntity.ok().build();
  }

  /**
   * Request a new predicate download (POST method, public API).
   */
  @PostMapping(produces = {MediaType.TEXT_PLAIN_VALUE, MediaType.APPLICATION_JSON_VALUE}, consumes = {
    MediaType.APPLICATION_JSON_VALUE})
  @ResponseBody
  @Secured(USER_ROLE)
  public ResponseEntity<String> startDownload(
    @NotNull @Valid @RequestBody PredicateDownloadRequest request, @Autowired Principal principal
  ) {
    try {
      Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
      return ResponseEntity.ok(createDownload(request, authentication, principal));
    } catch (ResponseStatusException rse) {
      return ResponseEntity.status(rse.getStatus()).body(rse.getReason());
    }
  }

  /**
   * Creates/Starts an occurrence download.
   *
   * Non-admin users may be given an existing download key, where the monthly download user
   * has already created a suitable download.
   */
  private String createDownload(DownloadRequest downloadRequest, Authentication authentication, @Autowired Principal principal) {
    Principal userAuthenticated = assertUserAuthenticated(principal);
    if (Objects.isNull(downloadRequest.getCreator())) {
      downloadRequest.setCreator(userAuthenticated.getName());
    }
    LOG.info("New download request: [{}]", downloadRequest);
    // User matches (or admin user)
    assertLoginMatches(downloadRequest, authentication, userAuthenticated);

    if (!assertMonthlyDownloadBypass(authentication) &&
      downloadRequest instanceof PredicateDownloadRequest) {
      PredicateDownloadRequest predicateDownloadRequest = (PredicateDownloadRequest) downloadRequest;
      if (!predicateDownloadRequest.getFormat().equals(DownloadFormat.SPECIES_LIST)) {

        // Check for recent monthly downloads with the same predicate
        PagingResponse<Download> monthlyDownloads = occurrenceDownloadService.listByUser("download.gbif.org",
          new PagingRequest(0, 50), EnumSet.of(PREPARING, RUNNING, SUCCEEDED));
        String existingMonthlyDownload = matchExistingDownload(monthlyDownloads, predicateDownloadRequest,
          Date.from(Instant.now().minus(35, ChronoUnit.DAYS)));
        if (existingMonthlyDownload != null) {
          return existingMonthlyDownload;
        }
      }

      // Check for recent user downloads (of this same user) with the same predicate
      PagingResponse<Download> userDownloads = occurrenceDownloadService.listByUser(userAuthenticated.getName(),
        new PagingRequest(0, 50), EnumSet.of(PREPARING, RUNNING, SUCCEEDED));
      String existingUserDownload = matchExistingDownload(userDownloads, predicateDownloadRequest,
        Date.from(Instant.now().minus(4, ChronoUnit.HOURS)));
      if (existingUserDownload != null) {
        return existingUserDownload;
      }
    }

    String downloadKey = requestService.create(downloadRequest);
    LOG.info("Created new download job with key [{}]", downloadKey);
    return downloadKey;
  }

  /**
   * Request a new download (GET method, internal API used by the portal).
   */
  @GetMapping(produces = {MediaType.TEXT_PLAIN_VALUE, MediaType.APPLICATION_JSON_VALUE})
  @Secured(USER_ROLE)
  @ResponseBody
  public String download(
    @Autowired HttpServletRequest httpRequest,
    @RequestParam(name = "notification_address", required = false) String emails,
    @RequestParam("format") String format,
    @Autowired Principal principal
  ) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(format), "Format can't be null");
    Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
    return createDownload(downloadPredicate(httpRequest, emails, format, principal), authentication, principal);
  }

  @GetMapping("predicate")
  public DownloadRequest downloadPredicate(
    @Autowired HttpServletRequest httpRequest,
    @RequestParam(name = "notification_address", required = false) String emails,
    @RequestParam("format") String format,
    @Autowired Principal principal
  ) {
    DownloadFormat downloadFormat = VocabularyUtils.lookupEnum(format, DownloadFormat.class);
    Preconditions.checkArgument(Objects.nonNull(downloadFormat), "Format param is not present");
    String creator = principal != null ? principal.getName() : null;
    Set<String> notificationAddress = asSet(emails);
    Predicate predicate = PredicateFactory.build(httpRequest.getParameterMap());
    LOG.info("Predicate build for passing to download [{}]", predicate);
    return new PredicateDownloadRequest(predicate, creator, notificationAddress, true, downloadFormat);
  }

  /**
   * Search existingDownloads for a download with a format and predicate matching newDownload,
   * from cutoff at the earliest.
   *
   * @return The download key, if there's a match.
   */
  private String matchExistingDownload(
    PagingResponse<Download> existingDownloads,
    PredicateDownloadRequest newDownload,
    Date cutoff) {
    for (Download existingDownload : existingDownloads.getResults()) {
      // Downloads are in descending order by creation date
      if (existingDownload.getCreated().before(cutoff)) {
        return null;
      }

      if (existingDownload.getRequest() instanceof PredicateDownloadRequest) {
        PredicateDownloadRequest existingPredicateDownload = (PredicateDownloadRequest) existingDownload.getRequest();
        if (newDownload.getFormat() == existingPredicateDownload.getFormat() &&
          Objects.equals(newDownload.getPredicate(), existingPredicateDownload.getPredicate())) {
          LOG.info("Found existing {} download {} ({}) matching new download request.",
            existingPredicateDownload.getFormat(), existingDownload.getKey(),
            existingPredicateDownload.getCreator());
          return existingDownload.getKey();
        }
      } else {
        LOG.warn("Unexpected download type {}", existingDownload.getClass());
      }
    }

    return null;
  }

  /**
   * Transforms a String that contains elements split by ',' into a Set of strings.
   */
  private static Set<String> asSet(String cvsString) {
    return Objects.nonNull(cvsString) ? Sets.newHashSet(COMMA_SPLITTER.split(cvsString)) : null;
  }
}
