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
package org.gbif.occurrence.download.resource;

import io.swagger.v3.oas.annotations.Hidden;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.occurrence.*;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.api.util.VocabularyUtils;
import org.gbif.api.vocabulary.Extension;
import org.gbif.occurrence.download.service.CallbackService;
import org.gbif.occurrence.download.service.PredicateFactory;

import java.io.File;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.URI;
import java.security.Principal;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.StringUtils;
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
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;

import static java.lang.annotation.ElementType.*;
import static org.gbif.api.model.occurrence.Download.Status.PREPARING;
import static org.gbif.api.model.occurrence.Download.Status.RUNNING;
import static org.gbif.api.model.occurrence.Download.Status.SUCCEEDED;
import static org.gbif.occurrence.download.service.DownloadSecurityUtil.assertLoginMatches;
import static org.gbif.occurrence.download.service.DownloadSecurityUtil.assertMonthlyDownloadBypass;
import static org.gbif.occurrence.download.service.DownloadSecurityUtil.assertUserAuthenticated;

@Validated
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

  private final DownloadType downloadType;

  private final Boolean downloadsDisabled;

  @Autowired
  public DownloadResource(
      @Value("${occurrence.download.archive_server.url}") String archiveServerUrl,
      DownloadRequestService service,
      CallbackService callbackService,
      OccurrenceDownloadService occurrenceDownloadService,
      DownloadType downloadType,
      @Value("${occurrence.download.disabled:false}") Boolean downloadsDisabled) {
    this.archiveServerUrl = archiveServerUrl;
    this.requestService = service;
    this.callbackService = callbackService;
    this.occurrenceDownloadService = occurrenceDownloadService;
    this.downloadType = downloadType;
    this.downloadsDisabled = downloadsDisabled;
  }

  private void assertDownloadType(Download download) {
    if (downloadType != download.getRequest().getType()) {
      throw new ResponseStatusException(
          HttpStatus.BAD_REQUEST, "Wrong download type for this endpoint");
    }
  }

  /**
   * A download key (example is the oldest download).
   */
  @Target({PARAMETER, METHOD, FIELD, ANNOTATION_TYPE})
  @Retention(RetentionPolicy.RUNTIME)
  @Inherited
  @Parameter(
    name = "key",
    required = true,
    description = "An identifier for a download.",
    schema = @Schema(implementation = String.class, format = "NNNNNNN-NNNNNNNNNNNNNNN"),
    example = "0001005-130906152512535",
    in = ParameterIn.PATH)
  @interface DownloadIdentifierPathParameter {}

  @Operation(
    operationId = "cancelDownload",
    summary = "Cancel a running download",
    description = "Cancel a running download",
    responses = @ApiResponse(responseCode = "204", content = @Content(schema = @Schema(hidden = true))))
  @DeleteMapping("{key}")
  public void delDownload(@PathVariable("key") @DownloadIdentifierPathParameter String jobId,
                          @Autowired Principal principal) {
    // service.get returns a download or throws NotFoundException
    Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
    Download download = occurrenceDownloadService.get(jobId);
    assertDownloadType(download);
    assertLoginMatches(download.getRequest(), authentication, principal);
    LOG.info("Delete download: [{}]", jobId);
    requestService.cancel(jobId);
  }

  /**
   * Single file download.
   *
   * <p>This redirects to the download server, so redeploys of this webservice don't affect
   * long-running downloads, and to take advantage of Apache's full implementation of HTTP (Range
   * requests etc).
   *
   * <p>(The commit introducing this comment removed an implementation of Range requests.)
   */
  @Operation(
    operationId = "retrieveDownload",
    summary = "Retrieve the resulting download file",
    description = "Retrieves the download file if it is available.",
    responses = @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(hidden = true))))
  @GetMapping(
      value = "{key}",
      produces = {
        APPLICATION_OCTET_STREAM_QS_VALUE,
        MediaType.APPLICATION_JSON_VALUE,
        "application/x-javascript"
      })
  public ResponseEntity<String> getResult(@PathVariable("key") @DownloadIdentifierPathParameter String downloadKey) {

    // if key contains avro or zip suffix remove it as we intend to work with the pure key
    downloadKey = StringUtils.removeEndIgnoreCase(downloadKey, AVRO_EXT);
    downloadKey = StringUtils.removeEndIgnoreCase(downloadKey, ZIP_EXT);

    Download download = occurrenceDownloadService.get(downloadKey);
    String extension =
        Optional.ofNullable(download)
            .map(d -> d.getRequest().getFormat().getExtension())
            .orElse(ZIP_EXT);

    if (download != null) {
      assertDownloadType(download);
    }

    LOG.debug("Get download data: [{}]", downloadKey);
    File downloadFile = requestService.getResultFile(downloadKey);

    String location = archiveServerUrl + downloadKey + extension;
    return ResponseEntity.status(HttpStatus.FOUND)
        .header(
            HttpHeaders.LAST_MODIFIED,
            new SimpleDateFormat().format(new Date(downloadFile.lastModified())))
        .location(URI.create(location))
        .body(location + "\n");
  }

  @Hidden
  @GetMapping("callback")
  public ResponseEntity oozieCallback(
      @RequestParam("job_id") String jobId, @RequestParam("status") String status) {
    LOG.debug("Received callback from Oozie for Job [{}] with status [{}]", jobId, status);
    callbackService.processCallback(jobId, status);
    return ResponseEntity.ok().build();
  }

  /** Request a new predicate download (POST method, public API). */
  @Operation(
    operationId = "requestDownload",
    summary = "Requests the creation of a download file.",
    description = "Starts the process of creating a download file. See the predicates section to consult the requests accepted by this service and the limits section to refer for information of how this service is limited per user.",
    responses = @ApiResponse(responseCode = "201", content = @Content(schema = @Schema(hidden = true))))
  @Parameters(
    value = {
      @Parameter(
        name = "source",
        hidden = true
      ),
      @Parameter(
        name = "User-Agent",
        in = ParameterIn.HEADER,
        hidden = true
      )
    })
  @PostMapping(
      produces = {MediaType.TEXT_PLAIN_VALUE, MediaType.APPLICATION_JSON_VALUE},
      consumes = {MediaType.APPLICATION_JSON_VALUE})
  @ResponseBody
  @Secured(USER_ROLE)
  public ResponseEntity<String> startDownload(
      @NotNull @Valid @RequestBody PredicateDownloadRequest request,
      @RequestParam(name = "source", required = false) String source,
      @Autowired Principal principal,
      @RequestHeader(value = "User-Agent") String userAgent) {
    if (Boolean.TRUE.equals(downloadsDisabled)) {
      return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
          .body("Service disabled for maintenance reasons");
    }

    try {
      request.setType(downloadType);
      Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
      return ResponseEntity.ok(
          createDownload(
              request,
              authentication,
              principal,
              parseSource(source, userAgent)));
    } catch (ResponseStatusException rse) {
      return ResponseEntity.status(rse.getStatus()).body(rse.getReason());
    }
  }

  /**
   * Creates/Starts an occurrence download.
   *
   * <p>Non-admin users may be given an existing download key, where the monthly download user has
   * already created a suitable download.
   */
  private String createDownload(
      DownloadRequest downloadRequest,
      Authentication authentication,
      @Autowired Principal principal,
      String source) {
    Principal userAuthenticated = assertUserAuthenticated(principal);
    if (Objects.isNull(downloadRequest.getCreator())) {
      downloadRequest.setCreator(userAuthenticated.getName());
    }
    LOG.info("New download request: [{}]", downloadRequest);
    // User matches (or admin user)
    assertLoginMatches(downloadRequest, authentication, userAuthenticated);

    if (!assertMonthlyDownloadBypass(authentication)
        && downloadRequest instanceof PredicateDownloadRequest) {
      PredicateDownloadRequest predicateDownloadRequest =
          (PredicateDownloadRequest) downloadRequest;
      if (!predicateDownloadRequest.getFormat().equals(DownloadFormat.SPECIES_LIST)) {

        // Check for recent monthly downloads with the same predicate
        PagingResponse<Download> monthlyDownloads =
            occurrenceDownloadService.listByUser(
                "download.gbif.org",
                new PagingRequest(0, 50),
                EnumSet.of(PREPARING, RUNNING, SUCCEEDED));
        String existingMonthlyDownload =
            matchExistingDownload(
                monthlyDownloads,
                predicateDownloadRequest,
                Date.from(Instant.now().minus(35, ChronoUnit.DAYS)));
        if (existingMonthlyDownload != null) {
          return existingMonthlyDownload;
        }
      }

      // Check for recent user downloads (of this same user) with the same predicate
      PagingResponse<Download> userDownloads =
          occurrenceDownloadService.listByUser(
              userAuthenticated.getName(),
              new PagingRequest(0, 50),
              EnumSet.of(PREPARING, RUNNING, SUCCEEDED));
      String existingUserDownload =
          matchExistingDownload(
              userDownloads,
              predicateDownloadRequest,
              Date.from(Instant.now().minus(4, ChronoUnit.HOURS)));
      if (existingUserDownload != null) {
        return existingUserDownload;
      }
    }

    String downloadKey = requestService.create(downloadRequest, source);
    LOG.info("Created new download job with key [{}]", downloadKey);
    return downloadKey;
  }

  /** Request a new download (GET method, internal API used by the portal). */
  @Hidden
  @GetMapping(produces = {MediaType.TEXT_PLAIN_VALUE, MediaType.APPLICATION_JSON_VALUE})
  @Secured(USER_ROLE)
  @ResponseBody
  public String download(
      @Autowired HttpServletRequest httpRequest,
      @RequestParam(name = "notification_address", required = false) String emails,
      @RequestParam("format") String format,
      @RequestParam(name = "extensions", required = false) String extensions,
      @RequestParam(name = "source", required = false) String source,
      @Autowired Principal principal,
      @RequestHeader(value = "User-Agent") String userAgent) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(format), "Format can't be null");
    Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
    return createDownload(
        downloadPredicate(httpRequest, emails, format, extensions, principal),
        authentication,
        principal,
        parseSource(source, userAgent));
  }

  @GetMapping("predicate")
  public DownloadRequest downloadPredicate(
      @Autowired HttpServletRequest httpRequest,
      @RequestParam(name = "notification_address", required = false) String emails,
      @RequestParam("format") String format,
      @RequestParam(name = "extensions", required = false) String extensions,
      @Autowired Principal principal) {
    DownloadFormat downloadFormat = VocabularyUtils.lookupEnum(format, DownloadFormat.class);
    Preconditions.checkArgument(Objects.nonNull(downloadFormat), "Format param is not present");
    String creator = principal != null ? principal.getName() : null;
    Set<String> notificationAddress = asSet(emails);
    Set<Extension> requestExtensions =
        Optional.ofNullable(asSet(extensions))
            .map(exts -> exts.stream().map(Extension::fromRowType).collect(Collectors.toSet()))
            .orElse(Collections.emptySet());
    Predicate predicate = PredicateFactory.build(httpRequest.getParameterMap());
    LOG.info("Predicate build for passing to download [{}]", predicate);
    return new PredicateDownloadRequest(
        predicate,
        creator,
        notificationAddress,
        true,
        downloadFormat,
        downloadType,
        requestExtensions);
  }

  /**
   * Search existingDownloads for a download with a format and predicate matching newDownload, from
   * cutoff at the earliest.
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
        PredicateDownloadRequest existingPredicateDownload =
            (PredicateDownloadRequest) existingDownload.getRequest();
        if (newDownload.getFormat() == existingPredicateDownload.getFormat()
            && newDownload.getType() == existingPredicateDownload.getType()
            && Objects.equals(
                newDownload.getPredicate(), existingPredicateDownload.getPredicate())) {
          LOG.info(
              "Found existing {} download {} ({}) matching new download request.",
              existingPredicateDownload.getFormat(),
              existingDownload.getKey(),
              existingPredicateDownload.getCreator());
          return existingDownload.getKey();
        }
      } else {
        LOG.warn("Unexpected download type {}", existingDownload.getClass());
      }
    }

    return null;
  }

  /** Transforms a String that contains elements split by ',' into a Set of strings. */
  private static Set<String> asSet(String cvsString) {
    return Objects.nonNull(cvsString) ? Sets.newHashSet(COMMA_SPLITTER.split(cvsString)) : null;
  }

  private String parseSource(String source, String userAgent) {
    if (source != null && !source.isEmpty()) {
      LOG.info("Source: {}", source);
      return source;
    }

    if (userAgent == null || userAgent.trim().isEmpty()) {
      LOG.info("No user agent received for source");
      return null;
    } else if (userAgent.toLowerCase().contains("rgbif")) {
      LOG.info("Using rgbif as source from user agent {}", userAgent);
      return "rgbif";
    } else if (userAgent.toLowerCase().contains("pygbif")) {
      LOG.info("Using pygbif as source from user agent {}", userAgent);
      return "pygbif";
    } else if (userAgent.toLowerCase().contains("python")) {
      LOG.info("Using python as source from user agent {}", userAgent);
      return "python";
    } else {
      LOG.info("Using user agent as source: {}", userAgent);
      return userAgent;
    }
  }
}
