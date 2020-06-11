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

import static org.gbif.occurrence.download.service.DownloadSecurityUtil.assertLoginMatches;
import static org.gbif.occurrence.download.service.DownloadSecurityUtil.assertUserAuthenticated;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.security.Principal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.apache.bval.guice.Validate;
import org.apache.commons.lang3.StringUtils;

import org.gbif.api.model.common.GbifUserPrincipal;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.api.util.VocabularyUtils;
import org.gbif.api.vocabulary.UserRole;
import org.gbif.occurrence.download.service.CallbackService;
import org.gbif.occurrence.download.service.PredicateFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.annotation.Secured;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

@RestController
@RequestMapping(
  produces = {MediaType.APPLICATION_JSON_VALUE, "application/x-javascript"},
  value = "occurrence/download/request"
)
public class DownloadResource {

  private static final String USER_ROLE = "USER";
  private static final String ZIP_EXT = ".zip";
  private static final String AVRO_EXT = ".avro";

  private static final Logger LOG = LoggerFactory.getLogger(DownloadResource.class);

  // low quality of source to default to JSON
  private static final String OCT_STREAM_QS = ";qs=0.5";

  private static final Splitter COMMA_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

  private final DownloadRequestService requestService;

  private final OccurrenceDownloadService occurrenceDownloadService;

  private final CallbackService callbackService;

  @Autowired
  public DownloadResource(DownloadRequestService service, CallbackService callbackService,
                          OccurrenceDownloadService occurrenceDownloadService) {
    requestService = service;
    this.callbackService = callbackService;
    this.occurrenceDownloadService = occurrenceDownloadService;
  }

  @DeleteMapping("{key}")
  public void delDownload(@PathVariable("key") String jobId, @Autowired Principal principal) {
    // service.get returns a download or throws NotFoundException
    assertLoginMatches(occurrenceDownloadService.get(jobId).getRequest(), principal);
    LOG.debug("Delete download: [{}]", jobId);
    requestService.cancel(jobId);
  }

  @GetMapping(
    value = "{key}",
    produces = MediaType.APPLICATION_OCTET_STREAM_VALUE + OCT_STREAM_QS
  )
  public ResponseEntity getResult(@RequestHeader("Range") String range,
                                                         @PathVariable("key") String downloadKey,
                                                         @Autowired HttpServletRequest request,
                                                         @Autowired HttpServletResponse response
                                                         ) {
    // if key contains avro or zip suffix remove it as we intend to work with the pure key
    downloadKey = StringUtils.removeEndIgnoreCase(downloadKey, AVRO_EXT);
    downloadKey = StringUtils.removeEndIgnoreCase(downloadKey, ZIP_EXT);

    String extension = Optional.ofNullable(occurrenceDownloadService.get(downloadKey))
      .map(download -> download.getRequest().getFormat().getExtension())
      .orElse(ZIP_EXT);

    LOG.debug("Get download data: [{}]", downloadKey);
    File download = requestService.getResultFile(downloadKey);

    try {
      if (range == null) {
        response.setContentType(MediaType.APPLICATION_OCTET_STREAM_VALUE + OCT_STREAM_QS);
        return ResponseEntity
          .status(HttpStatus.OK)
          // Show that we support Range requests (i.e. can resume downloads)
          .header("Accept-Ranges", "bytes")
          // Suggest filename for download in HTTP headers
          .header("Content-Disposition", "attachment; filename=" + downloadKey + extension)
          // Allow client to show a progress bar
          .header(HttpHeaders.CONTENT_LENGTH, Long.toString(download.length()))
          .header(HttpHeaders.LAST_MODIFIED, new SimpleDateFormat().format(new Date(download.lastModified())))
          //.contentType(MediaType.APPLICATION_OCTET_STREAM_VALUE + OCT_STREAM_QS)
          .body(new FileInputStream(download));
      } else {
        //LOG.debug("Ranged request, Range: {}", range);
        if (LOG.isDebugEnabled()) {
          // Temporarily, in case we find weird clients.
          LOG.debug("Range {} request, dumping all headers:", request.getMethod());
          Enumeration<String> h = request.getHeaderNames();
          while (h.hasMoreElements()) {
            String s = h.nextElement();
            LOG.debug("Header {}: {}", s, request.getHeader(s));
          }
        }

        // Determine requested range.
        final long from;
        final long to;
        try {
          // Multipart ranges are not supported: Range: bytes=0-50, 100-150,
          // but will fall through to the end of the range.

          String[] ranges = range.split("=")[1].split("-");
          // Single range: Range: bytes=1000-2000
          // Or open ranges: Range: bytes=1000-
          if (ranges[0].isEmpty()) {
            // End range: Range: bytes=-5000
            to = download.length() - 1;
            from = download.length() - Long.parseLong(ranges[1]);
          } else {
            // Normal or open range: bytes=1000-2000 or bytes=1000-
            from = Long.parseLong(ranges[0]);
            if (ranges.length == 2) {
              to = Long.parseLong(ranges[1]);
            } else {
              to = download.length() - 1;
            }
          }
        } catch (Exception e) {
          // Error log, as I assume clients shouldn't often make bad requests.
          LOG.error("Unable to parse range request for {}: {}", downloadKey, range);
          return ResponseEntity
            .status(HttpStatus.BAD_REQUEST)
            .build();
        }

        // Determine if the requested range exists
        if (to < 0 || from >= download.length()) {
          // Error log, since it seems strange that clients would make these requests.
          LOG.error("Unable to satisfy range request for {}: {}", downloadKey, range);
          return ResponseEntity.status(HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
            .header("Content-Range", String.format("bytes */%d", download.length()))
            .build();
        }

        final RandomAccessFile downloadRAF = new RandomAccessFile(download, "r");
        downloadRAF.seek(from);

        final long length = to - from + 1;
        final DownloadStreamer streamer = new DownloadStreamer(downloadRAF, length);
        final String responseRange = String.format("bytes %d-%d/%d", from, to, download.length());
        // Ensure this has the same headers (except Content-Range and Content-Length) as the full response above.
        response.setContentType(MediaType.APPLICATION_OCTET_STREAM_VALUE + OCT_STREAM_QS);
        return ResponseEntity
          .status(HttpStatus.PARTIAL_CONTENT)
          //.contentType(MediaType.APPLICATION_OCTET_STREAM_VALUE + OCT_STREAM_QS)
          .header("Accept-Ranges", "bytes")
          .header("Content-Range", responseRange)
          .header("Content-Disposition", "attachment; filename=" + downloadKey + extension)
          .header(HttpHeaders.CONTENT_LENGTH, Long.toString(length))
          .header(HttpHeaders.LAST_MODIFIED, new SimpleDateFormat().format(new Date(download.lastModified())))
          .body(streamer);
      }
    } catch (IOException e) {
      throw new IllegalStateException("Failed to read download " + downloadKey + " from " + download.getAbsolutePath(), e);
    }
  }

  /**
   * Streams some amount of a a pre-sought (hmm…) file.
   */
  class DownloadStreamer implements StreamingResponseBody {
    private RandomAccessFile download;
    private long length;
    final byte[] b = new byte[4096];

    public DownloadStreamer(RandomAccessFile download, long length) {
      this.download = download;
      this.length = length;
    }

    @Override
    public void writeTo(OutputStream outputStream) throws IOException {
      try {
        while (length > 0) {
          int read = download.read(b, 0, (int) (b.length > length ? length : b.length));
          outputStream.write(b, 0, read);
          length -= read;
        }
      } finally {
        download.close();
      }
    }
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
  @PostMapping(
    produces = {MediaType.TEXT_PLAIN_VALUE, MediaType.APPLICATION_JSON_VALUE},
    consumes = {MediaType.APPLICATION_JSON_VALUE}
  )
  @ResponseBody
  @Validate
  @Secured(USER_ROLE)
  public String startDownload(@NotNull @Valid @RequestBody PredicateDownloadRequest request, @Autowired Principal principal) {
    return createDownload(request, principal);
  }

  /**
   * Creates/Starts an occurrence download.
   */
  private String createDownload(DownloadRequest downloadRequest, @Autowired Principal principal) {
    Principal userAuthenticated = assertUserAuthenticated(principal);
    if (Objects.isNull(downloadRequest.getCreator())) {
      downloadRequest.setCreator(userAuthenticated.getName());
    }
    LOG.debug("Download: [{}]", downloadRequest);
    // assert authenticated user is the same as in download
    assertLoginMatches(downloadRequest, userAuthenticated);
    String downloadKey = requestService.create(downloadRequest);
    LOG.info("Created new download job with key [{}]", downloadKey);
    return downloadKey;
  }

  /**
   * Request a new download (GET method, internal API used by the portal).
   */
  @GetMapping(
    produces = {MediaType.TEXT_PLAIN_VALUE, MediaType.APPLICATION_JSON_VALUE}
  )
  @Secured(USER_ROLE)
  @ResponseBody
  public String download(@Autowired HttpServletRequest httpRequest,
                         @RequestParam(name = "notification_address", required = false) String emails,
                         @RequestParam("format") String format,
                         @Autowired Principal principal) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(format), "Format can't be null");
    return createDownload(downloadPredicate(httpRequest, emails, format, principal), principal);
  }

  @GetMapping("predicate")
  public DownloadRequest downloadPredicate(@Autowired HttpServletRequest httpRequest,
                                           @RequestParam(name = "notification_address", required = false) String emails,
                                           @RequestParam("format") String format,
                                           @Autowired Principal principal) {
    DownloadFormat downloadFormat = VocabularyUtils.lookupEnum(format,DownloadFormat.class);
    Preconditions.checkArgument(Objects.nonNull(downloadFormat), "Format param is not present");
    String creator = principal.getName();
    Set<String> notificationAddress = asSet(emails);
    Predicate predicate = PredicateFactory.build(httpRequest.getParameterMap());
    LOG.info("Predicate build for passing to download [{}]", predicate);
    return new PredicateDownloadRequest(predicate, creator, notificationAddress, true, downloadFormat);
  }

  /**
   * Transforms a String that contains elements split by ',' into a Set of strings.
   */
  private static Set<String> asSet(String cvsString) {
    return Objects.nonNull(cvsString) ? Sets.newHashSet(COMMA_SPLITTER.split(cvsString)) : null;
  }
}
