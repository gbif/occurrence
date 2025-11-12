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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.gbif.api.exception.QueryBuildingException;
import org.gbif.api.exception.ServiceUnavailableException;
import org.gbif.api.model.common.paging.PagingRequest;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.occurrence.*;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.api.util.VocabularyUtils;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.occurrence.download.hive.DownloadTerms;
import org.gbif.occurrence.download.hive.HiveColumns;
import org.gbif.occurrence.download.query.QueryVisitorsFactory;
import org.gbif.occurrence.download.service.CallbackService;
import org.gbif.occurrence.download.service.PredicateFactory;
import org.gbif.occurrence.download.util.SqlValidation;
import org.gbif.occurrence.query.sql.HiveSqlQuery;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.URI;
import java.security.Principal;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

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

import io.swagger.v3.oas.annotations.Hidden;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.extensions.Extension;
import io.swagger.v3.oas.annotations.extensions.ExtensionProperty;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;

import static java.lang.annotation.ElementType.*;
import static org.gbif.api.model.occurrence.Download.Status.*;
import static org.gbif.api.vocabulary.UserRole.REGISTRY_ADMIN;
import static org.gbif.occurrence.download.service.DownloadSecurityUtil.*;

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

  private final SqlValidation sqlValidation = new SqlValidation();

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

  /** A download key (example is the oldest download). */
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
      responses =
          @ApiResponse(responseCode = "204", content = @Content(schema = @Schema(hidden = true))),
      extensions =
          @Extension(
              name = "Order",
              properties = @ExtensionProperty(name = "Order", value = "0030")))
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "204", description = "Download cancelled."),
        @ApiResponse(responseCode = "404", description = "Invalid download key.")
      })
  @DeleteMapping("{key}")
  public void delDownload(
      @PathVariable("key") @DownloadIdentifierPathParameter String jobId,
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
      responses =
          @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(hidden = true))),
      extensions =
          @Extension(
              name = "Order",
              properties = @ExtensionProperty(name = "Order", value = "0020")))
  @ApiResponses(
      value = {
        @ApiResponse(
            responseCode = "302",
            description =
                "Download found, follow the redirect to the data file (e.g. zip file)."),
        @ApiResponse(responseCode = "404", description = "Invalid download key."),
        @ApiResponse(
            responseCode = "410",
            description = "Download file was erased and is no longer available.")
      })
  @GetMapping(
      value = "{key}",
      produces = {
        APPLICATION_OCTET_STREAM_QS_VALUE,
        MediaType.APPLICATION_JSON_VALUE,
        "application/x-javascript"
      })
  public ResponseEntity<String> getResult(
      @PathVariable("key") @DownloadIdentifierPathParameter String downloadKey) {

    // if key contains avro or zip suffix remove it as we intend to work with the pure key
    downloadKey = StringUtils.removeEndIgnoreCase(downloadKey, AVRO_EXT);
    downloadKey = StringUtils.removeEndIgnoreCase(downloadKey, ZIP_EXT);

    Download download = occurrenceDownloadService.get(downloadKey);

    if (download == null) {
      return ResponseEntity.status(HttpStatus.NOT_FOUND)
          .body("\"Download with this key not found.\"\n");
    }

    assertDownloadType(download);

    if (download.getStatus() == FILE_ERASED) {
      return ResponseEntity.status(HttpStatus.GONE)
          .body("\"This download was erased, but the metadata is retained.\"\n");
    }

    String extension = download.getRequest().getFileExtension();

    LOG.debug("Get download data: [{}]", downloadKey);
    File downloadFile = requestService.getResultFile(download);

    String location = archiveServerUrl + downloadKey + extension;
    return ResponseEntity.status(HttpStatus.FOUND)
        .header(
            HttpHeaders.LAST_MODIFIED,
            new SimpleDateFormat().format(new Date(downloadFile.lastModified())))
        .location(URI.create(location))
        .body(location);
  }

  @Hidden
  @GetMapping("callback")
  public ResponseEntity<Object> airflowCallback(
      @RequestParam("job_id") String jobId, @RequestParam("status") String status) {
    LOG.debug("Received callback from Airflow for Job [{}] with status [{}]", jobId, status);
    callbackService.processCallback(jobId, status);
    return ResponseEntity.ok().build();
  }

  /** Request a new predicate download (POST method, public API). */
  @Operation(
      operationId = "requestDownload",
      summary = "Requests the creation of a download file.",
      description =
          "Starts the process of creating a download file. See the predicates "
              + "section to consult the requests accepted by this service and the limits section to refer "
              + "for information of how this service is limited per user.",
      extensions =
          @Extension(
              name = "Order",
              properties = @ExtensionProperty(name = "Order", value = "0010")))
  @Parameters(
      value = {
        @Parameter(name = "source", hidden = true),
        @Parameter(name = "User-Agent", in = ParameterIn.HEADER, hidden = true)
      })
  @io.swagger.v3.oas.annotations.parameters.RequestBody(
    content = @Content(
      schema = @Schema(
        oneOf = {PredicateDownloadRequest.class, SqlDownloadRequest.class},
        example = "{\n" +
          "  \"creator\": \"gbif_username\",\n" +
          "  \"sendNotification\": true,\n" +
          "  \"notification_address\": [\"gbif@example.org\"],\n" +
          "  \"format\": \"DWCA\",\n" +
          "  \"description\": \"A user-friendly description of the download request.\",\n" +
          "  \"machineDescription\": {\"key\": \"value\"},\n" +
          "  \"predicate\": {\n" +
          "    \"type\": \"and\",\n" +
          "    \"predicates\": [\n" +
          "      {\n" +
          "        \"type\": \"equals\",\n" +
          "        \"key\": \"COUNTRY\",\n" +
          "        \"value\": \"FR\"\n" +
          "      },\n" +
          "      {\n" +
          "        \"type\": \"equals\",\n" +
          "        \"key\": \"YEAR\",\n" +
          "        \"value\": \"2017\"\n" +
          "      }\n" +
          "    ]\n" +
          "  },\n" +
          "  \"verbatimExtensions\": [\n" +
          "    \"http://rs.tdwg.org/ac/terms/Multimedia\"\n" +
          "  ]\n" +
          "}"
      )
    )
  )
  @ApiResponses(
      value = {
        @ApiResponse(
            responseCode = "201",
            description = "Download requested, key returned."),
        @ApiResponse(
            responseCode = "400",
            description = "Invalid query, see [predicates](#predicates)."),
        @ApiResponse(
            responseCode = "429",
            description =
                "Too many downloads, wait for one of your downloads to complete. See [limits](#limits)")
      })
  @PostMapping(
      produces = {MediaType.TEXT_PLAIN_VALUE, MediaType.APPLICATION_JSON_VALUE},
      consumes = {MediaType.APPLICATION_JSON_VALUE})
  @ResponseBody
  @Secured(USER_ROLE)
  public ResponseEntity<String> startDownload(
      @NotNull @Valid @RequestBody DownloadRequest request,
      @Parameter(hidden = true) @RequestParam(name = "source", required = false) String source,
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
          createDownload(request, authentication, principal, parseSource(source, userAgent)));
    } catch (ResponseStatusException rse) {
      return ResponseEntity.status(rse.getStatusCode()).body(rse.getReason());
    }
  }

  /**
   * Creates/Starts a download.
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

    if (!checkUserInRole(authentication, REGISTRY_ADMIN)
        && downloadRequest instanceof PredicateDownloadRequest) {
      PredicateDownloadRequest predicateDownloadRequest =
          (PredicateDownloadRequest) downloadRequest;
      if (!predicateDownloadRequest.getFormat().equals(DownloadFormat.SPECIES_LIST)) {

        // Check for recent monthly downloads with the same predicate
        PagingResponse<Download> monthlyDownloads =
            occurrenceDownloadService.listByUser(
                "download.gbif.org",
                new PagingRequest(0, 50),
                EnumSet.of(PREPARING, RUNNING, SUCCEEDED, SUSPENDED),
                LocalDateTime.now().minus(35, ChronoUnit.DAYS),
                false);
        String existingMonthlyDownload =
            matchExistingDownload(monthlyDownloads, predicateDownloadRequest);
        if (existingMonthlyDownload != null) {
          return existingMonthlyDownload;
        }
      }

      // Check for recent user downloads (of this same user) with the same predicate
      PagingResponse<Download> userDownloads =
          occurrenceDownloadService.listByUser(
              userAuthenticated.getName(),
              new PagingRequest(0, 50),
              EnumSet.of(PREPARING, RUNNING, SUCCEEDED, SUSPENDED),
              LocalDateTime.now().minus(48, ChronoUnit.HOURS),
              false);
      String existingUserDownload = matchExistingDownload(userDownloads, predicateDownloadRequest);
      if (existingUserDownload != null) {
        return existingUserDownload;
      }
    }

    // SQL validation.
    if (downloadRequest.getFormat().equals(DownloadFormat.SQL_TSV_ZIP)) {
      try {
        String userSql = ((SqlDownloadRequest) downloadRequest).getSql();
        LOG.info("Received SQL download request «{}»", userSql);
        HiveSqlQuery sqlQuery = sqlValidation.validateAndParse(userSql, true);
        LOG.info("SQL is valid. Parsed as «{}».", sqlQuery.getSql());
        LOG.info("SQL is valid. Where clause is «{}».", sqlQuery.getSqlWhere());
        LOG.info("SQL is valid. SQL headers are «{}».", sqlQuery.getSqlSelectColumnNames());
      } catch (QueryBuildingException qbe) {
        LOG.info("SQL is invalid: {}", qbe.getMessage());
        throw new ResponseStatusException(HttpStatus.BAD_REQUEST, qbe.getMessage(), qbe);
      } catch (Exception e) {
        LOG.error("SQL is invalid with unexpected exception: "+e.getMessage(), e);
        throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage(), e);
      }
    }

    try {
      String downloadKey = requestService.create(downloadRequest, source);
      LOG.info("Created new download job with key [{}]", downloadKey);
      return downloadKey;
    } catch (ServiceUnavailableException sue) {
      LOG.error("Failed to create download request", sue);
      throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, sue.getMessage(), sue);
    }
  }

  /** Validates an SQL download request's SQL */
  @Operation(
    operationId = "validateDownloadRequest",
    summary = "**Experimental** Validates the SQL contained in an SQL download request.",
    description =
      "**Experimental** Validates the SQL in an SQL download request.  See the SQL section "
        + " for information on what queries are accepted.",
    extensions =
    @Extension(
      name = "Order",
      properties = @ExtensionProperty(name = "Order", value = "0040")))
  @io.swagger.v3.oas.annotations.parameters.RequestBody(
    content = @Content(
      schema = @Schema(
        oneOf = {SqlDownloadRequest.class},
        example = "{\n" +
          "  \"creator\": \"gbif_username\",\n" +
          "  \"sendNotification\": true,\n" +
          "  \"notification_address\": [\"gbif@example.org\"],\n" +
          "  \"format\": \"SQL_TSV_ZIP\",\n" +
          "  \"sql\": \"SELECT datasetKey, countryCode, COUNT(*) FROM occurrence WHERE continent = 'EUROPE' GROUP BY datasetKey, countryCode\"" +
          "}"
      )
    )
  )
  @ApiResponses(
    value = {
      @ApiResponse(
        responseCode = "200",
        description = "SQL is valid."),
      @ApiResponse(
        responseCode = "400",
        description = "Invalid query, see other documentation.")
    })
  @PostMapping(
    path = "validate",
    produces = {MediaType.APPLICATION_JSON_VALUE},
    consumes = {MediaType.APPLICATION_JSON_VALUE})
  public ResponseEntity<Object> validateRequest(@NotNull @Valid @RequestBody DownloadRequest downloadRequest) {
    if (downloadRequest.getFormat().equals(DownloadFormat.SQL_TSV_ZIP)) {
      try {
        String userSql = ((SqlDownloadRequest) downloadRequest).getSql();
        LOG.info("Received SQL download request for validation «{}»", userSql);
        HiveSqlQuery sqlQuery = sqlValidation.validateAndParse(userSql, false);
        LOG.info("SQL is valid. Parsed as «{}».", sqlQuery.getUserSql());
        LOG.info("SQL is valid. Where clause is «{}».", sqlQuery.getSqlWhere());
        LOG.info("SQL is valid. SQL headers are «{}».", sqlQuery.getSqlSelectColumnNames());
        ((SqlDownloadRequest) downloadRequest).setSql(sqlQuery.getUserSql());
        return ResponseEntity.ok(downloadRequest);
      } catch (QueryBuildingException qbe) {
        LOG.info("SQL is invalid: {}", qbe.getMessage());
        Map<String, Object> body = new HashMap<>();
        body.put("status", "INVALID");
        body.put("reason", qbe.getMessage());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(body);
      } catch (Exception e) {
        LOG.error("SQL is invalid with unexpected exception: "+e.getMessage(), e);
        Map<String, Object> body = new HashMap<>();
        body.put("status", "INVALID");
        body.put("reason", e.getMessage());
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        body.put("trace", sw.toString());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(body);
      }
    } else {
      LOG.debug("Received validation request for «{}», which is a predicate download", downloadRequest);
      return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY).body("\"Validation of predicate downloads not implemented.\"");
    }
  }

  /** Request a new download (GET method, internal API used by the portal). */
  @Hidden
  @GetMapping(produces = {MediaType.TEXT_PLAIN_VALUE, MediaType.APPLICATION_JSON_VALUE})
  @Secured(USER_ROLE)
  @ResponseBody
  public ResponseEntity<String> download(
      @Autowired HttpServletRequest httpRequest,
      @RequestParam(name = "notification_address", required = false) String emails,
      @RequestParam("format") String format,
      @RequestParam(name = "extensions", required = false) String extensions,
      @RequestParam(name = "interpretedExtensions", required = false) String interpretedExtensions,
      @RequestParam(name = "source", required = false) String source,
      @RequestParam(name = "description", required = false) String description,
      @RequestParam(name = "machineDescription", required = false) String machineDescriptionJson,
      @RequestParam(name = "checklistKey", required = false) String checklistKey,
      @Autowired Principal principal,
      @RequestHeader(value = "User-Agent") String userAgent) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(format), "Format can't be null");
    Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

    try {
      return ResponseEntity.ok(
          createDownload(
              downloadPredicate(
                  httpRequest,
                  emails,
                  format,
                  extensions,
                  interpretedExtensions,
                  description,
                  machineDescriptionJson,
                  checklistKey,
                  principal),
              authentication,
              principal,
              parseSource(source, userAgent)));
    } catch (ResponseStatusException rse) {
      return ResponseEntity.status(rse.getStatusCode()).body(rse.getReason());
    }
  }

  /**
   * Download predicate from a search string.
   */
  @Operation(
    operationId = "searchToPredicate",
    summary = "Converts a plain search query into a download predicate.",
    description =
      "Takes a search query used for the ordinary search API and returns a predicate suitable for the download " +
        "API.  In many cases, a query from the website can be converted using this method.",
    extensions =
    @Extension(
      name = "Order",
      properties = @ExtensionProperty(name = "Order", value = "0050")))
  @Parameters(
    value = {
      @Parameter(name = "notification_address", description = "Email notification address."),
      @Parameter(name = "format", description = "Download format."),
      @Parameter(name = "verbatimExtensions", description = "Verbatim extensions to include in a Darwin Core Archive " +
        "download."),
      // TODO: interpretedExtensions but it's only for events
      @Parameter(name = "checklistKey", description = "*Experimental.* The checklist to use that will supply interpreted" +
        " taxonomic fields. The default is to use the GBIF Backbone." +
        "download."),
    })
  @GetMapping("predicate")
  public DownloadRequest downloadPredicate(
      @Autowired HttpServletRequest httpRequest,
      @RequestParam(name = "notification_address", required = false) String emails,
      @RequestParam("format") String format,
      @RequestParam(name = "verbatimExtensions", required = false) String verbatimExtensions,
      @RequestParam(name = "interpretedExtensions", required = false) String interpretedExtensions,
      @RequestParam(name = "description", required = false) String description,
      @RequestParam(name = "machineDescription", required = false) String machineDescriptionJson,
      @RequestParam(name = "checklistKey", required = false) String checklistKey,
      @Autowired Principal principal) {
    DownloadFormat downloadFormat = VocabularyUtils.lookupEnum(format, DownloadFormat.class);
    Preconditions.checkArgument(Objects.nonNull(downloadFormat), "Format param is not present");
    String creator = principal != null ? principal.getName() : null;
    Set<String> notificationAddress = asSet(emails);
    Set<org.gbif.api.vocabulary.Extension> requestVerbatimExtensions = toExtension(verbatimExtensions);
    Set<org.gbif.api.vocabulary.Extension> requestInterpretedExtensions = toExtension(interpretedExtensions);
    Predicate predicate = PredicateFactory.build(httpRequest.getParameterMap());
    LOG.info("Predicate build for passing to download [{}]", predicate);

    JsonNode machineDescription = null;
    if (machineDescriptionJson != null) {
      try {
        ObjectMapper objectMapper = new ObjectMapper();
        machineDescription = objectMapper.readTree(machineDescriptionJson);  // Parse the JSON string
      } catch (JsonProcessingException e) {
        LOG.error("Could not parse the machineDescription {}", machineDescriptionJson, e);
      }
    }

    return new PredicateDownloadRequest(
        predicate,
        creator,
        notificationAddress,
        notificationAddress != null,
        downloadFormat,
        downloadType,
        description,
        machineDescription,
        requestVerbatimExtensions,
        requestInterpretedExtensions,
        checklistKey
    );
  }

  private static Set<org.gbif.api.vocabulary.Extension> toExtension(String extensions) {
    return Optional.ofNullable(asSet(extensions))
        .map(
            exts ->
                exts.stream()
                    .map(org.gbif.api.vocabulary.Extension::fromRowType)
                    .collect(Collectors.toSet()))
        .orElse(Collections.emptySet());
  }

  /**
   * Convert a predicate download request into an SQL download request
   */
  @Operation(
    operationId = "searchToSql",
    summary = "Converts a predicate download request into an SQL download request.",
    description =
      "Takes a predicate download request used by the occurrence download API and returns an SQL predicate request " +
        "suitable for the SQL download API.\n\n" +
        "The list of columns in the SELECT clause is that used for a SIMPLE_CSV or DWCA download, according to the " +
        "format used in the request.\n\n" +
        "**Experimental** This method may be changed in the future.",
    extensions =
    @Extension(
      name = "Order",
      properties = @ExtensionProperty(name = "Order", value = "0060")))
  @io.swagger.v3.oas.annotations.parameters.RequestBody(
    content = @Content(
      schema = @Schema(
        oneOf = {PredicateDownloadRequest.class},
        example = "{\n" +
          "  \"creator\": \"gbif_username\",\n" +
          "  \"sendNotification\": true,\n" +
          "  \"notification_address\": [\"gbif@example.org\"],\n" +
          "  \"format\": \"SIMPLE_CSV\",\n" +
          "  \"description\": \"A user-friendly description of the download request.\",\n" +
          "  \"machineDescription\": {\"key\": \"value\"},\n" +
          "  \"predicate\": {\n" +
          "    \"type\": \"and\",\n" +
          "    \"predicates\": [\n" +
          "      {\n" +
          "        \"type\": \"equals\",\n" +
          "        \"key\": \"COUNTRY\",\n" +
          "        \"value\": \"FR\"\n" +
          "      },\n" +
          "      {\n" +
          "        \"type\": \"equals\",\n" +
          "        \"key\": \"YEAR\",\n" +
          "        \"value\": \"2017\"\n" +
          "      }\n" +
          "    ]\n" +
          "  }\n" +
          "}"
      )
    )
  )
  @PostMapping(
    path = "sql",
    produces = {MediaType.APPLICATION_JSON_VALUE},
    consumes = {MediaType.APPLICATION_JSON_VALUE})
  public ResponseEntity<Object> downloadSqlPost(@NotNull @Valid @RequestBody PredicateDownloadRequest downloadRequest) {
    try {
      // SELECT column list is the same as a SIMPLE_CSV download by default, or a DWCA download if specified.
      StringBuilder generatedSql = new StringBuilder("SELECT ");
      if (downloadRequest.getFormat().equals(DownloadFormat.DWCA)) {
        generatedSql.append(DownloadTerms.DOWNLOAD_INTERPRETED_TERMS_WITH_GBIFID.stream()
          .map(t -> GbifTerm.verbatimScientificName == t ? "v_" + DwcTerm.scientificName.simpleName() :
            HiveColumns.isoEscapeColumnName(t.simpleName()))
          .collect(Collectors.joining(", ")));

        generatedSql.append(", ");

        generatedSql.append(DownloadTerms.DOWNLOAD_VERBATIM_TERMS.stream()
          .map(t -> "v_" + t.simpleName())
          .collect(Collectors.joining(", ")));
      } else {
        generatedSql.append(DownloadTerms.SIMPLE_DOWNLOAD_TERMS.stream()
          .map(HiveColumns::isoSqlColumnName)
          .collect(Collectors.joining(", ")));
      }

      generatedSql.append(" FROM occurrence");

      if (downloadRequest.getPredicate() != null) {
        String generatedWhereClause = QueryVisitorsFactory.createSqlQueryVisitor().buildQuery(downloadRequest.getPredicate());
        // This is not pretty.
        generatedWhereClause = generatedWhereClause
          .replaceAll("\\byear\\b", "\"year\"")
          .replaceAll("\\bmonth\\b", "\"month\"")
          .replaceAll("\\bday\\b", "\"day\"")
          .replaceAll("\\border\\b", "\"order\"")
          .replaceAll("\\bstringArrayContains\\b", "gbif_stringArrayContains")
          .replaceAll("\\bstringArrayLike\\b", "gbif_stringArrayLike")
          .replaceAll("\\bcontains\\b", "gbif_within")
          .replaceAll("\\bgeoDistance\\b", "gbif_geoDistance");

        generatedSql.append(" WHERE ").append(generatedWhereClause);
      }

      LOG.info("Validating generated SQL «{}»", generatedSql);
      HiveSqlQuery sqlQuery = sqlValidation.validateAndParse(generatedSql.toString(), false);
      LOG.info("SQL is valid. Parsed as «{}».", sqlQuery.getUserSql());
      LOG.info("SQL is valid. Where clause is «{}».", sqlQuery.getSqlWhere());
      LOG.info("SQL is valid. SQL headers are «{}».", sqlQuery.getSqlSelectColumnNames());
      SqlDownloadRequest request = new SqlDownloadRequest(
        sqlQuery.getUserSql(),
        downloadRequest.getCreator(),
        downloadRequest.getNotificationAddresses(),
        downloadRequest.getSendNotification(),
        DownloadFormat.SQL_TSV_ZIP,
        downloadType,
        downloadRequest.getDescription(),
        downloadRequest.getMachineDescription(),
        downloadRequest.getChecklistKey());
      LOG.info("Returning request {}", request);
      return ResponseEntity.ok(request);
    } catch (Exception e) {
      LOG.error("SQL generated from predicates is invalid: "+e.getMessage(), e);
      Map<String, Object> body = new HashMap<>();
      body.put("status", "INVALID");
      body.put("reason", e.getMessage());
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
      body.put("trace", sw.toString());
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(body);
    }
  }

  /**
   * SQL where clause from a search string.
   */
  @Operation(
    operationId = "searchToSql",
    summary = "Converts a plain search query into an SQL download predicate.",
    description =
      "Takes a search query used by the occurrence search API and returns an SQL Download query suitable for the SQL download " +
        "API.  In many cases, a query from the website can be converted using this method.\n\n" +
        "**Experimental** This method may be changed in the future.",
    extensions =
    @Extension(
      name = "Order",
      properties = @ExtensionProperty(name = "Order", value = "0062")))
  @GetMapping("sql")
  public ResponseEntity<Object> downloadSqlGet(@Autowired HttpServletRequest httpRequest,
    @RequestParam(name = "notification_address", required = false) String emails,
    @RequestParam(name = "description", required = false) String description,
    @RequestParam(name = "machineDescription", required = false) String machineDescriptionJson,
    @RequestParam(name = "checklistKey", required = false) String checklistKey,
    @Autowired Principal principal) {
    String creator = principal != null ? principal.getName() : null;
    Set<String> notificationAddress = asSet(emails);

    Predicate predicate = PredicateFactory.build(httpRequest.getParameterMap());
    LOG.info("Predicate build for passing to download [{}]", predicate);

    JsonNode machineDescription = null;
    if (machineDescriptionJson != null) {
      try {
        ObjectMapper objectMapper = new ObjectMapper();
        machineDescription = objectMapper.readTree(machineDescriptionJson);
      } catch (JsonProcessingException e) {
        LOG.error("Could not parse the machineDescription {}", machineDescriptionJson, e);
      }
    }

    PredicateDownloadRequest request = new PredicateDownloadRequest(
      predicate,
      creator,
      notificationAddress,
      notificationAddress != null,
      DownloadFormat.SIMPLE_CSV,
      downloadType,
      description,
      machineDescription,
      null,
      null,
      checklistKey);

    return downloadSqlPost(request);
  }

  /**
   * Search existingDownloads for a download with a format and predicate matching newDownload.
   *
   * @return The download key, if there's a match.
   */
  private String matchExistingDownload(
      PagingResponse<Download> existingDownloads, PredicateDownloadRequest newDownload) {
    for (Download existingDownload : existingDownloads.getResults()) {
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
        } else {
          LOG.info(
              "Download {} didn't match with new download with creator {}, format {}, type {} and predicate {}",
              existingDownload.getKey(),
              newDownload.getCreator(),
              newDownload.getFormat(),
              newDownload.getType(),
              newDownload.getPredicate());
        }
      } else {
        LOG.warn("Unexpected download type {}", existingDownload.getClass());
      }
    }

    LOG.info(
        "{} downloads found but none of them matched for user {}, format {}, type {} and predicate {}",
        existingDownloads.getCount(),
        newDownload.getCreator(),
        newDownload.getFormat(),
        newDownload.getType(),
        newDownload.getPredicate());

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
