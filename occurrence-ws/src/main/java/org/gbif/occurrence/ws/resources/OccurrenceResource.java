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
package org.gbif.occurrence.ws.resources;

import io.swagger.v3.oas.annotations.*;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.servers.Server;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.gbif.api.annotation.NullToNotFound;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.service.occurrence.OccurrenceService;
import org.gbif.occurrence.persistence.experimental.OccurrenceRelationshipService;
import org.gbif.occurrence.search.OccurrenceGetByKey;
import org.gbif.occurrence.ws.provider.OccurrenceDwcXMLConverter;
import org.gbif.occurrence.ws.provider.OccurrenceVerbatimDwcXMLConverter;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static org.gbif.ws.paths.OccurrencePaths.FRAGMENT_PATH;
import static org.gbif.ws.paths.OccurrencePaths.OCCURRENCE_PATH;
import static org.gbif.ws.paths.OccurrencePaths.VERBATIM_PATH;

/**
 * Occurrence resource, the verbatim sub resource, and occurrence metrics.
 */
@OpenAPIDefinition(
  info = @Info(
    title = "Occurrence API",
    version = "v1",
    description =
      "This API works against the GBIF Occurrence Store, which handles occurrence records and makes them available through the web service and download files.\n"+
        "Internally we use a [Java web service client](https://github.com/gbif/occurrence/tree/master/occurrence-ws-client) for the consumption of these HTTP-based, RESTful web services.\n",
    termsOfService = "https://www.gbif.org/terms"),
  servers = {
    @Server(url = "https://api.gbif.org/v1/", description = "Production"),
    @Server(url = "https://api.gbif-uat.org/v1/", description = "User testing")
  })
@Tag(name = "Occurrences", description = "This API provides services related to the retrieval of single occurrence records.")
@RestController
@RequestMapping(
  value = OCCURRENCE_PATH,
  produces = {MediaType.APPLICATION_JSON_VALUE, "application/x-javascript"}
)
public class OccurrenceResource {

  @VisibleForTesting
  public static final String ANNOSYS_PATH = "annosys";

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceResource.class);

  private final OccurrenceService occurrenceService;
  private final OccurrenceRelationshipService occurrenceRelationshipService;
  private final OccurrenceGetByKey occurrenceGetByKey;

  @Autowired
  public OccurrenceResource(
    OccurrenceService occurrenceService,
    OccurrenceGetByKey occurrenceGetByKey,
    OccurrenceRelationshipService occurrenceRelationshipService
  ) {
    this.occurrenceService = occurrenceService;
    this.occurrenceGetByKey = occurrenceGetByKey;
    this.occurrenceRelationshipService = occurrenceRelationshipService;
  }

  /**
   * A stable (old!) GBIF id parameter for documentation.
   */
  @Target({PARAMETER, METHOD, FIELD, ANNOTATION_TYPE})
  @Retention(RetentionPolicy.RUNTIME)
  @Inherited
  @Parameter(
    name = "gbifId",
    description = "Integer gbifId for the occurrence.",
    example = "1258202889",
    schema = @Schema(implementation = Long.class, minimum = "1"),
    in = ParameterIn.PATH)
  @interface GbifIdPathParameter {}

  /**
   * Stable dataset key and occurrence id parameters for documentation.
   */
  @Target({PARAMETER, METHOD, FIELD, ANNOTATION_TYPE})
  @Retention(RetentionPolicy.RUNTIME)
  @Inherited
  @Parameters(
    value = {
      @Parameter(
        name = "datasetKey",
        description = "UUID key for the dataset.",
        example = "0001480b-76ca-4f30-86bc-f4292481554b",
        schema = @Schema(implementation = Long.class, minimum = "1"),
        in = ParameterIn.PATH),
      @Parameter(
        name = "occurrenceId",
        description = "Occurrence ID from the dataset.",
        example = "651D49B2-FF77-7F3F-E053-2614A8C050DE",
        schema = @Schema(implementation = Long.class, minimum = "1"),
        in = ParameterIn.PATH),
    }
  )
  @interface DatasetKeyOccurrenceIdPathParameters {}

  /**
   * This retrieves a single Occurrence detail by its gbifId from the occurrence store.
   *
   * @param gbifId Occurrence gbifId
   * @return requested Occurrence or null if none could be found
   */
  @Operation(
    operationId = "getOccurrenceById",
    summary = "Occurrence by id",
    description = "Retrieve details for a single, interpreted occurrence")
  @GbifIdPathParameter
  @ApiResponses(
    value = {
      @ApiResponse(
        responseCode = "200",
        description = "Occurrence found",
        content = {
          @Content(
            mediaType = "application/json",
            schema = @Schema(implementation = Occurrence.class))
        }),
      @ApiResponse(
        responseCode = "400",
        description = "Invalid identifier supplied",
        content = @Content),
      @ApiResponse(
        responseCode = "404",
        description = "Occurrence not found",
        content = @Content)
    })
  @NullToNotFound
  @GetMapping("{gbifId}")
  public Occurrence get(@PathVariable("gbifId") Long gbifId) {
    LOG.debug("Request Occurrence [{}]:", gbifId);
    return occurrenceGetByKey.get(gbifId);
  }

  /**
   * This retrieves a single occurrence fragment in its raw form as a string.
   *
   * @param gbifId The Occurrence gbifId
   * @return requested occurrence fragment or null if none could be found
   */
  @Operation(
    operationId = "getOccurrenceFragmentById",
    summary = "Occurrence fragment by id",
    description = "Retrieve a single occurrence fragment in its raw form (JSON or XML)")
  @Parameters(
    value = {
      @Parameter(
        name = "gbifId",
        description = "Integer gbifId for the occurrence.",
        examples = {@ExampleObject("1258202889"), @ExampleObject("142316233")},
        schema = @Schema(implementation = Long.class, minimum = "1"),
        in = ParameterIn.PATH),
    }
  )
  @ApiResponses(
    value = {
      @ApiResponse(
        responseCode = "200",
        description = "Occurrence fragment in JSON or XML format",
        content = {
          @Content(
            mediaType = "application/json",
            schema = @Schema(implementation = Occurrence.class)),
          @Content(
            mediaType = "text/xml",
            schema = @Schema(implementation = Occurrence.class))
        }),
      @ApiResponse(
        responseCode = "400",
        description = "Invalid identifier supplied",
        content = @Content),
      @ApiResponse(
        responseCode = "404",
        description = "Occurrence fragment not found",
        content = @Content)
    })
  @GetMapping("/{gbifId}/" + FRAGMENT_PATH)
  @ResponseBody
  @NullToNotFound
  public String getFragment(@PathVariable("gbifId") Long gbifId) {
    LOG.debug("Request occurrence fragment [{}]:", gbifId);
    // TODO: Content type needs to be text/xml if necessary.
    return occurrenceService.getFragment(gbifId);
  }

  /**
   * This retrieves a single occurrence fragment in its raw form as a string.
   *
   * @param datasetKey dataset UUID identifier
   * @param occurrenceId record identifier in the dataset
   * @return requested occurrence or null if none could be found
   */
  @Operation(
    operationId = "getOccurrenceFragmentByDatasetKeyAndOccurrenceId",
    summary = "Occurrence fragment by dataset key and occurrence id",
    description = "Retrieve a single occurrence fragment in its raw form (JSON or XML) by its dataset key and occurrenceId in that dataset")
  @DatasetKeyOccurrenceIdPathParameters
  @ApiResponses(
    value = {
      @ApiResponse(
        responseCode = "200",
        description = "Occurrence fragment found",
        content = {
          @Content(
            mediaType = "application/json",
            schema = @Schema(implementation = Occurrence.class)),
          @Content(
            mediaType = "text/xml",
            schema = @Schema(implementation = Occurrence.class))
        }),
      @ApiResponse(
        responseCode = "400",
        description = "Invalid identifier(s) supplied",
        content = @Content),
      @ApiResponse(
        responseCode = "404",
        description = "Dataset or occurrenceId not found",
        content = @Content)
    })
  @GetMapping("/{datasetKey}/{occurrenceId}/" + FRAGMENT_PATH)
  @ResponseBody
  @NullToNotFound
  public String getFragment(@PathVariable("datasetKey") UUID datasetKey, @PathVariable("occurrenceId") String occurrenceId) {
    LOG.debug("Retrieve occurrence by dataset [{}] and occcurrenceId [{}]", datasetKey, occurrenceId);
    Occurrence occurrence = occurrenceGetByKey.get(datasetKey, occurrenceId);
    if (occurrence != null) {
      // TODO: Content type needs to be text/xml if necessary.
      return getFragment(occurrence.getKey());
    }
    return null;
  }

  /**
   * This retrieves a single Occurrence detail from the occurrence store.
   *
   * @param datasetKey dataset UUID identifier
   * @param occurrenceId record identifier in the dataset
   *
   * @return requested occurrence or null if none could be found
   */
  @Operation(
    operationId = "getOccurrenceByDatasetKeyAndOccurrenceId",
    summary = "Occurrence by dataset key and occurrence id",
    description = "Retrieve a single, interpreted occurrence by its dataset key and occurrenceId in that dataset")
  @DatasetKeyOccurrenceIdPathParameters
  @ApiResponses(
    value = {
      @ApiResponse(
        responseCode = "200",
        description = "Occurrence found",
        content = {
          @Content(
            mediaType = "application/json",
            schema = @Schema(implementation = Occurrence.class))
        }),
      @ApiResponse(
        responseCode = "400",
        description = "Invalid identifier supplied",
        content = @Content),
      @ApiResponse(
        responseCode = "404",
        description = "Occurrence not found",
        content = @Content)
    })
  @GetMapping("/{datasetKey}/{occurrenceId}")
  @ResponseBody
  @NullToNotFound
  public Occurrence get(@PathVariable("datasetKey") UUID datasetKey, @PathVariable("occurrenceId") String occurrenceId) {
    LOG.debug("Retrieve occurrence by dataset [{}] and occcurrenceId [{}]", datasetKey, occurrenceId);
    return occurrenceGetByKey.get(datasetKey, occurrenceId);
  }

  /**
   * This retrieves a single VerbatimOccurrence detail by its key from the occurrence store and transforms it into the API
   * version which uses Maps.
   *
   * @param gbifId The Occurrence gbifId
   * @return requested VerbatimOccurrence or null if none could be found
   */
  @Operation(
    operationId = "getVerbatimOccurrenceById",
    summary = "Verbatim occurrence by id",
    description = "Retrieve a single, verbatim occurrence without any interpretation")
  @GbifIdPathParameter
  @ApiResponses(
    value = {
      @ApiResponse(
        responseCode = "200",
        description = "Occurrence found",
        content = {
          @Content(
            mediaType = "application/json",
            schema = @Schema(implementation = VerbatimOccurrence.class))
        }),
      @ApiResponse(
        responseCode = "400",
        description = "Invalid identifier supplied",
        content = @Content),
      @ApiResponse(
        responseCode = "404",
        description = "Occurrence not found",
        content = @Content)
    })
  @GetMapping("/{gbifId}/" + VERBATIM_PATH)
  @NullToNotFound
  public VerbatimOccurrence getVerbatim(@PathVariable("gbifId") Long gbifId) {
    LOG.debug("Request VerbatimOccurrence [{}]:", gbifId);
    return occurrenceGetByKey.getVerbatim(gbifId);
  }

  /**
   * This retrieves a single occurrence fragment in its raw form as a string.
   *
   * @param datasetKey dataset UUID identifier
   * @param occurrenceId record identifier in the dataset
   *
   * @return requested VerbatimOccurrence or null if none could be found
   */
  @Operation(
    operationId = "getVerbatimOccurrenceByDatasetKeyAndOccurrenceId",
    summary = "Verbatim occurrence by dataset key and occurrence id",
    description = "Retrieve a single, verbatim occurrence without any interpretation by its dataset key and occurrenceId")
  @DatasetKeyOccurrenceIdPathParameters
  @ApiResponses(
    value = {
      @ApiResponse(
        responseCode = "200",
        description = "Occurrence found",
        content = {
          @Content(
            mediaType = "application/json",
            schema = @Schema(implementation = Occurrence.class))
        }),
      @ApiResponse(
        responseCode = "400",
        description = "Invalid identifier supplied",
        content = @Content),
      @ApiResponse(
        responseCode = "404",
        description = "Occurrence not found",
        content = @Content)
    })
  @GetMapping("/{datasetKey}/{occurrenceId}/" + VERBATIM_PATH)
  @ResponseBody
  @NullToNotFound
  public VerbatimOccurrence getVerbatim(@PathVariable("datasetKey") UUID datasetKey, @PathVariable("occurrenceId") String occurrenceId) {
    LOG.debug("Retrieve occurrence verbatim by dataset [{}] and occurrenceId [{}]", datasetKey, occurrenceId);
    return occurrenceGetByKey.getVerbatim(datasetKey, occurrenceId);
  }

  /**
   * Provides a list of related occurrence records in JSON.
   * @return A list of related occurrences or an empty list if relationships are not configured or none exist.
   */
  @Operation(
    operationId = "experimentalGetRelatedOccurrences",
    summary = "Related occurrences by gbifId",
    description = "**Experimental** Retrieve a list of related occurrences")
  @Parameters(
    value = {
      @Parameter(
        name = "gbifId",
        description = "Integer gbifId for the occurrence.",
        example = "1056006536",
        schema = @Schema(implementation = Long.class, minimum = "1"),
        in = ParameterIn.PATH),
    }
  )
  @ApiResponses(
    value = {
      @ApiResponse(
        responseCode = "200",
        description = "Occurrence found, clusters listed if present",
        content = @Content),
      @ApiResponse(
        responseCode = "400",
        description = "Invalid identifier supplied",
        content = @Content),
      @ApiResponse(
        responseCode = "404",
        description = "Occurrence not found",
        content = @Content)
    })
  @GetMapping("/{gbifId}/experimental/related")
  public String getRelatedOccurrences(@PathVariable("gbifId") Long gbifId) {
    LOG.debug("Request RelatedOccurrences [{}]:", gbifId);
    List<String> relationshipsAsJsonSnippets = occurrenceRelationshipService.getRelatedOccurrences(gbifId);
    String currentOccurrenceAsJson = occurrenceRelationshipService.getCurrentOccurrence(gbifId);
    return String.format("{\"currentOccurrence\":%s,\"relatedOccurrences\":[%s]}",
      currentOccurrenceAsJson,
      String.join(",", relationshipsAsJsonSnippets));
  }

  /**
   * Removed API call, which supported a stream of featured occurrences on the old GBIF.org homepage.
   * @return An empty list.
   */
  @Hidden
  @GetMapping("featured")
  @ResponseBody
  @Deprecated
  public List<Object> getFeaturedOccurrences() {
    LOG.warn("Featured occurrences have been removed.");
    return Lists.newArrayList();
  }

  /**
   * This method is implemented specifically to support Annosys and is not advertised or
   * documented in the public API.  <em>It may be removed at any time without notice</em>.
   *
   * @param gbifId
   * @return
   */
  @Hidden
  @GetMapping(
    value = ANNOSYS_PATH + "/{gbifId}",
    produces = MediaType.APPLICATION_XML_VALUE
  )
  public String getAnnosysOccurrence(@PathVariable("gbifId") Long gbifId) {
    LOG.debug("Request Annosys occurrence [{}]:", gbifId);
    return OccurrenceDwcXMLConverter.occurrenceXMLAsString(occurrenceGetByKey.get(gbifId));
  }

  /**
   * This method is implemented specifically to support Annosys and is not advertised or
   * documented in the public API.  <em>It may be removed at any time without notice</em>.
   *
   * @param gbifId
   * @return
   */
  @Hidden
  @NullToNotFound
  @GetMapping(
    value = ANNOSYS_PATH + "/{gbifId}/" + VERBATIM_PATH,
    produces = MediaType.APPLICATION_XML_VALUE
  )
  public String getAnnosysVerbatim(@PathVariable("gbifId") Long gbifId) {
    LOG.debug("Request Annosys verbatim occurrence [{}]:", gbifId);
    return OccurrenceVerbatimDwcXMLConverter.verbatimOccurrenceXMLAsString(occurrenceGetByKey.getVerbatim(gbifId));
  }
}
