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
package org.gbif.metrics.ws.resources;

import org.gbif.api.annotation.NullToNotFound;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.occurrence.search.OccurrenceGetByKey;

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

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;

import static org.gbif.ws.paths.OccurrencePaths.OCCURRENCE_PATH;
import static org.gbif.ws.paths.OccurrencePaths.VERBATIM_PATH;

/**
 * Occurrence-by-key endpoints.
 */
@Tag(
  name = "Occurrences",
  description = "Retrieval of single occurrence records (by gbifId or datasetKey+occurrenceId)."
)
@RestController
@RequestMapping(
  value = OCCURRENCE_PATH,
  produces = {MediaType.APPLICATION_JSON_VALUE, "application/x-javascript"}
)
public class OccurrenceByKeyResource {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceByKeyResource.class);

  private final OccurrenceGetByKey occurrenceGetByKey;

  @Autowired
  public OccurrenceByKeyResource(OccurrenceGetByKey occurrenceGetByKey) {
    this.occurrenceGetByKey = occurrenceGetByKey;
  }

  @Operation(
      operationId = "getOccurrenceById",
      summary = "Occurrence by id",
      description = "Retrieve details for a single, interpreted occurrence by its gbifId.")
  @ApiResponse(responseCode = "200", description = "Occurrence found")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "400", description = "Invalid identifier", content = {}),
        @ApiResponse(responseCode = "404", description = "Occurrence not found", content = {})
      })
  @NullToNotFound
  @GetMapping("{gbifId}")
  public Occurrence get(@PathVariable("gbifId") Long gbifId) {
    LOG.debug("Request Occurrence [{}]", gbifId);
    return occurrenceGetByKey.get(gbifId);
  }

  @Operation(
      operationId = "getOccurrenceByDatasetKeyAndOccurrenceId",
      summary = "Occurrence by dataset key and occurrence id",
      description = "Retrieve a single, interpreted occurrence by its dataset key and occurrenceId in that dataset.")
  @Parameter(name = "datasetKey", schema = @Schema(implementation = UUID.class), in = ParameterIn.PATH)
  @Parameter(name = "occurrenceId", schema = @Schema(implementation = String.class), in = ParameterIn.PATH)
  @ApiResponse(responseCode = "200", description = "Occurrence found")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "400", description = "Invalid identifier", content = {}),
        @ApiResponse(responseCode = "404", description = "Occurrence not found", content = {})
      })
  @GetMapping("{datasetKey}/{occurrenceId}")
  @ResponseBody
  @NullToNotFound
  public Occurrence get(
      @PathVariable("datasetKey") UUID datasetKey,
      @PathVariable("occurrenceId") String occurrenceId) {
    LOG.debug("Retrieve occurrence by dataset [{}] and occurrenceId [{}]", datasetKey, occurrenceId);
    return occurrenceGetByKey.get(datasetKey, occurrenceId);
  }

  @Operation(
      operationId = "getVerbatimOccurrenceById",
      summary = "Verbatim occurrence by id",
      description = "Retrieve a single, verbatim occurrence without any interpretation.")
  @ApiResponse(responseCode = "200", description = "Verbatim occurrence found")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "400", description = "Invalid identifier", content = {}),
        @ApiResponse(responseCode = "404", description = "Occurrence not found", content = {})
      })
  @GetMapping("{gbifId}/" + VERBATIM_PATH)
  @NullToNotFound
  public VerbatimOccurrence getVerbatim(@PathVariable("gbifId") Long gbifId) {
    LOG.debug("Request VerbatimOccurrence [{}]", gbifId);
    return occurrenceGetByKey.getVerbatim(gbifId);
  }

  @Operation(
      operationId = "getVerbatimOccurrenceByDatasetKeyAndOccurrenceId",
      summary = "Verbatim occurrence by dataset key and occurrence id",
      description = "Retrieve a single, verbatim occurrence without any interpretation by its dataset key and occurrenceId.")
  @Parameter(name = "datasetKey", schema = @Schema(implementation = UUID.class), in = ParameterIn.PATH)
  @Parameter(name = "occurrenceId", schema = @Schema(implementation = String.class), in = ParameterIn.PATH)
  @ApiResponse(responseCode = "200", description = "Verbatim occurrence found")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "400", description = "Invalid identifier", content = {}),
        @ApiResponse(responseCode = "404", description = "Occurrence not found", content = {})
      })
  @GetMapping("{datasetKey}/{occurrenceId}/" + VERBATIM_PATH)
  @ResponseBody
  @NullToNotFound
  public VerbatimOccurrence getVerbatim(
      @PathVariable("datasetKey") UUID datasetKey,
      @PathVariable("occurrenceId") String occurrenceId) {
    LOG.debug("Retrieve verbatim occurrence by dataset [{}] and occurrenceId [{}]", datasetKey, occurrenceId);
    return occurrenceGetByKey.getVerbatim(datasetKey, occurrenceId);
  }
}
