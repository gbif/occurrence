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

import io.swagger.v3.oas.annotations.parameters.RequestBody;
import org.gbif.api.model.metrics.cube.Rollup;
import org.gbif.api.model.predicate.Predicate;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.TypeStatus;
import org.gbif.metrics.ws.provider.ProvidedCountPredicate;
import org.gbif.metrics.ws.service.OccurrenceMetricsService;

import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;

import io.swagger.v3.oas.annotations.Hidden;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;

/**
 * A simple generic resource that will look up a numerical count from the named cube and address
 * provided. Should no address be provided, a default builder which counts all records is used.
 */
@RestController
@RequestMapping(
    value = "occurrence",
    produces = {org.springframework.http.MediaType.APPLICATION_JSON_VALUE})
public class OccurrenceCubeResource {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceCubeResource.class);

  private final OccurrenceMetricsService metricsService;

  public OccurrenceCubeResource(OccurrenceMetricsService metricsService) {
    this.metricsService = metricsService;
  }

  /** Looks up an addressable count from the cube. */
  @Tag(name = "Occurrence metrics")
  @Operation(
      operationId = "getOccurrenceCount",
      summary = "Occurrence counts",
      description =
          "Returns occurrence counts for a predefined set of dimensions. "
              + "The supported dimensions are enumerated in the [/occurrence/count/schema](#operation/getOccurrenceCountSchema) service.")
  @Parameters(
      value = {
        @io.swagger.v3.oas.annotations.Parameter(
            name = "basisOfRecord",
            schema = @Schema(implementation = BasisOfRecord.class),
            in = ParameterIn.QUERY),
        @io.swagger.v3.oas.annotations.Parameter(
            name = "country",
            schema = @Schema(implementation = Country.class),
            in = ParameterIn.QUERY),
        @io.swagger.v3.oas.annotations.Parameter(
            name = "datasetKey",
            schema = @Schema(implementation = UUID.class),
            in = ParameterIn.QUERY),
        @io.swagger.v3.oas.annotations.Parameter(
            name = "isGeoreferenced",
            schema = @Schema(implementation = Boolean.class),
            in = ParameterIn.QUERY),
        @io.swagger.v3.oas.annotations.Parameter(
            name = "issue",
            schema = @Schema(implementation = OccurrenceIssue.class),
            in = ParameterIn.QUERY),
        @io.swagger.v3.oas.annotations.Parameter(
            name = "protocol",
            schema = @Schema(implementation = EndpointType.class),
            in = ParameterIn.QUERY),
        @io.swagger.v3.oas.annotations.Parameter(
            name = "publishingCountry",
            schema = @Schema(implementation = Country.class),
            in = ParameterIn.QUERY),
        @io.swagger.v3.oas.annotations.Parameter(
            name = "taxonKey",
            schema = @Schema(implementation = String.class),
            in = ParameterIn.QUERY),
        @io.swagger.v3.oas.annotations.Parameter(
            name = "typeStatus",
            schema = @Schema(implementation = TypeStatus.class),
            in = ParameterIn.QUERY),
        @io.swagger.v3.oas.annotations.Parameter(
            name = "year",
            schema = @Schema(implementation = Integer.class),
            in = ParameterIn.QUERY),
        @io.swagger.v3.oas.annotations.Parameter(
            name = "checklistKey",
            schema = @Schema(implementation = String.class),
            in = ParameterIn.QUERY)
      })
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "Count returned."),
        @ApiResponse(responseCode = "400", description = "Invalid query.", content = @Content)
      })
  @GetMapping("count")
  public Long count(
      @Parameter(hidden = true, description = "Built from query params (basisOfRecord, country, etc.)")
          @ProvidedCountPredicate
          Predicate predicate) {
    return metricsService.count(predicate);
  }

  /**
   * Count by predicate (POST). Used by occurrence-ws (download service) when deployed without ES.
   */
  @Hidden
  @RequestBody(
      content = @Content(schema = @Schema(implementation = Predicate.class)))
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "Count returned."),
        @ApiResponse(responseCode = "400", description = "Invalid predicate.", content = @Content)
      })
  @PostMapping(
      value = "count",
      consumes = org.springframework.http.MediaType.APPLICATION_JSON_VALUE)
  public Long countByPredicate(
      @org.springframework.web.bind.annotation.RequestBody
          @javax.validation.Valid
          Predicate predicate) {
    return metricsService.count(predicate);
  }

  @Tag(name = "Occurrence metrics")
  @Operation(
      operationId = "getOccurrenceCountSchema",
      summary = "Supported occurrence count metrics",
      description = "List the metrics supported by the service.")
  @ApiResponses(value = {@ApiResponse(responseCode = "200", description = "Supported metrics.")})
  @GetMapping("count/schema")
  public List<Rollup> getSchema() {
    return org.gbif.api.model.metrics.cube.OccurrenceCube.ROLLUPS;
  }

  @Tag(name = "Occurrence inventories")
  @Operation(
      operationId = "getOccurrenceInventoryBasisOfRecord",
      summary = "Occurrence inventory by basis of record")
  @ApiResponses(value = {@ApiResponse(responseCode = "200", description = "Inventory counts returned.")})
  @GetMapping("counts/basisOfRecord")
  public Map<String, Long> getBasisOfRecordCounts() {
    return metricsService.getBasisOfRecordCounts();
  }

  @Tag(name = "Occurrence inventories")
  @Operation(
      operationId = "getOccurrenceInventoryCountry",
      summary = "Occurrence inventory by country")
  @io.swagger.v3.oas.annotations.Parameter(name = "publishingCountry", in = ParameterIn.QUERY)
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "Inventory counts returned."),
        @ApiResponse(responseCode = "400", description = "Invalid query.", content = @Content)
      })
  @GetMapping("counts/countries")
  public Map<String, Long> getCountries(
      @RequestParam(value = "publishingCountry", required = false) String publishingCountry) {
    return metricsService.getCountries(publishingCountry);
  }

  @Tag(name = "Occurrence inventories")
  @Operation(
      operationId = "getOccurrenceInventoryDataset",
      summary = "Occurrence inventory by dataset")
  @io.swagger.v3.oas.annotations.Parameter(name = "country", in = ParameterIn.QUERY)
  @io.swagger.v3.oas.annotations.Parameter(name = "taxonKey", in = ParameterIn.QUERY)
  @io.swagger.v3.oas.annotations.Parameter(name = "checklistKey", in = ParameterIn.QUERY)
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "Inventory counts returned."),
        @ApiResponse(responseCode = "400", description = "Invalid query.", content = @Content)
      })
  @GetMapping("counts/datasets")
  public Map<String, Long> getDatasets(
      @RequestParam(value = "country", required = false) String country,
      @RequestParam(value = "nubKey", required = false) Integer nubKey,
      @RequestParam(value = "taxonKey", required = false) String taxonKey,
      @RequestParam(value = "checklistKey", required = false) String checklistKey) {
    return metricsService.getDatasets(country, nubKey, taxonKey, checklistKey);
  }

  @Hidden
  @Tag(name = "Occurrence inventories")
  @Operation(operationId = "getOccurrenceInventoryKingdom", summary = "Occurrence inventory by kingdom")
  @io.swagger.v3.oas.annotations.Parameter(name = "checklistKey", in = ParameterIn.QUERY)
  @ApiResponses(value = {@ApiResponse(responseCode = "200", description = "Inventory counts returned.")})
  @GetMapping("counts/kingdom")
  public Map<String, Long> getKingdomCounts(
      @RequestParam(value = "checklistKey", required = false) String checklistKey) {
    return metricsService.getKingdomCounts(checklistKey);
  }

  @Tag(name = "Occurrence inventories")
  @Operation(
      operationId = "getOccurrenceInventoryPublishingCountry",
      summary = "Occurrence inventory by publishing country")
  @io.swagger.v3.oas.annotations.Parameter(name = "country", in = ParameterIn.QUERY)
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "Inventory counts returned."),
        @ApiResponse(responseCode = "400", description = "Invalid query.", content = @Content)
      })
  @GetMapping("counts/publishingCountries")
  public Map<String, Long> getPublishingCountries(
      @RequestParam(value = "country", required = false) String country) {
    return metricsService.getPublishingCountries(country);
  }

  @Tag(name = "Occurrence inventories")
  @Operation(
      operationId = "getOccurrenceInventoryYear",
      summary = "Occurrence inventory by year")
  @io.swagger.v3.oas.annotations.Parameter(name = "year", in = ParameterIn.QUERY, example = "1981,1991")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "Inventory counts returned."),
        @ApiResponse(responseCode = "400", description = "Invalid query.", content = @Content)
      })
  @GetMapping("counts/year")
  public Map<String, Long> getYearCounts(
      @RequestParam(value = "year", required = false) String year) {
    if (!Strings.isNullOrEmpty(year)) {
      parseYearRange(year);
    }
    return metricsService.getYearCounts(year);
  }

  @VisibleForTesting
  protected static void parseYearRange(String year) {
    int now = Calendar.getInstance().get(Calendar.YEAR) + 1;
    String[] parts = year.split(",");
    if (parts.length == 1) {
      int from = Integer.parseInt(parts[0].trim());
      if (from < 1000 || from > now) {
        throw new IllegalArgumentException("Valid year range between 1000 and now expected");
      }
    } else if (parts.length == 2) {
      int from = Integer.parseInt(parts[0].trim());
      int to = Integer.parseInt(parts[1].trim());
      if (from < 1000 || to > now) {
        throw new IllegalArgumentException("Valid year range between 1000 and now expected, separated by a comma");
      }
    } else {
      throw new IllegalArgumentException("Parameter " + year + " is not a valid year range");
    }
  }
}
