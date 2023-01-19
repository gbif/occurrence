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

import io.swagger.v3.oas.annotations.Hidden;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.Explode;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.search.OccurrencePredicateSearchRequest;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.gbif.api.util.Range;
import org.gbif.api.util.VocabularyUtils;
import org.gbif.api.vocabulary.*;
import org.gbif.occurrence.search.SearchTermService;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.security.access.annotation.Secured;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.view.RedirectView;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static org.gbif.api.model.common.paging.PagingConstants.PARAM_LIMIT;
import static org.gbif.api.model.common.search.SearchConstants.QUERY_PARAM;
import static org.gbif.ws.paths.OccurrencePaths.*;

/**
 * Occurrence resource.
 */
@Tag(
  name = "Searching occurrences",
  description = "This API provides services for searching occurrence records that have been indexed by GBIF.\n\n" +
  "In order to retrieve all results for a given search filter you need to issue individual requests for each page, which is limited to " +
  "a maximum size of 300 records per page. Note that for technical reasons we also have a hard limit for any query of 100,000 records. " +
  "You will get an error if the offset + limit exceeds 100,000. To retrieve all records beyond 100,000 you should use our asynchronous " +
  "download service (below) instead.")
@RestController
@RequestMapping(
  value = OCC_SEARCH_PATH,
  produces = {MediaType.APPLICATION_JSON_VALUE, "application/x-javascript"}
)
public class OccurrenceSearchResource {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceSearchResource.class);

  private static final String USER_ROLE = "USER";

  private final OccurrenceSearchService searchService;

  private final SearchTermService searchTermService;

  @Autowired
  public OccurrenceSearchResource(OccurrenceSearchService searchService, SearchTermService searchTermService) {
    this.searchService = searchService;
    this.searchTermService = searchTermService;
  }

  // TODO Document
  @PostMapping("predicate")
  public SearchResponse<Occurrence,OccurrenceSearchParameter> search(@NotNull @Valid @RequestBody OccurrencePredicateSearchRequest request) {
     LOG.debug("Executing query, predicate {}, limit {}, offset {}", request.getPredicate(), request.getLimit(),
              request.getOffset());
    return searchService.search(request);
  }

  // TODO Document or hide?
  @PostMapping
  public SearchResponse<Occurrence,OccurrenceSearchParameter> postSearch(@NotNull @Valid @RequestBody OccurrenceSearchRequest request) {
    LOG.debug("Executing query, parameters {}, limit {}, offset {}", request.getParameters(), request.getLimit(),
              request.getOffset());
    return searchService.search(request);
  }

  /**
   * The usual (search) limit and offset parameters
   */
  @Target({PARAMETER, METHOD, FIELD, ANNOTATION_TYPE})
  @Retention(RetentionPolicy.RUNTIME)
  @Inherited
  @Parameters(
    value = {
      @Parameter(
        name = "limit",
        description = "Controls the number of results in the page. Using too high a value will be overwritten with the default maximum threshold, depending on the service. Sensible defaults are used so this may be omitted.",
        schema = @Schema(implementation = Integer.class, minimum = "0"),
        in = ParameterIn.QUERY),
      @Parameter(
        name = "offset",
        description = "Determines the offset for the search results. A limit of 20 and offset of 40 will get the third page of 20 results. Some services have a maximum offset.",
        schema = @Schema(implementation = Integer.class, minimum = "0"),
        in = ParameterIn.QUERY),
    }
  )
  @interface CommonOffsetLimitParameters {}

  /**
   * The usual (search) facet parameters
   */
  @Target({PARAMETER, METHOD, FIELD, ANNOTATION_TYPE})
  @Retention(RetentionPolicy.RUNTIME)
  @Inherited
  @Parameters(
    value = {
      @Parameter(
        name = "facet",
        description =
          "A facet name used to retrieve the most frequent values for a field. Facets are allowed for all the parameters except for: eventDate, geometry, lastInterpreted, locality, organismId, stateProvince, waterBody. This parameter may by repeated to request multiple facets, as in this example /occurrence/search?facet=datasetKey&facet=basisOfRecord&limit=0",
        schema = @Schema(implementation = String.class),
        in = ParameterIn.QUERY),
      @Parameter(
        name = "facetMincount",
        description =
          "Used in combination with the facet parameter. Set facetMincount={#} to exclude facets with a count less than {#}, e.g. /search?facet=type&limit=0&facetMincount=10000 only shows the type value 'OCCURRENCE' because 'CHECKLIST' and 'METADATA' have counts less than 10000.",
        schema = @Schema(implementation = Integer.class),
        in = ParameterIn.QUERY),
      @Parameter(
        name = "facetMultiselect",
        description =
          "Used in combination with the facet parameter. Set facetMultiselect=true to still return counts for values that are not currently filtered, e.g. /search?facet=type&limit=0&type=CHECKLIST&facetMultiselect=true still shows type values 'OCCURRENCE' and 'METADATA' even though type is being filtered by type=CHECKLIST",
        schema = @Schema(implementation = Boolean.class),
        in = ParameterIn.QUERY),
      @Parameter(
        name = "facetLimit",
        description =
          "Facet parameters allow paging requests using the parameters facetOffset and facetLimit",
        schema = @Schema(implementation = Integer.class),
        in = ParameterIn.QUERY),
      @Parameter(
        name = "facetOffset",
        description =
          "Facet parameters allow paging requests using the parameters facetOffset and facetLimit",
        schema = @Schema(implementation = Integer.class, minimum = "0"),
        in = ParameterIn.QUERY)
    }
  )
  @interface CommonFacetParameters {}

  @Operation(
    operationId = "searchOccurrence",
    summary = "Occurrence search",
    description = "Full search across all occurrences. Results are ordered by relevance.") // TODO: is that true?
  @Parameters(
    value = {
      @Parameter(
        name = "basisOfRecord",
        description = "Basis of record, as defined in our BasisOfRecord vocabulary",
        schema = @Schema(implementation = BasisOfRecord.class),
        in = ParameterIn.QUERY,
        explode = Explode.TRUE),
      @Parameter(
        name = "catalogNumber",
        description = "An identifier of any form assigned by the source within a physical collection or digital dataset for the record which may not be unique, but should be fairly unique in combination with the institution and collection code.",
        schema = @Schema(implementation = String.class),
        in = ParameterIn.QUERY),
      @Parameter(
        name = "classKey",
        description = "Class classification key.",
        schema = @Schema(implementation = Integer.class, minimum = "0"),
        in = ParameterIn.QUERY),
      @Parameter(
        name = "collectionCode",
        description = "An identifier of any form assigned by the source to identify the physical collection or digital dataset uniquely within the context of an institution.",
        schema = @Schema(implementation = String.class),
        in = ParameterIn.QUERY),
      @Parameter(
        name = "continent",
        description = "Continent, as defined in our Continent vocabulary",
        schema = @Schema(implementation = Continent.class),
        in = ParameterIn.QUERY,
        explode = Explode.TRUE),
      @Parameter(
        name = "coordinateUncertaintyInMeters",
        description = "The horizontal distance (in meters) from the given decimalLatitude and decimalLongitude describing the smallest circle containing the whole of the Location. Supports range queries.", // TODO LINK
        schema = @Schema(implementation = Range.class), // TODO
        in = ParameterIn.QUERY),
      @Parameter(
        name = "country",
        description = "The 2-letter country code (as per ISO-3166-1) of the country in which the occurrence was recorded.",
        schema = @Schema(implementation = Country.class),
        in = ParameterIn.QUERY,
        explode = Explode.FALSE),
      @Parameter(
        name = "crawlId",
        description = "Crawl attempt that harvested this record.",
        schema = @Schema(implementation = Integer.class),
        in = ParameterIn.QUERY),
      @Parameter(
        name = "datasetId",
        description = "The ID of the dataset.",
        schema = @Schema(implementation = String.class),
        in = ParameterIn.QUERY),
      @Parameter(
        name = "datasetKey",
        description = "The occurrence dataset key (a UUID).",
        schema = @Schema(implementation = UUID.class),
        in = ParameterIn.QUERY),
      @Parameter(
        name = "datasetName",
        description = "The name of the dataset.",
        schema = @Schema(implementation = String.class),
        in = ParameterIn.QUERY),
      @Parameter(
        name = "decimalLatitude",
        description = "Latitude in decimals between -90 and 90 based on WGS 84. Supports range queries.", // TODO
        schema = @Schema(implementation = Range.class), // TODO
        in = ParameterIn.QUERY),
      @Parameter(
        name = "decimalLongitude",
        description = "Longitude in decimals between -180 and 180 based on WGS 84. Supports range queries.", // TODO
        schema = @Schema(implementation = Double.class), // TOD
        in = ParameterIn.QUERY),
      @Parameter(
        name = "depth",
        description = "Depth in meters relative to altitude. For example 10 meters below a lake surface with given altitude. Supports range queries.",
        schema = @Schema(implementation = Double.class),
        in = ParameterIn.QUERY),
      @Parameter(
        name = "elevation",
        description = "Elevation (altitude) in meters above sea level. Supports range queries.",
        schema = @Schema(implementation = Range.class),
        in = ParameterIn.QUERY),
      @Parameter(
        name = "establishmentMeans",
        description = "EstablishmentMeans, as defined in our EstablishmentMeans enum",
        schema = @Schema(implementation = String.class), // TODO
        in = ParameterIn.QUERY),
      @Parameter(
        name = "eventDate",
        description = "Occurrence date in ISO 8601 format: yyyy, yyyy-MM, yyyy-MM-dd, or MM-dd. Supports range queries.",
        schema = @Schema(implementation = Date.class),
        in = ParameterIn.QUERY),
      @Parameter(
        name = "eventId",
        description = "An identifier for the information associated with a sampling event.",
        schema = @Schema(implementation = String.class),
        in = ParameterIn.QUERY),
      @Parameter(
        name = "familyKey",
        description = "Family classification key.",
        schema = @Schema(implementation = Integer.class, minimum = "0"),
        in = ParameterIn.QUERY),
      @Parameter(
        name = "format",
        description = "Export format, accepts TSV(default) and CSV",
        schema = @Schema(implementation = String.class),
        in = ParameterIn.QUERY),
      @Parameter(
        name = "fromDate", // Huh?
        description = "Start partial date of a date range, accepts the format yyyy-MM, for example: 2015-11",
        schema = @Schema(implementation = String.class),
        in = ParameterIn.QUERY),
      @Parameter(
        name = "gadmGid",
        description = "A GADM geographic identifier at any level, for example AGO, AGO.1_1, AGO.1.1_1 or AGO.1.1.1_1",
        schema = @Schema(implementation = String.class),
        in = ParameterIn.QUERY),
      @Parameter(
        name = "gadmLevel",
        description = "A GADM region level, valid values range from 0 to 3",
        schema = @Schema(implementation = String.class),
        in = ParameterIn.QUERY),
      @Parameter(
        name = "gadmLevel0Gid",
        description = "A GADM geographic identifier at the zero level, for example AGO",
        schema = @Schema(implementation = String.class),
        in = ParameterIn.QUERY),
      @Parameter(
        name = "gadmLevel1Gid",
        description = "A GADM geographic identifier at the first level, for example AGO.1_1",
        schema = @Schema(implementation = String.class),
        in = ParameterIn.QUERY),
      @Parameter(
        name = "gadmLevel2Gid",
        description = "A GADM geographic identifier at the second level, for example AFG.1.1_1",
        schema = @Schema(implementation = String.class),
        in = ParameterIn.QUERY),
      @Parameter(
        name = "gadmLevel3Gid",
        description = "A GADM geographic identifier at the third level, for example AFG.1.1.1_1",
        schema = @Schema(implementation = String.class),
        in = ParameterIn.QUERY),
     @Parameter(
       name = "genusKey",
       description = "Genus classification key.",
       schema = @Schema(implementation = Integer.class, minimum = "0"),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "geoDistance",
       description = "Filters to match occurrence records with coordinate values within a specified distance of a coordinate, it supports units: in (inch), yd (yards), ft (feet), km (kilometers), mmi (nautical miles), mm (millimeters), cm centimeters, mi (miles), m (meters), for example /occurrence/search?geoDistance=90,100,5km",
       schema = @Schema(implementation = String.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "geometry",
       description = "Searches for occurrences inside a polygon described in Well Known Text (WKT) format. Only POINT, LINESTRING, LINEARRING, POLYGON and MULTIPOLYGON are accepted WKT types. For example, a shape written as POLYGON ((30.1 10.1, 40 40, 20 40, 10 20, 30.1 10.1)) would be queried as is, i.e. /occurrence/search?geometry=POLYGON((30.1 10.1, 40 40, 20 40, 10 20, 30.1 10.1)). _Polygons must have *anticlockwise* ordering of points, or will give unpredictable results._ (A clockwise polygon represents the opposite area: the Earth's surface with a 'hole' in it. Such queries are not supported.)",
       schema = @Schema(implementation = String.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "hasCoordinate",
       description = "Limits searches to occurrence records which contain a value in both latitude and longitude (i.e. hasCoordinate=true limits to occurrence records with coordinate values and hasCoordinate=false limits to occurrence records without coordinate values).",
       schema = @Schema(implementation = String.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "identifiedBy",
       description = "The person who provided the taxonomic identification of the occurrence.",
       schema = @Schema(implementation = String.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "identifiedByID",
       description = "Identifier (e.g. ORCID) for the person who provided the taxonomic identification of the occurrence.",
       schema = @Schema(implementation = String.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "institutionCode",
       description = "An identifier of any form assigned by the source to identify the institution the record belongs to. Not guaranteed to be unique.",
       schema = @Schema(implementation = String.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "issue",
       description = "A specific interpretation issue as defined in our OccurrenceIssue enum",
       schema = @Schema(implementation = OccurrenceIssue.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "kingdomKey",
       description = "Kingdom classification key.",
       schema = @Schema(implementation = Integer.class, minimum = "0"),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "lastInterpreted",
       description = "This date the record was last modified in GBIF, in ISO 8601 format: yyyy, yyyy-MM, yyyy-MM-dd, or MM-dd. Supports range queries. Note that this is the date the record was last changed in GBIF, not necessarily the date the record was first/last changed by the publisher. Data is re-interpreted when we change the taxonomic backbone, geographic data sources, or interpretation processes.",
       schema = @Schema(implementation = Date.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "license",
       description = "The type license applied to the dataset or record.",
       schema = @Schema(implementation = License.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "locality",
       description = "The specific description of the place.",
       schema = @Schema(implementation = String.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "mediaType",
       description = "The kind of multimedia associated with an occurrence as defined in our MediaType enum",
       schema = @Schema(implementation = MediaType.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "modified",
       description = "The most recent date-time on which the resource was changed, according to the publisher",
       schema = @Schema(implementation = Date.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "month",
       description = "The month of the year, starting with 1 for January. Supports range queries.",
       schema = @Schema(implementation = Short.class, minimum = "1", maximum = "12"),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "occurrenceId",
       description = "A single globally unique identifier for the occurrence record as provided by the publisher.",
       schema = @Schema(implementation = String.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "occurrenceStatus",
       description = "Either 'ABSENT' or 'PRESENT'; the presence or absence of the occurrence.",
       schema = @Schema(implementation = OccurrenceStatus.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "orderKey",
       description = "Order classification key.",
       schema = @Schema(implementation = Integer.class, minimum = "0"),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "organismId",
       description = "An identifier for the Organism instance (as opposed to a particular digital record of the Organism). May be a globally unique identifier or an identifier specific to the data set.",
       schema = @Schema(implementation = String.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "organismQuantity",
       description = "A number or enumeration value for the quantity of organisms.",
       schema = @Schema(implementation = String.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "organismQuantityType",
       description = "The type of quantification system used for the quantity of organisms.",
       schema = @Schema(implementation = String.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "otherCatalogNumbers",
       description = "Previous or alternate fully qualified catalog numbers.",
       schema = @Schema(implementation = String.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "phylumKey",
       description = "Phylum classification key.",
       schema = @Schema(implementation = Integer.class, minimum = "0"),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "preparations",
       description = "Preparation or preservation method for a specimen.",
       schema = @Schema(implementation = String.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "programme",
       description = "A group of activities, often associated with a specific funding stream, such as the GBIF BID programme.",
       schema = @Schema(implementation = String.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "projectId",
       description = "The identifier for a project, which is often assigned by a funded programme.",
       schema = @Schema(implementation = String.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "protocol",
       description = "Protocol or mechanism used to provide the occurrence record.",
       schema = @Schema(implementation = EndpointType.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "publishingCountry",
       description = "The 2-letter country code (as per ISO-3166-1) of the owining organization's country.",
       schema = @Schema(implementation = Country.class),
       in = ParameterIn.QUERY,
       explode = Explode.FALSE),
     @Parameter(
       name = "publishingOrg",
       description = "The publishing organization key (a UUID).",
       schema = @Schema(implementation = UUID.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "publishingOrgKey", // TODO BOTH?
       description = "The publishing organization key (a uuid).",
       schema = @Schema(implementation = UUID.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "recordedBy",
       description = "The person who recorded the occurrence.",
       schema = @Schema(implementation = String.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "recordedByID",
       description = "Identifier (e.g. ORCID) for the person who recorded the occurrence.",
       schema = @Schema(implementation = String.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "recordNumber",
       description = "An identifier given to the record at the time it was recorded in the field.",
       schema = @Schema(implementation = String.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "relativeOrganismQuantity",
       description = "The relative measurement of the quantity of the organism (i.e. without absolute units).",
       schema = @Schema(implementation = String.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "repatriated",
       description = "Searches for records whose publishing country is different to the country where the record was recorded in.",
       schema = @Schema(implementation = Boolean.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "sampleSizeUnit",
       description = "The unit of measurement of the size (time duration, length, area, or volume) of a sample in a sampling event.",
       schema = @Schema(implementation = String.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "sampleSizeValue",
       description = "A numeric value for a measurement of the size (time duration, length, area, or volume) of a sample in a sampling event.",
       schema = @Schema(implementation = Double.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "samplingProtocol",
       description = "The name of, reference to, or description of the method or protocol used during a sampling event",
       schema = @Schema(implementation = String.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "scientificName",
       description = "A scientific name from the [GBIF backbone](https://www.gbif.org/dataset/d7dddbf4-2cf0-4f39-9b2a-bb099caae36c). All included and synonym taxa are included in the search. Under the hood a call to the [species match service](https://www.gbif.org/developer/species#searching) is done first to retrieve a taxonKey. Only unique scientific names will return results, homonyms (many monomials) return nothing! Consider to use the taxonKey parameter instead and the species match service directly", // TODO link
       schema = @Schema(implementation = String.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "speciesKey",
       description = "Species classification key.",
       schema = @Schema(implementation = Integer.class, minimum = "0"),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "stateProvince",
       description = "The name of the next smaller administrative region than country (state, province, canton, department, region, etc.) in which the Location occurs.",
       schema = @Schema(implementation = String.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "subgenusKey",
       description = "Subgenus classification key.",
       schema = @Schema(implementation = Integer.class, minimum = "0"),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "taxonKey",
       description = "A taxon key from the GBIF backbone. All included and synonym taxa are included in the search, so a search for aves with taxonKey=212 (i.e. /occurrence/search?taxonKey=212) will match all birds, no matter which species.",
       schema = @Schema(implementation = Integer.class, minimum = "0"),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "typeStatus",
       description = "Nomenclatural type (type status, typified scientific name, publication) applied to the subject.",
       schema = @Schema(implementation = TypeStatus.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "verbatimScientificName",
       description = "The scientific name provided to GBIF by the data publisher, before interpretation and processing by GBIF.",
       schema = @Schema(implementation = String.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "verbatimTaxonId",
       description = "The taxon identifier provided to GBIF by the data publisher.",
       schema = @Schema(implementation = String.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "waterBody",
       description = "The name of the water body in which the Locations occurs.",
       schema = @Schema(implementation = String.class),
       in = ParameterIn.QUERY),
     @Parameter(
       name = "year",
       description = "The 4 digit year. A year of 98 will be interpreted as AD 98. Supports range queries.",
       schema = @Schema(implementation = Integer.class, minimum = "1500"),
       in = ParameterIn.QUERY),

      @Parameter(
        name = "hl",
        description =
          "Set `hl=true` to highlight terms matching the query when in fulltext search fields. The highlight will be an emphasis tag of class `gbifH1` e.g. [`/search?q=plant&hl=true`](https://api.gbif.org/v1/literature/search?q=plant&hl=true). Fulltext search fields include: title, keyword, country, publishing country, publishing organization title, hosting organization title, and description. One additional full text field is searched which includes information from metadata documents, but the text of this field is not returned in the response.",
        schema = @Schema(implementation = Boolean.class),
        in = ParameterIn.QUERY),
      @Parameter(
        name = "q",
        description =
          "Simple full text search parameter. The value for this parameter can be a simple word or a phrase. Wildcards are not supported",
        schema = @Schema(implementation = String.class),
        in = ParameterIn.QUERY),
      @Parameter(
        name = "request",
        hidden = true
      )
    })
  @CommonOffsetLimitParameters
  @CommonFacetParameters
  @ApiResponses(
    value = {
      @ApiResponse(
        responseCode = "200",
        description = "Occurrence search is valid",
        content = {
          @Content(
            mediaType = "application/json",
            schema = @Schema(implementation = SearchResponse.class)) // TODO SearchResponse
        }),
      @ApiResponse(
        responseCode = "400",
        description = "Invalid query, e.g. invalid vocabulary values",
        content = @Content)
    })
  @GetMapping
  public SearchResponse<Occurrence,OccurrenceSearchParameter> search(OccurrenceSearchRequest request) {
    LOG.debug("Executing query, parameters {}, limit {}, offset {}", request.getParameters(), request.getLimit(),
              request.getOffset());
    return searchService.search(request);
  }

  /**
   * Remove after the portal is updated, e.g. during or after December 2018.RegistryMethodSecurityConfiguration
   *
   * Old location for a GET download, doesn't make sense to be within occurrence search.
   */
  @Hidden
  @GetMapping("download")
  @Deprecated
  @Secured(USER_ROLE)
  // TODO Remove!
  public RedirectView download() {
    LOG.warn("Deprecated internal API used! (download)");
    RedirectView redirectView = new RedirectView();
    redirectView.setContextRelative(true);
    redirectView.setUrl("/occurrence/download/request");
    redirectView.setPropagateQueryParams(true);
    redirectView.setStatusCode(HttpStatus.TEMPORARY_REDIRECT);
    return redirectView;
  }

  /**
   * Remove after the portal is updated, e.g. during or after December 2018.
   *
   * Old location for a GET download predicate request, doesn't make sense to be within occurrence search.
   */
  // TODO Remove!
  @Hidden
  @GetMapping("predicate")
  @Deprecated
  public RedirectView downloadPredicate() {
    LOG.warn("Deprecated internal API used! (predicate)");
    RedirectView redirectView = new RedirectView();
    redirectView.setContextRelative(true);
    redirectView.setUrl("/occurrence/download/request/predicate");
    redirectView.setPropagateQueryParams(true);
    redirectView.setStatusCode(HttpStatus.TEMPORARY_REDIRECT);
    return redirectView;
  }

  /**
   * This same limit property is used on all the suggest methods.
   */
  @Target({PARAMETER, METHOD, FIELD, ANNOTATION_TYPE})
  @Retention(RetentionPolicy.RUNTIME)
  @Inherited
  @Parameter(
    name = "limit",
    required = true,
    description = "Controls the number of suggestions.",
    schema = @Schema(implementation = Integer.class, minimum = "1"),
    example = "5",
    in = ParameterIn.QUERY)
  @interface SuggestLimitParameter {}

  /**
   * This same query property is used on all the suggest methods.
   */
  @Target({PARAMETER, METHOD, FIELD, ANNOTATION_TYPE})
  @Retention(RetentionPolicy.RUNTIME)
  @Inherited
  @Parameter(
    name = "q",
    required = true,
    description = "Simple search suggestion parameter. Wildcards are not supported.",
    schema = @Schema(implementation = String.class),
    example = "A",
    in = ParameterIn.QUERY)
  @interface SuggestQParameter {}

  @Operation(
    operationId = "suggestCatalogNumbers",
    summary = "Suggest catalogue numbers",
    description = "Search that returns matching catalogue numbers. Results are ordered by relevance.",
    responses = @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(hidden = true))))
  @GetMapping(CATALOG_NUMBER_PATH)
  @ResponseBody
  public List<String> suggestCatalogNumbers(@RequestParam(QUERY_PARAM) @SuggestQParameter String prefix,
                                            @RequestParam(PARAM_LIMIT) @SuggestLimitParameter int limit) {
    LOG.debug("Executing catalog number suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestCatalogNumbers(prefix, limit);
  }

  @Operation(
    operationId = "suggestCollectionCodes",
    summary = "Suggest collection codes",
    description = "Search that returns matching collection codes. Results are ordered by relevance.",
    responses = @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(hidden = true))))
  @GetMapping(COLLECTION_CODE_PATH)
  @ResponseBody
  public List<String> suggestCollectionCodes(@RequestParam(QUERY_PARAM) @SuggestQParameter String prefix,
                                             @RequestParam(PARAM_LIMIT) @SuggestLimitParameter int limit) {
    LOG.debug("Executing collection codes suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestCollectionCodes(prefix, limit);
  }

  @Operation(
    operationId = "suggestRecordedBy",
    summary = "Suggest recorded by values",
    description = "Search that returns matching recorded by values. Results are ordered by relevance.",
    responses = @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(hidden = true))))
  @GetMapping(RECORDED_BY_PATH)
  @ResponseBody
  public List<String> suggestRecordedBy(@RequestParam(QUERY_PARAM) @SuggestQParameter String prefix,
                                        @RequestParam(PARAM_LIMIT) @SuggestLimitParameter int limit) {
    LOG.debug("Executing recorded_by suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestRecordedBy(prefix, limit);
  }

  @Operation(
    operationId = "suggestIdentifiedBy",
    summary = "Suggest identified by values",
    description = "Search that returns matching identified by values. Results are ordered by relevance.",
    responses = @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(hidden = true))))
  @GetMapping(IDENTIFIED_BY_PATH)
  @ResponseBody
  public List<String> suggestIdentifiedBy(@RequestParam(QUERY_PARAM) @SuggestQParameter String prefix,
                                          @RequestParam(PARAM_LIMIT) @SuggestLimitParameter int limit) {
    LOG.debug("Executing recorded_by suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestIdentifiedBy(prefix, limit);
  }

  @Operation(
    operationId = "suggestRecordNumbers",
    summary = "Suggest record numbers",
    description = "Search that returns matching record numbers. Results are ordered by relevance.",
    responses = @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(hidden = true))))
  @GetMapping(RECORD_NUMBER_PATH)
  @ResponseBody
  public List<String> suggestRecordNumbers(@RequestParam(QUERY_PARAM) @SuggestQParameter String prefix,
                                           @RequestParam(PARAM_LIMIT) @SuggestLimitParameter int limit) {
    LOG.debug("Executing record number suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestRecordNumbers(prefix, limit);
  }

  @Operation(
    operationId = "suggestInstitutionCodes",
    summary = "Suggest institution codes",
    description = "Search that returns matching institution codes. Results are ordered by relevance.",
    responses = @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(hidden = true))))
  @GetMapping(INSTITUTION_CODE_PATH)
  @ResponseBody
  public List<String> suggestInstitutionCodes(@RequestParam(QUERY_PARAM) @SuggestQParameter String prefix,
                                              @RequestParam(PARAM_LIMIT) int limit) {
    LOG.debug("Executing institution codes suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestInstitutionCodes(prefix, limit);
  }

  @Operation(
    operationId = "suggestOccurrenceIds",
    summary = "Suggest occurrence ids",
    description = "Search that returns matching occurrence ids. Results are ordered by relevance.",
    responses = @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(hidden = true))))
  @GetMapping(OCCURRENCE_ID_PATH)
  @ResponseBody
  public List<String> suggestOccurrenceIds(@RequestParam(QUERY_PARAM) @SuggestQParameter String prefix,
                                           @RequestParam(PARAM_LIMIT) @SuggestLimitParameter int limit) {
    LOG.debug("Executing occurrenceId suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestOccurrenceIds(prefix, limit);
  }

  @Operation(
    operationId = "suggestOrganismIds",
    summary = "Suggest organism ids",
    description = "Search that returns matching organism ids. Results are ordered by relevance.",
    responses = @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(hidden = true))))
  @GetMapping(ORGANISM_ID_PATH)
  @ResponseBody
  public List<String> suggestOrganismIds(@RequestParam(QUERY_PARAM) @SuggestQParameter String prefix,
                                         @RequestParam(PARAM_LIMIT) @SuggestLimitParameter int limit) {
    LOG.debug("Executing organismId suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestOrganismIds(prefix, limit);
  }

  @Operation(
    operationId = "suggestLocalities",
    summary = "Suggest locality strings",
    description = "Search that returns matching localities. Results are ordered by relevance.",
    responses = @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(hidden = true))))
  @GetMapping(LOCALITY_PATH)
  @ResponseBody
  public List<String> suggestLocalities(@RequestParam(QUERY_PARAM) @SuggestQParameter String prefix,
                                        @RequestParam(PARAM_LIMIT) @SuggestLimitParameter int limit) {
    LOG.debug("Executing locality suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestLocalities(prefix, limit);
  }

  @Operation(
    operationId = "suggestStateProvinces",
    summary = "Suggest states/provinces",
    description = "Search that returns matching states or provinces. Results are ordered by relevance.",
    responses = @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(hidden = true))))
  @GetMapping(STATE_PROVINCE_PATH)
  @ResponseBody
  public List<String> suggestStatesProvinces(@RequestParam(QUERY_PARAM) @SuggestQParameter String prefix,
                                             @RequestParam(PARAM_LIMIT) @SuggestLimitParameter int limit) {
    LOG.debug("Executing stateProvince suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestStateProvinces(prefix, limit);
  }

  @Operation(
    operationId = "suggestWaterBodies",
    summary = "Suggest water bodies",
    description = "Search that returns matching water bodies. Results are ordered by relevance.",
    responses = @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(hidden = true))))
  @GetMapping(WATER_BODY_PATH)
  @ResponseBody
  public List<String> suggestWaterBodies(@RequestParam(QUERY_PARAM) @SuggestQParameter String prefix,
                                         @RequestParam(PARAM_LIMIT) @SuggestLimitParameter int limit) {
    LOG.debug("Executing waterBody suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestWaterBodies(prefix, limit);
  }

  @Operation(
    operationId = "suggestSamplingProtocols",
    summary = "Suggest sampling protocols",
    description = "Search that returns matching sampling protocols. Results are ordered by relevance.",
    responses = @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(hidden = true))))
  @GetMapping(SAMPLING_PROTOCOL_PATH)
  @ResponseBody
  public List<String> suggestSamplingProtocols(@RequestParam(QUERY_PARAM) @SuggestQParameter String prefix,
                                               @RequestParam(PARAM_LIMIT) @SuggestLimitParameter int limit) {
    LOG.debug("Executing samplingProtocol suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestSamplingProtocol(prefix, limit);
  }

  @Operation(
    operationId = "suggestEventIds",
    summary = "Suggest event ids",
    description = "Search that returns matching event ids. Results are ordered by relevance.",
    responses = @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(hidden = true))))
  @GetMapping(EVENT_ID_PATH)
  @ResponseBody
  public List<String> suggestEventIds(@RequestParam(QUERY_PARAM) @SuggestQParameter String prefix,
                                      @RequestParam(PARAM_LIMIT) @SuggestLimitParameter int limit) {
    LOG.debug("Executing eventId suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestEventId(prefix, limit);
  }

  @Operation(
    operationId = "suggestParentEventIds",
    summary = "Suggest parent event ids",
    description = "Search that returns matching parent event ids. Results are ordered by relevance.",
    responses = @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(hidden = true))))
  @GetMapping(PARENT_EVENT_ID_PATH)
  @ResponseBody
  public List<String> suggestParentEventIds(@RequestParam(QUERY_PARAM) @SuggestQParameter String prefix,
                                           @RequestParam(PARAM_LIMIT) @SuggestLimitParameter int limit) {
    LOG.debug("Executing parentEventId suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestParentEventId(prefix, limit);
  }

  @Operation(
    operationId = "suggestDatasetNames",
    summary = "Suggest dataset names",
    description = "Search that returns matching dataset names. Results are ordered by relevance.",
    responses = @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(hidden = true))))
  @GetMapping(DATASET_NAME_PATH)
  @ResponseBody
  public List<String> suggestDatasetNames(@RequestParam(QUERY_PARAM) @SuggestQParameter String prefix,
                                          @RequestParam(PARAM_LIMIT) @SuggestLimitParameter int limit) {
    LOG.debug("Executing datasetName suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestDatasetName(prefix, limit);
  }

  @Operation(
    operationId = "suggestOtherCatalogNumbers",
    summary = "Suggest other catalogue numbers",
    description = "Search that returns matching other catalogue numbers. Results are ordered by relevance.",
    responses = @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(hidden = true))))
  @GetMapping(OTHER_CATALOG_NUMBERS_PATH)
  @ResponseBody
  public List<String> suggestOtherCatalogNumbers(@RequestParam(QUERY_PARAM) @SuggestQParameter String prefix,
                                                 @RequestParam(PARAM_LIMIT) @SuggestLimitParameter int limit) {
    LOG.debug("Executing otherCatalogNumbers suggest/search, query {}, limit {}", prefix, limit);
    return searchService.suggestOtherCatalogNumbers(prefix, limit);
  }

  @Operation(
    operationId = "suggestTerm",
    summary = "Suggest values for supported terms",
    description = "Search that returns values for supported terms. Results are ordered by relevance.",
    // TODO: what is supported?
    responses = @ApiResponse(responseCode = "200", content = @Content(schema = @Schema(hidden = true))))
  @Parameters(
    value = {
      @Parameter(
        name = "term",
        description = "A supported term",
        schema = @Schema(implementation = BasisOfRecord.class),
        in = ParameterIn.QUERY,
        example = "Continent")
    }
  )
  @GetMapping("experimental/term/{term}")
  @ResponseBody
  public List<String> searchTerm(@PathVariable("term") String term,
                                 @RequestParam(QUERY_PARAM) String query,
                                 @RequestParam(PARAM_LIMIT) @SuggestLimitParameter int limit) {
    LOG.debug("Executing term suggest/search, term {}, query {}, limit {}", term, query, limit);
    return
      VocabularyUtils.lookup(term, OccurrenceSearchParameter.class)
        .map(parameter -> searchTermService.searchFieldTerms(query, parameter, limit))
        .orElseThrow(() -> new IllegalArgumentException("Search not supported for term " +  term));
  }
}
