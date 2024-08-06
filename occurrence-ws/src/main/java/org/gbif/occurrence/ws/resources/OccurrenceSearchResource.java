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

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static org.gbif.api.model.common.paging.PagingConstants.PARAM_LIMIT;
import static org.gbif.api.model.common.search.SearchConstants.QUERY_PARAM;
import static org.gbif.ws.paths.OccurrencePaths.CATALOG_NUMBER_PATH;
import static org.gbif.ws.paths.OccurrencePaths.COLLECTION_CODE_PATH;
import static org.gbif.ws.paths.OccurrencePaths.DATASET_NAME_PATH;
import static org.gbif.ws.paths.OccurrencePaths.EVENT_ID_PATH;
import static org.gbif.ws.paths.OccurrencePaths.IDENTIFIED_BY_PATH;
import static org.gbif.ws.paths.OccurrencePaths.INSTITUTION_CODE_PATH;
import static org.gbif.ws.paths.OccurrencePaths.LOCALITY_PATH;
import static org.gbif.ws.paths.OccurrencePaths.OCCURRENCE_ID_PATH;
import static org.gbif.ws.paths.OccurrencePaths.OCC_SEARCH_PATH;
import static org.gbif.ws.paths.OccurrencePaths.ORGANISM_ID_PATH;
import static org.gbif.ws.paths.OccurrencePaths.OTHER_CATALOG_NUMBERS_PATH;
import static org.gbif.ws.paths.OccurrencePaths.PARENT_EVENT_ID_PATH;
import static org.gbif.ws.paths.OccurrencePaths.RECORDED_BY_PATH;
import static org.gbif.ws.paths.OccurrencePaths.RECORD_NUMBER_PATH;
import static org.gbif.ws.paths.OccurrencePaths.SAMPLING_PROTOCOL_PATH;
import static org.gbif.ws.paths.OccurrencePaths.STATE_PROVINCE_PATH;
import static org.gbif.ws.paths.OccurrencePaths.WATER_BODY_PATH;

import io.swagger.v3.oas.annotations.Hidden;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.Explode;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.extensions.Extension;
import io.swagger.v3.oas.annotations.extensions.ExtensionProperty;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.SneakyThrows;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.search.OccurrencePredicateSearchRequest;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchRequest;
import org.gbif.api.service.occurrence.OccurrenceSearchService;
import org.gbif.api.util.Range;
import org.gbif.api.util.VocabularyUtils;
import org.gbif.api.vocabulary.BasisOfRecord;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.GbifRegion;
import org.gbif.api.vocabulary.License;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.api.vocabulary.OccurrenceStatus;
import org.gbif.api.vocabulary.Sex;
import org.gbif.api.vocabulary.TaxonomicStatus;
import org.gbif.api.vocabulary.ThreatStatus;
import org.gbif.api.vocabulary.TypeStatus;
import org.gbif.occurrence.search.SearchTermService;
import org.gbif.occurrence.search.es.EsSearchRequestBuilder;
import org.gbif.occurrence.search.es.OccurrenceEsField;
import org.gbif.vocabulary.client.ConceptClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * Occurrence resource.
 */
@Tag(
  name = "Searching occurrences",
  description = "This API provides services for searching occurrence records that have been indexed by GBIF.\n\n" +
  "In order to retrieve all results for a given search filter you need to issue individual requests for each page, which is limited to " +
  "a maximum size of 300 records per page. Note that for technical reasons we also have a hard limit for any query of 100,000 records. " +
  "You will get an error if the offset + limit exceeds 100,000. To retrieve all records beyond 100,000 you should use our asynchronous " +
  "download service (below) instead.",
  extensions = @Extension(name = "Order", properties = @ExtensionProperty(name = "Order", value = "0200"))
)
@RestController
@RequestMapping(
  value = OCC_SEARCH_PATH,
  produces = {MediaType.APPLICATION_JSON_VALUE, "application/x-javascript"}
)
public class OccurrenceSearchResource {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceSearchResource.class);

  private final OccurrenceSearchService searchService;

  private final SearchTermService searchTermService;

  private final EsSearchRequestBuilder esSearchRequestBuilder;

  @Autowired
  public OccurrenceSearchResource(
      OccurrenceSearchService searchService,
      SearchTermService searchTermService,
      ConceptClient conceptClient) {
    this.searchService = searchService;
    this.searchTermService = searchTermService;
    this.esSearchRequestBuilder =
        new EsSearchRequestBuilder(OccurrenceEsField.buildFieldMapper(), conceptClient);
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
        description = "Controls the number of results in the page. Using too high a value will be overwritten with the maximum threshold, which is 300 for this service. Sensible defaults are used so this may be omitted.",
        schema = @Schema(implementation = Integer.class, minimum = "0"),
        in = ParameterIn.QUERY),
      @Parameter(
        name = "offset",
        description = "Determines the offset for the search results. A limit of 20 and offset of 40 will get the third page of 20 results. This service has a maximum offset of 100,000.",
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
        description = "A facet name used to retrieve the most frequent values for a field. Facets are allowed for " +
          "all search parameters except geometry and geoDistance. This parameter may by repeated to request multiple " +
          "facets, as in [this example](https://api.gbif.org/v1/occurrence/search?facet=datasetKey&facet=basisOfRecord&limit=0).\n\n" +
          "Note terms not available for searching are not available for faceting.",
        schema = @Schema(implementation = String.class),
        in = ParameterIn.QUERY),
      @Parameter(
        name = "facetMincount",
        description = "Used in combination with the facet parameter. Set facetMincount={#} to exclude facets with a " +
          "count less than {#}, e.g. [/search?facet=basisOfRecord&limit=0&facetMincount=10000](https://api.gbif.org/v1/occurrence/search?facet=basisOfRecord&limit=0&facetMincount=1000000].",
        schema = @Schema(implementation = Integer.class),
        in = ParameterIn.QUERY),
      @Parameter(
        name = "facetMultiselect",
        description = "Used in combination with the facet parameter. Set facetMultiselect=true to still return counts " +
          "for values that are not currently filtered, e.g. [/search?facet=basisOfRecord&limit=0&basisOfRecord=HUMAN_OBSERVATION&facetMultiselect=true]" +
          "(https://api.gbif.org/v1/occurrence/search?facet=basisOfRecord&limit=0&basisOfRecord=HUMAN_OBSERVATION&facetMultiselect=true) " +
          "still shows Basis of Record values 'PRESERVED_SPECIMEN' and so on, even though Basis of Record is being filtered.",
        schema = @Schema(implementation = Boolean.class),
        in = ParameterIn.QUERY),
      @Parameter(
        name = "facetLimit",
        description = "Facet parameters allow paging requests using the parameters facetOffset and facetLimit",
        schema = @Schema(implementation = Integer.class),
        in = ParameterIn.QUERY),
      @Parameter(
        name = "facetOffset",
        description = "Facet parameters allow paging requests using the parameters facetOffset and facetLimit",
        schema = @Schema(implementation = Integer.class, minimum = "0"),
        in = ParameterIn.QUERY)
    }
  )
  @interface CommonFacetParameters {}

  public static final String API_PARAMETER_MAY_BE_REPEATED = "*Parameter may be repeated.*";
  public static final String API_PARAMETER_RANGE_QUERIES = "*Supports range queries.*";
  public static final String API_PARAMETER_RANGE_OR_REPEAT = "*Parameter may be repeated or a range.*";

  @Operation(
    operationId = "searchOccurrence",
    summary = "Occurrence search",
    description = "Full search across all occurrences.",
    extensions = @Extension(name = "Order", properties = @ExtensionProperty(name = "Order", value = "0000")))
  @Parameters(
    value = {
      @Parameter(
        name = "acceptedTaxonKey",
        description = "A taxon key from the GBIF backbone. Only synonym taxa are included in the search, so a search for Aves with acceptedTaxonKey=212 (i.e. [/occurrence/search?taxonKey=212](https://api.gbif.org/v1/occurrence/search?acceptedTaxonKey=212)) will match occurrences identified as birds, but not any known family, genus or species of bird." +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = Integer.class, minimum = "0")),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "2476674"),
      @Parameter(
        name = "associatedSequences",
        description = "Identifier (publication, global unique identifier, URI) of genetic sequence " +
          "information associated with the material entity.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "http://www.ncbi.nlm.nih.gov/nuccore/U34853.1"),
      @Parameter(
        name = "basisOfRecord",
        description = "Basis of record, as defined in our BasisOfRecord vocabulary.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = BasisOfRecord.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "PRESERVED_SPECIMEN"),
      @Parameter(
        name = "bed",
        description = "The full name of the lithostratigraphic bed from which the material entity was collected.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "Harlem coal"),
      @Parameter(
        name = "catalogNumber",
        description = "An identifier of any form assigned by the source within a physical collection or digital " +
          "dataset for the record which may not be unique, but should be fairly unique in combination with the " +
          "institution and collection code.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "K001275042"),
      @Parameter(
        name = "classKey",
        description = "Class classification key.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = Integer.class, minimum = "0")),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "212"),
      @Parameter(
        name = "collectionCode",
        description = "An identifier of any form assigned by the source to identify the physical collection or digital " +
          "dataset uniquely within the context of an institution.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "F"),
      @Parameter(
        name = "collectionKey",
        description = "A key (UUID) for a collection registered in the [Global Registry of Scientific Collections]" +
          "(https://www.gbif.org/grscicoll).\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = UUID.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "dceb8d52-094c-4c2c-8960-75e0097c6861"),
      @Parameter(
        name = "continent",
        description = "Continent, as defined in our Continent vocabulary.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = Continent.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "EUROPE"),
      @Parameter(
        name = "coordinateUncertaintyInMeters",
        description = "The horizontal distance (in metres) from the given decimalLatitude and decimalLongitude " +
          "describing the smallest circle containing the whole of the Location.\n\n" +
          API_PARAMETER_RANGE_QUERIES,
        schema = @Schema(implementation = Range.class),
        in = ParameterIn.QUERY,
        example = "0,500"),
      @Parameter(
        name = "country",
        description = "The 2-letter country code (as per ISO-3166-1) of the country in which the occurrence " +
          "was recorded.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = Country.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "AF"),
      @Parameter(
        name = "crawlId",
        description = "Crawl attempt that harvested this record.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = Integer.class)),
        in = ParameterIn.QUERY,
        example = "1"),
      @Parameter(
        name = "datasetId",
        description = "The ID of the dataset.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "https://doi.org/10.1594/PANGAEA.315492"),
      @Parameter(
        name = "datasetKey",
        description = "The occurrence dataset key (a UUID).\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = UUID.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "13b70480-bd69-11dd-b15f-b8a03c50a862"),
      @Parameter(
        name = "datasetName",
        description = "The exact name of the dataset.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY),
      @Parameter(
        name = "decimalLatitude",
        description = "Latitude in decimal degrees between -90° and 90° based on WGS 84.\n\n" +
          API_PARAMETER_RANGE_QUERIES,
        schema = @Schema(implementation = Range.class),
        in = ParameterIn.QUERY,
        example = "40.5,45"),
      @Parameter(
        name = "degreeOfEstablishment",
        description = "The degree to which an organism survives, reproduces and expands its range at the given " +
          "place and time, as defined in the [GBIF DegreeOfEstablishment vocabulary](https://registry.gbif.org/vocabulary/DegreeOfEstablishment/concepts).\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "Invasive"),
      @Parameter(
        name = "decimalLongitude",
        description = "Longitude in decimals between -180 and 180 based on WGS 84.\n\n" +
          API_PARAMETER_RANGE_QUERIES,
        schema = @Schema(implementation = Range.class),
        in = ParameterIn.QUERY,
        example = "-120,-95.5"),
      @Parameter(
        name = "depth",
        description = "Depth in metres relative to altitude. For example 10 metres below a lake surface with " +
          "given altitude.\n\n" +
          API_PARAMETER_RANGE_OR_REPEAT,
        schema = @Schema(implementation = Range.class),
        in = ParameterIn.QUERY,
        example = "10,20"),
      @Parameter(
        name = "distanceFromCentroidInMeters",
        description = "The horizontal distance (in metres) of the occurrence from the nearest centroid known to be used " +
          "in automated georeferencing procedures, if that distance is 5000m or less.  Occurrences (especially specimens) " +
          "near a country centroid may have a poor-quality georeference, especially if coordinateUncertaintyInMeters is " +
          "blank or large.\n\n" +
          API_PARAMETER_RANGE_QUERIES,
        schema = @Schema(implementation = Range.class),
        in = ParameterIn.QUERY,
        example = "0,500"),
      @Parameter(
        name = "dwcaExtension",
        description = "A known Darwin Core Archive extension RowType.  Limits the search to occurrences which have " +
          "this extension, although they will not necessarily have any useful data recorded using the extension.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "http://rs.tdwg.org/ac/terms/Multimedia"),
      @Parameter(
        name = "earliestEonOrLowestEonothem",
        description = "The full name of the earliest possible geochronologic era or lowest chronostratigraphic " +
          "erathem attributable to the stratigraphic horizon from which the material entity was collected" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "Mesozoic"),
      @Parameter(
        name = "earliestEraOrLowestErathem",
        description = "The full name of the latest possible geochronologic eon or highest chrono-stratigraphic " +
          "eonothem or the informal name (\"Precambrian\") attributable to the stratigraphic horizon " +
          "from which the material entity was collected" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "Proterozoic"),
      @Parameter(
        name = "earliestPeriodOrLowestSystem",
        description = "The full name of the earliest possible geochronologic period or lowest chronostratigraphic " +
          "system attributable to the stratigraphic horizon from which the material entity was collected" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "Neogene"),
      @Parameter(
        name = "earliestEpochOrLowestSeries",
        description = "The full name of the earliest possible geochronologic epoch or lowest chronostratigraphic " +
          "series attributable to the stratigraphic horizon from which the material entity was collected" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "Holocene"),
      @Parameter(
        name = "earliestAgeOrLowestStage",
        description = "The full name of the earliest possible geochronologic age or lowest chronostratigraphic " +
          "stage attributable to the stratigraphic horizon from which the material entity was collected" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "Skullrockian"),
      @Parameter(
        name = "elevation",
        description = "Elevation (altitude) in metres above sea level.\n\n" +
          API_PARAMETER_RANGE_OR_REPEAT,
        schema = @Schema(implementation = Range.class),
        in = ParameterIn.QUERY,
        example = "1000,1250"),
      @Parameter(
        name = "endDayOfYear",
        description = "The latest integer day of the year on which the event occurred.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true,  schema = @Schema(implementation = Short.class, minimum = "1", maximum = "366")),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "6"),
      @Parameter(
        name = "establishmentMeans",
        description = "Whether an organism or organisms have been introduced to a given place and time through " +
          "the direct or indirect activity of modern humans, as defined in the " +
          "[GBIF EstablishmentMeans vocabulary](https://registry.gbif.org/vocabulary/EstablishmentMeans/concepts).\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "Native"),
      @Parameter(
        name = "eventDate",
        description = "Occurrence date in ISO 8601 format: yyyy, yyyy-MM or yyyy-MM-dd.\n\n" +
          API_PARAMETER_RANGE_OR_REPEAT,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = Date.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "2000,2001-06-30"),
      @Parameter(
        name = "eventDateGte",
        hidden = true, // https://github.com/gbif/occurrence/issues/346
        description = "Occurrence date in ISO 8601 format: yyyy, yyyy-MM or yyyy-MM-dd.\n\n" +
          API_PARAMETER_RANGE_OR_REPEAT,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = Date.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "2000,2001-06-30"),
      @Parameter(
        name = "eventId",
        description = "An identifier for the information associated with a sampling event.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "A 123"),
      @Parameter(
        name = "familyKey",
        description = "Family classification key.",
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = Integer.class, minimum = "0")),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "2405"),
      @Parameter(
        name = "fieldNumber",
        description = "An identifier given to the event in the field. Often serves as a link between field notes and the event.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "RV Sol 87-03-08"),
      @Parameter(
        name = "formation",
        description = "The full name of the lithostratigraphic formation from which the material entity was collected.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "Notch Peak Formation"),
      @Parameter(
        name = "gadmGid",
        description = "A GADM geographic identifier at any level, for example AGO, AGO.1_1, AGO.1.1_1 or AGO.1.1.1_1\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "AGO.1_1"),
      @Parameter(
        name = "gadmLevel0Gid",
        description = "A GADM geographic identifier at the zero level, for example AGO.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "AGO"),
      @Parameter(
        name = "gadmLevel1Gid",
        description = "A GADM geographic identifier at the first level, for example AGO.1_1.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "AGO.1_1"),
      @Parameter(
        name = "gadmLevel2Gid",
        description = "A GADM geographic identifier at the second level, for example AFG.1.1_1.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "AFG.1.1_1"),
      @Parameter(
        name = "gadmLevel3Gid",
        description = "A GADM geographic identifier at the third level, for example AFG.1.1.1_1.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "AFG.1.1.1_1"),
      @Parameter(
        name = "gbifId",
        description = "The unique GBIF key for a single occurrence.",
        schema = @Schema(implementation = Long.class),
        in = ParameterIn.QUERY,
        example = "2005380410"),
      @Parameter(
        name = "gbifRegion",
        description = "Gbif region based on country code.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = GbifRegion.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "AFRICA"),
      @Parameter(
        name = "genusKey",
        description = "Genus classification key.",
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = Integer.class, minimum = "0")),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "2877951"),
      @Parameter(
        name = "geoDistance",
        description = "Filters to match occurrence records with coordinate values within a specified distance of " +
          "a coordinate.\n\nDistance may be specified in kilometres (km) or metres (m).",
        schema = @Schema(implementation = String.class),
        in = ParameterIn.QUERY,
        example = "90,100,5km"),
      @Parameter(
        name = "georeferencedBy",
        description = "Name of a person, group, or organization who determined the georeference (spatial representation) for the location.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "Brad Millen"),
      @Parameter(
        name = "geometry",
        description = "Searches for occurrences inside a polygon described in Well Known Text (WKT) format. " +
          "Only `POLYGON` and `MULTIPOLYGON` are accepted WKT types.\n\n" +
          "For example, a shape written as `POLYGON ((30.1 10.1, 40 40, 20 40, 10 20, 30.1 10.1))` would be queried " +
          "as is.\n\n" +
          "_Polygons must have *anticlockwise* ordering of points._ " +
          "(A clockwise polygon represents the opposite area: the Earth's surface with a 'hole' in it. " +
          "Such queries are not supported.)\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "POLYGON ((30.1 10.1, 40 40, 20 40, 10 20, 30.1 10.1))"),
      @Parameter(
        name = "group",
        description = "The full name of the lithostratigraphic group from which the material entity was collected.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "Bathurst"),
      @Parameter(
        name = "hasCoordinate",
        description = "Limits searches to occurrence records which contain a value in both latitude and " +
          "longitude (i.e. `hasCoordinate=true` limits to occurrence records with coordinate values and " +
          "`hasCoordinate=false` limits to occurrence records without coordinate values).",
        schema = @Schema(implementation = Boolean.class),
        in = ParameterIn.QUERY,
        example = "true"),
      @Parameter(
        name = "higherGeography",
        description = "Geographic name less specific than the information captured in the locality term.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "Argentina"),
      @Parameter(
        name = "highestBiostratigraphicZone",
        description = "The full name of the highest possible geological biostratigraphic zone of the stratigraphic " +
          "horizon from which the material entity was collected.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "Blancan"),
      @Parameter(
        name = "hasGeospatialIssue",
        description = "Includes/excludes occurrence records which contain spatial issues (as determined in our " +
          "record interpretation), i.e. hasGeospatialIssue=true returns only those records with spatial issues " +
          "while hasGeospatialIssue=false includes only records without spatial issues.\n\n" +
          "The absence of this parameter returns any record with or without spatial issues.",
        schema = @Schema(implementation = Boolean.class),
        in = ParameterIn.QUERY,
        example = "true"),
      @Parameter(
        name = "hostingOrganizationKey",
        description = "The key (UUID) of the publishing organization whose installation (server) hosts the original " +
          "dataset.\n\n" +
          "(This is of little interest to most data users.)\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = UUID.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "fbca90e3-8aed-48b1-84e3-369afbd000ce"),
      @Parameter(
        name = "identifiedBy",
        description = "The person who provided the taxonomic identification of the occurrence.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "Allison"),
      @Parameter(
        name = "identifiedByID",
        description = "Identifier (e.g. ORCID) for the person who provided the taxonomic identification of the occurrence.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "https://orcid.org/0000-0001-6492-4016"),
      @Parameter(
        name = "installationKey",
        description = "The occurrence installation key (a UUID).\n\n" +
          "(This is of little interest to most data users.  It is the identifier for the server that provided the data " +
          "to GBIF.)\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = UUID.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "17a83780-3060-4851-9d6f-029d5fcb81c9"),
      @Parameter(
        name = "institutionCode",
        description = "An identifier of any form assigned by the source to identify the institution the " +
          "record belongs to. Not guaranteed to be unique.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "K"),
      @Parameter(
        name = "institutionKey",
        description = "A key (UUID) for an institution registered in the [Global Registry of Scientific Collections]" +
          "(https://www.gbif.org/grscicoll).\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = UUID.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "fa252605-26f6-426c-9892-94d071c2c77f"),
      @Parameter(
        name = "issue",
        description = "A specific interpretation issue as defined in our OccurrenceIssue enumeration.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = OccurrenceIssue.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "COUNTRY_COORDINATE_MISMATCH"),
      @Parameter(
        name = "isInCluster",
        description = "*Experimental.* Searches for records which are part of a cluster.  See the documentation on " +
          "[clustering](/en/data-processing/clustering-occurrences).",
        schema = @Schema(implementation = Boolean.class),
        in = ParameterIn.QUERY,
        example = "true"),
      @Parameter(
        name = "island",
        description = "The name of the island on or near which the location occurs.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "Zanzibar"),
      @Parameter(
        name = "islandGroup",
        description = "The name of the island group in which the location occurs.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "Seychelles"),
      @Parameter(
        name = "isSequenced",
        description = "Flag occurrence when associated sequences exists",
        schema = @Schema(implementation = Boolean.class),
        in = ParameterIn.QUERY,
        example = "true"),
      @Parameter(
        name = "iucnRedListCategory",
        description = "A threat status category from the IUCN Red List.  The two-letter code for the status should " +
          "be used.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = ThreatStatus.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "EX"),
      @Parameter(
        name = "kingdomKey",
        description = "Kingdom classification key.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = Integer.class, minimum = "0")),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "5"),
      @Parameter(
        name = "lastInterpreted",
        description = "This date the record was last modified in GBIF, in ISO 8601 format: yyyy, yyyy-MM, yyyy-MM-dd, " +
          "or MM-dd.\n\n" +
          "Note that this is the date the record was last changed in GBIF, not necessarily the date the record was " +
          "first/last changed by the publisher. Data is re-interpreted when we change the taxonomic backbone, " +
          "geographic data sources, or interpretation processes.\n\n" +
          API_PARAMETER_RANGE_OR_REPEAT,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = Date.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "2023-02"),
      @Parameter(
        name = "latestEonOrHighestEonothem",
        description = "The full name of the latest possible geochronologic eon or highest chrono-stratigraphic " +
          "eonothem or the informal name (\"Precambrian\") attributable to the stratigraphic horizon " +
          "from which the material entity was collected" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "Proterozoic"),
      @Parameter(
        name = "latestEraOrHighestErathem",
        description = "The full name of the latest possible geochronologic era or highest chronostratigraphic " +
          "erathem attributable to the stratigraphic horizon from which the material entity was collected" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "Cenozoic"),
      @Parameter(
        name = "latestPeriodOrHighestSystem",
        description = "The full name of the latest possible geochronologic period or highest chronostratigraphic " +
          "system attributable to the stratigraphic horizon from which the material entity was collected" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "Neogene"),
      @Parameter(
        name = "latestEpochOrHighestSeries",
        description = "The full name of the latest possible geochronologic epoch or highest chronostratigraphic " +
          "series attributable to the stratigraphic horizon from which the material entity was collected" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "Pleistocene"),
      @Parameter(
        name = "latestAgeOrHighestStage",
        description = "The full name of the latest possible geochronologic age or highest chronostratigraphic " +
          "stage attributable to the stratigraphic horizon from which the material entity was collected." +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "Boreal"),
      @Parameter(
        name = "license",
        description = "The licence applied to the dataset or record by the publisher.\n\n" +
          API_PARAMETER_RANGE_OR_REPEAT,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = License.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "CC0_1_0"),
      @Parameter(
        name = "lifeStage",
        description = "The age class or life stage of an organism at the time the occurrence was recorded, as defined " +
          "in the GBIF LifeStage vocabulary](https://registry.gbif.org/vocabulary/LifeStage/concepts).\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "Juvenile"),
      @Parameter(
        name = "locality",
        description = "The specific description of the place.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY),
      @Parameter(
        name = "lowestBiostratigraphicZone",
        description = "The full name of the lowest possible geological biostratigraphic zone of the " +
          "stratigraphic horizon from which the material entity was collected.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "Maastrichtian"),
      @Parameter(
        name = "mediaType",
        description = "The kind of multimedia associated with an occurrence as defined in our MediaType enumeration.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = MediaType.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY),
      @Parameter(
        name = "member",
        description = "The full name of the lithostratigraphic member from which the material entity was collected.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "Lava Dam Member"),
      @Parameter(
        name = "modified",
        description = "The most recent date-time on which the occurrnce was changed, according to the publisher.\n\n" +
          API_PARAMETER_RANGE_OR_REPEAT,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = Date.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "2023-02-20"),
      @Parameter(
        name = "month",
        description = "The month of the year, starting with 1 for January.\n\n" +
          API_PARAMETER_RANGE_OR_REPEAT,
        array = @ArraySchema(uniqueItems = true,  schema = @Schema(implementation = Short.class, minimum = "1", maximum = "12")),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "5"),
       @Parameter(
         name = "networkKey",
         description = "The network's GBIF key (a UUID).\n\n" +
           API_PARAMETER_MAY_BE_REPEATED,
         array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = UUID.class)),
         explode = Explode.TRUE,
         in = ParameterIn.QUERY,
         example = "2b7c7b4f-4d4f-40d3-94de-c28b6fa054a6"),
      @Parameter(
        name = "occurrenceId",
        description = "A globally unique identifier for the occurrence record as provided by the publisher.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "URN:catalog:UWBM:Bird:126493"),
      @Parameter(
        name = "occurrenceStatus",
        description = "Either `ABSENT` or `PRESENT`; the presence or absence of the occurrence.",
        schema = @Schema(implementation = OccurrenceStatus.class),
        in = ParameterIn.QUERY,
        example = "PRESENT"),
      @Parameter(
        name = "orderKey",
        description = "Order classification key.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = Integer.class, minimum = "0")),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "1448"),
      @Parameter(
        name = "organismId",
        description = "An identifier for the organism instance (as opposed to a particular digital record " +
          "of the organism). May be a globally unique identifier or an identifier specific to the data set.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY),
      @Parameter(
        name = "organismQuantity",
        description = "A number or enumeration value for the quantity of organisms.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "1"),
      @Parameter(
        name = "organismQuantityType",
        description = "The type of quantification system used for the quantity of organisms.\n\n" +
          "*Note this term is not aligned to a vocabulary.*\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "individuals"),
      @Parameter(
        name = "otherCatalogNumbers",
        description = "Previous or alternate fully qualified catalog numbers.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY),
      @Parameter(
        name = "parentEventId",
        description = "An identifier for the information associated with a sampling event.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "A 123"),
      @Parameter(
        name = "pathway",
        description = "The process by which an organism came to be in a given place at a given time, as defined in the " +
          "[GBIF Pathway vocabulary](https://registry.gbif.org/vocabulary/Pathway/concepts).\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "Agriculture"),
      @Parameter(
        name = "phylumKey",
        description = "Phylum classification key.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = Integer.class, minimum = "0")),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "44"),
      @Parameter(
        name = "preparations",
        description = "Preparation or preservation method for a specimen.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "pinned"),
      @Parameter(
        name = "previousIdentifications",
        description = "Previous assignment of name to the organism.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "Chalepidae"),
      @Parameter(
        name = "programme",
        description = "A group of activities, often associated with a specific funding stream, such as the " +
          "GBIF BID programme.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "BID"),
      @Parameter(
        name = "projectId",
        description = "The identifier for a project, which is often assigned by a funded programme.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "bid-af2020-039-reg"),
      @Parameter(
        name = "protocol",
        description = "Protocol or mechanism used to provide the occurrence record.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = EndpointType.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "DWC_ARCHIVE"),
      @Parameter(
        name = "publishingCountry",
        description = "The 2-letter country code (as per ISO-3166-1) of the owning organization's country.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = Country.class)),
        explode = Explode.TRUE,
        example = "AD"),
      @Parameter(
        name = "publishedByGbifRegion",
        description = "GBIF region based on the owning organization's country.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = GbifRegion.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "AFRICA"),
      @Parameter(
        name = "publishingOrg",
        description = "The publishing organization's GBIF key (a UUID).\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = UUID.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "e2e717bf-551a-4917-bdc9-4fa0f342c530"),
      @Parameter(
        name = "recordedBy",
        description = "The person who recorded the occurrence.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "MiljoStyrelsen"),
      @Parameter(
        name = "recordedByID",
        description = "Identifier (e.g. ORCID) for the person who recorded the occurrence.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "https://orcid.org/0000-0003-0623-6682"),
      @Parameter(
        name = "recordNumber",
        description = "An identifier given to the record at the time it was recorded in the field.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "1"),
      @Parameter(
        name = "relativeOrganismQuantity",
        description = "The relative measurement of the quantity of the organism (i.e. without absolute units).\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY),
      @Parameter(
        name = "repatriated",
        description = "Searches for records whose publishing country is different to the country in which the " +
          "record was recorded.",
        schema = @Schema(implementation = Boolean.class),
        in = ParameterIn.QUERY,
        example = "true"),
      @Parameter(
        name = "sampleSizeUnit",
        description = "The unit of measurement of the size (time duration, length, area, or volume) of a sample in a " +
          "sampling event.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "hectares"),
      @Parameter(
        name = "sampleSizeValue",
        description = "A numeric value for a measurement of the size (time duration, length, area, or volume) of a " +
          "sample in a sampling event.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = Double.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "50.50"),
      @Parameter(
        name = "samplingProtocol",
        description = "The name of, reference to, or description of the method or protocol used during a sampling event.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "malaise trap"),
      @Parameter(
        name = "sex",
        description = "The sex of the biological individual(s) represented in the occurrence.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = Sex.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "MALE"),
      @Parameter(
        name = "scientificName",
        description = "A scientific name from the [GBIF backbone](https://www.gbif.org/dataset/d7dddbf4-2cf0-4f39-9b2a-bb099caae36c). " +
          "All included and synonym taxa are included in the search.\n\n" +
          "Under the hood a call to the [species match service](https://www.gbif.org/developer/species#searching) " +
          "is done first to retrieve a taxonKey. Only unique scientific names will return results, homonyms " +
          "(many monomials) return nothing! Consider to use the taxonKey parameter instead and the species match " +
          "service directly.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "Quercus robur"),
      @Parameter(
        name = "speciesKey",
        description = "Species classification key." +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = Integer.class, minimum = "0")),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "2476674"),
      @Parameter(
        name = "startDayOfYear",
        description = "The earliest integer day of the year on which the event occurred.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true,  schema = @Schema(implementation = Short.class, minimum = "1", maximum = "366")),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "5"),
      @Parameter(
        name = "stateProvince",
        description = "The name of the next smaller administrative region than country (state, province, canton, " +
          "department, region, etc.) in which the Location occurs.\n\n" +
          "This term does not have any data quality checks; see also the GADM parameters.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "Leicestershire"),
      @Parameter(
        name = "subgenusKey",
        hidden = true, // Not yet implemented
        description = "Subgenus classification key.",
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = Integer.class, minimum = "0")),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "0"),
      @Parameter(
        name = "taxonConceptId",
        description = "An identifier for the taxonomic concept to which the record refers - not for the nomenclatural details of a taxon.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "8fa58e08-08de-4ac1-b69c-1235340b7001"),
      @Parameter(
        name = "taxonKey",
        description = "A taxon key from the GBIF backbone. All included (child) and synonym taxa are included in the search, so a search for Aves with taxonKey=212 (i.e. [/occurrence/search?taxonKey=212](https://api.gbif.org/v1/occurrence/search?taxonKey=212)) will match all birds, no matter which species." +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = Integer.class, minimum = "0")),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "2476674"),
      @Parameter(
        name = "taxonId",
        description = "The taxon identifier provided to GBIF by the data publisher.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "urn:lsid:dyntaxa.se:Taxon:103026"),
      @Parameter(
        name = "taxonomicStatus",
        description = "A taxonomic status from our TaxonomicStatus enumeration.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = TaxonomicStatus.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "SYNONYM"),
      @Parameter(
        name = "typeStatus",
        description = "Nomenclatural type (type status, typified scientific name, publication) applied to the subject.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = TypeStatus.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "HOLOTYPE"),
      @Parameter(
        name = "verbatimScientificName",
        description = "The scientific name provided to GBIF by the data publisher, before interpretation and " +
          "processing by GBIF.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "Quercus robur L."),
      @Parameter(
        name = "waterBody",
        description = "The name of the water body in which the Locations occurs.\n\n" +
          API_PARAMETER_MAY_BE_REPEATED,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "Lake Michigan"),
      @Parameter(
        name = "year",
        description = "The 4 digit year. A year of 98 will be interpreted as AD 98.\n\n" +
          API_PARAMETER_RANGE_OR_REPEAT,
        array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = Integer.class)),
        explode = Explode.TRUE,
        in = ParameterIn.QUERY,
        example = "1998"),

      @Parameter(
        name = "hl",
        description =
          "Set `hl=true` to highlight terms matching the query when in full-text search fields. The highlight will " +
            "be an emphasis tag of class `gbifH1` e.g. " +
            "[`/search?q=plant&hl=true`](https://api.gbif.org/v1/literature/search?q=plant&hl=true).\n\n" +
            "Full-text search fields include: title, keyword, country, publishing country, publishing organization " +
            "title, hosting organization title, and description. One additional full text field is searched which " +
            "includes information from metadata documents, but the text of this field is not returned in the response.",
        schema = @Schema(implementation = Boolean.class),
        in = ParameterIn.QUERY,
        example = "true"),
      @Parameter(
        name = "q",
        description =
          "Simple full-text search parameter. The value for this parameter can be a simple word or a phrase. " +
            "Wildcards are not supported",
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
        description = "Occurrence search is valid"
      ),
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
   * A search request using POST rather than GET.
   */
  @Hidden
  @PostMapping
  public SearchResponse<Occurrence,OccurrenceSearchParameter> postSearch(@NotNull @Valid @RequestBody OccurrenceSearchRequest request) {
    LOG.debug("Executing post query, parameters {}, limit {}, offset {}", request.getParameters(), request.getLimit(),
      request.getOffset());
    return searchService.search(request);
  }

  /**
   * A search request using predicate parameters.
   */
  @Operation(
    operationId = "predicateSearchOccurrence",
    summary = "Occurrence search using predicates",
    description = "Full search across all occurrences specified using predicates (as used for the download API).",
    extensions = @Extension(name = "Order", properties = @ExtensionProperty(name = "Order", value = "0100")))
  @Parameter(name = "request", hidden = true)
  @CommonOffsetLimitParameters
  @CommonFacetParameters
  @ApiResponses(
    value = {
      @ApiResponse(
        responseCode = "200",
        description = "Occurrence search is valid"
      ),
      @ApiResponse(
        responseCode = "400",
        description = "Invalid query, e.g. invalid vocabulary values",
        content = @Content)
    })
  @PostMapping("predicate")
  public SearchResponse<Occurrence,OccurrenceSearchParameter> search(@NotNull @Valid @RequestBody OccurrencePredicateSearchRequest request) {
    LOG.debug("Executing query, predicate {}, limit {}, offset {}", request.getPredicate(), request.getLimit(),
      request.getOffset());
    return searchService.search(request);
  }

  /**
   * Convert a predicate query to the query used by ElasticSearch, intended for use by internal systems like the portal.
   */
  @Hidden
  @PostMapping("predicate/toesquery")
  @SneakyThrows
  public String toEsQuery(@NotNull @Valid @RequestBody OccurrencePredicateSearchRequest request) {
    return esSearchRequestBuilder.buildQuery(request).map(AbstractQueryBuilder::toString).orElseThrow(() -> new IllegalArgumentException("Request can't be translated"));
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
    responses = @ApiResponse(responseCode = "200"),
    extensions = @Extension(name = "Order", properties = @ExtensionProperty(name = "Order", value = "1000")))
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
    responses = @ApiResponse(responseCode = "200"),
    extensions = @Extension(name = "Order", properties = @ExtensionProperty(name = "Order", value = "1000")))
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
    responses = @ApiResponse(responseCode = "200"),
    extensions = @Extension(name = "Order", properties = @ExtensionProperty(name = "Order", value = "1000")))
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
    responses = @ApiResponse(responseCode = "200"),
    extensions = @Extension(name = "Order", properties = @ExtensionProperty(name = "Order", value = "1000")))
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
    responses = @ApiResponse(responseCode = "200"),
    extensions = @Extension(name = "Order", properties = @ExtensionProperty(name = "Order", value = "1000")))
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
    responses = @ApiResponse(responseCode = "200"),
    extensions = @Extension(name = "Order", properties = @ExtensionProperty(name = "Order", value = "1000")))
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
    responses = @ApiResponse(responseCode = "200"),
    extensions = @Extension(name = "Order", properties = @ExtensionProperty(name = "Order", value = "1000")))
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
    responses = @ApiResponse(responseCode = "200"),
    extensions = @Extension(name = "Order", properties = @ExtensionProperty(name = "Order", value = "1000")))
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
    responses = @ApiResponse(responseCode = "200"),
    extensions = @Extension(name = "Order", properties = @ExtensionProperty(name = "Order", value = "1000")))
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
    responses = @ApiResponse(responseCode = "200"),
    extensions = @Extension(name = "Order", properties = @ExtensionProperty(name = "Order", value = "1000")))
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
    responses = @ApiResponse(responseCode = "200"),
    extensions = @Extension(name = "Order", properties = @ExtensionProperty(name = "Order", value = "1000")))
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
    responses = @ApiResponse(responseCode = "200"),
    extensions = @Extension(name = "Order", properties = @ExtensionProperty(name = "Order", value = "1000")))
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
    responses = @ApiResponse(responseCode = "200"),
    extensions = @Extension(name = "Order", properties = @ExtensionProperty(name = "Order", value = "1000")))
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
    responses = @ApiResponse(responseCode = "200"),
    extensions = @Extension(name = "Order", properties = @ExtensionProperty(name = "Order", value = "1000")))
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
    responses = @ApiResponse(responseCode = "200"),
    extensions = @Extension(name = "Order", properties = @ExtensionProperty(name = "Order", value = "1000")))
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
    responses = @ApiResponse(responseCode = "200"),
    extensions = @Extension(name = "Order", properties = @ExtensionProperty(name = "Order", value = "1000")))
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
    responses = @ApiResponse(responseCode = "200"),
    extensions = @Extension(name = "Order", properties = @ExtensionProperty(name = "Order", value = "5000")))
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
