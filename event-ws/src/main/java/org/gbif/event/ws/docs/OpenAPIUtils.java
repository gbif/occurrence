package org.gbif.event.ws.docs;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.enums.Explode;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Date;
import java.util.UUID;
import org.gbif.api.util.Range;
import org.gbif.api.vocabulary.Continent;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.api.vocabulary.EventIssue;
import org.gbif.api.vocabulary.GbifRegion;
import org.gbif.api.vocabulary.License;
import org.gbif.api.vocabulary.TaxonomicStatus;
import org.gbif.api.vocabulary.ThreatStatus;
import org.springframework.http.MediaType;

public class OpenAPIUtils {

  public static final String API_PARAMETER_MAY_BE_REPEATED = "*Parameter may be repeated.*";
  public static final String API_PARAMETER_RANGE_QUERIES = "*Supports range queries.*";
  public static final String API_PARAMETER_RANGE_OR_REPEAT =
      "*Parameter may be repeated or a range.*";

  /** Documentation for the id path parameter. */
  @Target({PARAMETER, METHOD, FIELD, ANNOTATION_TYPE})
  @Retention(RetentionPolicy.RUNTIME)
  @Inherited
  @Parameter(
      name = "id",
      description = "Internal ID of the record",
      example = "5e48baa446c2a463bc76a13c1cef60c2b08fb1cf",
      schema = @Schema(implementation = String.class, minimum = "1"),
      in = ParameterIn.PATH)
  public @interface IdPathParameter {}

  /** Documentation for the event id path parameter. */
  @Target({PARAMETER, METHOD, FIELD, ANNOTATION_TYPE})
  @Retention(RetentionPolicy.RUNTIME)
  @Inherited
  @Parameter(
      name = "eventId",
      description = "Event ID of the record",
      example = "FISHINGTRIP_day1_PM",
      schema = @Schema(implementation = String.class, minimum = "1"),
      in = ParameterIn.PATH)
  public @interface EventIdPathParameter {}

  /** Documentation for the event id path parameter. */
  @Target({PARAMETER, METHOD, FIELD, ANNOTATION_TYPE})
  @Retention(RetentionPolicy.RUNTIME)
  @Inherited
  @Parameter(
      name = "datasetKey",
      description = "Dataset key of the record",
      example = "7b4b54bf-4a2f-4181-b28a-6f1943bdd782",
      schema = @Schema(implementation = UUID.class, minimum = "1"),
      in = ParameterIn.PATH)
  public @interface DatasetKeyPathParameter {}

  /** Error responses for documentation. */
  @Target({PARAMETER, METHOD, FIELD, ANNOTATION_TYPE})
  @Retention(RetentionPolicy.RUNTIME)
  @Inherited
  @ApiResponses(
      value = {
        @ApiResponse(
            responseCode = "400",
            description = "Invalid identifier supplied",
            content = @Content),
        @ApiResponse(responseCode = "404", description = "Record not found", content = @Content)
      })
  public @interface EventErrorResponses {}

  /** The usual (search) limit and offset parameters */
  @Target({PARAMETER, METHOD, FIELD, ANNOTATION_TYPE})
  @Retention(RetentionPolicy.RUNTIME)
  @Inherited
  @Parameters(
      value = {
        @Parameter(
            name = "limit",
            description =
                "Controls the number of results in the page. Using too high a value will be overwritten with the maximum threshold, which is 300 for this service. Sensible defaults are used so this may be omitted.",
            schema = @Schema(implementation = Integer.class, minimum = "0"),
            in = ParameterIn.QUERY),
        @Parameter(
            name = "offset",
            description =
                "Determines the offset for the search results. A limit of 20 and offset of 40 will get the third page of 20 results. This service has a maximum offset of 100,000.",
            schema = @Schema(implementation = Integer.class, minimum = "0"),
            in = ParameterIn.QUERY),
      })
  public @interface CommonOffsetLimitParameters {}

  /** The usual (search) facet parameters */
  @Target({PARAMETER, METHOD, FIELD, ANNOTATION_TYPE})
  @Retention(RetentionPolicy.RUNTIME)
  @Inherited
  @Parameters(
      value = {
        @Parameter(
            name = "facet",
            description =
                "A facet name used to retrieve the most frequent values for a field. Facets are allowed for "
                    + "all search parameters except geometry and geoDistance. This parameter may by repeated to request multiple "
                    + "facets, as in [this example](https://api.gbif.org/v1/event/search?facet=datasetKey&facet=eventType&limit=0).\n\n"
                    + "Note terms not available for searching are not available for faceting.",
            schema = @Schema(implementation = String.class),
            in = ParameterIn.QUERY),
        @Parameter(
            name = "facetMincount",
            description =
                "Used in combination with the facet parameter. Set facetMincount={#} to exclude facets with a "
                    + "count less than {#}, e.g. [/search?facet=eventType&limit=0&facetMincount=10000](https://api.gbif.org/v1/event/search?facet=eventType&limit=0&facetMincount=1000000].",
            schema = @Schema(implementation = Integer.class),
            in = ParameterIn.QUERY),
        @Parameter(
            name = "facetMultiselect",
            description =
                "Used in combination with the facet parameter. Set facetMultiselect=true to still return counts "
                    + "for values that are not currently filtered, e.g. [/search?facet=eventType&limit=0&eventType=Survey&facetMultiselect=true]"
                    + "(https://api.gbif.org/v1/event/search?facet=eventType&limit=0&eventType=Survey&facetMultiselect=true) "
                    + "still shows event type values of 'Project' and so on, even though the event type is being filtered.",
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
      })
  public @interface CommonFacetParameters {}

  @Target({PARAMETER, METHOD, FIELD, ANNOTATION_TYPE})
  @Retention(RetentionPolicy.RUNTIME)
  @Inherited
  @Parameters(
      value = {
        @Parameter(
            name = "acceptedTaxonKey",
            description =
                "A taxon key from the GBIF backbone or the specified checklist (see checklistKey parameter). "
                    + "Only synonym taxa are included in the search, so a search for Aves with acceptedTaxonKey=212 "
                    + "(i.e. [/event/search?taxonKey=212](https://api.gbif.org/v1/event/search?acceptedTaxonKey=212)) will match events identified as birds, but not any known family, genus or species of bird."
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(
                    uniqueItems = true,
                    schema = @Schema(implementation = Integer.class, minimum = "0")),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "2476674"),
        @Parameter(
            name = "associatedSequences",
            description =
                "Identifier (publication, global unique identifier, URI) of genetic sequence "
                    + "information associated with the material entity.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "http://www.ncbi.nlm.nih.gov/nuccore/U34853.1"),
        @Parameter(
            name = "catalogNumber",
            description =
                "An identifier of any form assigned by the source within a physical collection or digital "
                    + "dataset for the record which may not be unique, but should be fairly unique in combination with the "
                    + "institution and collection code.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "K001275042"),
        @Parameter(
            name = "classKey",
            description = "Class classification key.\n\n" + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(
                    uniqueItems = true,
                    schema = @Schema(implementation = Integer.class, minimum = "0")),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "212"),
        @Parameter(
            name = "checklistKey",
            description =
                "*Experimental.* The checklist key. This determines which taxonomy will be used for "
                    + "the search in conjunction with other taxon keys or scientificName. If this is not specified, the GBIF "
                    + "backbone taxonomy will be used.",
            schema = @Schema(implementation = String.class),
            in = ParameterIn.QUERY,
            example = "2d59e5db-57ad-41ff-97d6-11f5fb264527"),
        @Parameter(
            name = "collectionCode",
            description =
                "An identifier of any form assigned by the source to identify the physical collection or digital "
                    + "dataset uniquely within the context of an institution.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "F"),
        @Parameter(
            name = "continent",
            description =
                "Continent, as defined in our Continent vocabulary.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(
                    uniqueItems = true,
                    schema = @Schema(implementation = Continent.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "EUROPE"),
        @Parameter(
            name = "coordinateUncertaintyInMeters",
            description =
                "The horizontal distance (in metres) from the given decimalLatitude and decimalLongitude "
                    + "describing the smallest circle containing the whole of the Location.\n\n"
                    + API_PARAMETER_RANGE_QUERIES,
            schema = @Schema(implementation = Range.class),
            in = ParameterIn.QUERY,
            example = "0,500"),
        @Parameter(
            name = "country",
            description =
                "The 2-letter country code (as per ISO-3166-1) of the country in which the event "
                    + "was recorded.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = Country.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "AF"),
        @Parameter(
            name = "crawlId",
            description =
                "Crawl attempt that harvested this record.\n\n" + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = Integer.class)),
            in = ParameterIn.QUERY,
            example = "1"),
        @Parameter(
            name = "datasetId",
            description = "The ID of the dataset.\n\n" + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "https://doi.org/10.1594/PANGAEA.315492"),
        @Parameter(
            name = "datasetKey",
            description = "The event dataset key (a UUID).\n\n" + API_PARAMETER_MAY_BE_REPEATED,
            array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = UUID.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "13b70480-bd69-11dd-b15f-b8a03c50a862"),
        @Parameter(
            name = "datasetName",
            description = "The exact name of the dataset.\n\n" + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY),
        @Parameter(
            name = "day",
            description =
                "The day of the month, a number between 1 and 31.\n\n"
                    + API_PARAMETER_RANGE_OR_REPEAT,
            array =
                @ArraySchema(
                    uniqueItems = true,
                    schema = @Schema(implementation = Short.class, minimum = "1", maximum = "31")),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "15"),
        @Parameter(
            name = "decimalLatitude",
            description =
                "Latitude in decimal degrees between -90° and 90° based on WGS 84.\n\n"
                    + API_PARAMETER_RANGE_QUERIES,
            schema = @Schema(implementation = Range.class),
            in = ParameterIn.QUERY,
            example = "40.5,45"),
        @Parameter(
            name = "decimalLongitude",
            description =
                "Longitude in decimals between -180 and 180 based on WGS 84.\n\n"
                    + API_PARAMETER_RANGE_QUERIES,
            schema = @Schema(implementation = Range.class),
            in = ParameterIn.QUERY,
            example = "-120,-95.5"),
        @Parameter(
            name = "depth",
            description =
                "Depth in metres relative to altitude. For example 10 metres below a lake surface with "
                    + "given altitude.\n\n"
                    + API_PARAMETER_RANGE_OR_REPEAT,
            schema = @Schema(implementation = Range.class),
            in = ParameterIn.QUERY,
            example = "10,20"),
        @Parameter(
            name = "dwcaExtension",
            description =
                "A known Darwin Core Archive extension RowType.  Limits the search to events which have "
                    + "this extension, although they will not necessarily have any useful data recorded using the extension.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "http://rs.tdwg.org/ac/terms/Multimedia"),
        @Parameter(
            name = "elevation",
            description =
                "Elevation (altitude) in metres above sea level.\n\n"
                    + API_PARAMETER_RANGE_OR_REPEAT,
            schema = @Schema(implementation = Range.class),
            in = ParameterIn.QUERY,
            example = "1000,1250"),
        @Parameter(
            name = "endDayOfYear",
            description =
                "The latest integer day of the year on which the event occurred.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(
                    uniqueItems = true,
                    schema = @Schema(implementation = Short.class, minimum = "1", maximum = "366")),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "6"),
        @Parameter(
            name = "eventDate",
            description =
                "Event date in ISO 8601 format: yyyy, yyyy-MM or yyyy-MM-dd.\n\n"
                    + API_PARAMETER_RANGE_OR_REPEAT,
            array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = Date.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "2000,2001-06-30"),
        @Parameter(
            name = "eventId",
            description =
                "An identifier for the information associated with a sampling event.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "A 123"),
        @Parameter(
            name = "familyKey",
            description = "Family classification key.",
            array =
                @ArraySchema(
                    uniqueItems = true,
                    schema = @Schema(implementation = Integer.class, minimum = "0")),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "2405"),
        @Parameter(
            name = "fieldNumber",
            description =
                "An identifier given to the event in the field. Often serves as a link between field notes and the event.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "RV Sol 87-03-08"),
        @Parameter(
            name = "gadmGid",
            description =
                "A GADM geographic identifier at any level, for example AGO, AGO.1_1, AGO.1.1_1 or AGO.1.1.1_1\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "AGO.1_1"),
        @Parameter(
            name = "gadmLevel0Gid",
            description =
                "A GADM geographic identifier at the zero level, for example AGO.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "AGO"),
        @Parameter(
            name = "gadmLevel1Gid",
            description =
                "A GADM geographic identifier at the first level, for example AGO.1_1.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "AGO.1_1"),
        @Parameter(
            name = "gadmLevel2Gid",
            description =
                "A GADM geographic identifier at the second level, for example AFG.1.1_1.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "AFG.1.1_1"),
        @Parameter(
            name = "gadmLevel3Gid",
            description =
                "A GADM geographic identifier at the third level, for example AFG.1.1.1_1.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "AFG.1.1.1_1"),
        @Parameter(
            name = "gbifId",
            description = "The unique GBIF key for a single event.",
            schema = @Schema(implementation = Long.class),
            in = ParameterIn.QUERY,
            example = "2005380410"),
        @Parameter(
            name = "gbifRegion",
            description = "Gbif region based on country code.\n\n" + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(
                    uniqueItems = true,
                    schema = @Schema(implementation = GbifRegion.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "AFRICA"),
        @Parameter(
            name = "genusKey",
            description = "Genus classification key.",
            array =
                @ArraySchema(
                    uniqueItems = true,
                    schema = @Schema(implementation = Integer.class, minimum = "0")),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "2877951"),
        @Parameter(
            name = "geoDistance",
            description =
                "Filters to match event records with coordinate values within a specified distance of "
                    + "a coordinate.\n\nDistance may be specified in kilometres (km) or metres (m).",
            schema = @Schema(implementation = String.class),
            in = ParameterIn.QUERY,
            example = "90,100,5km"),
        @Parameter(
            name = "georeferencedBy",
            description =
                "Name of a person, group, or organization who determined the georeference (spatial representation) for the location.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "Brad Millen"),
        @Parameter(
            name = "geometry",
            description =
                "Searches for events inside a polygon described in Well Known Text (WKT) format. "
                    + "Only `POLYGON` and `MULTIPOLYGON` are accepted WKT types.\n\n"
                    + "For example, a shape written as `POLYGON ((30.1 10.1, 40 40, 20 40, 10 20, 30.1 10.1))` would be queried "
                    + "as is.\n\n"
                    + "_Polygons must have *anticlockwise* ordering of points._ "
                    + "(A clockwise polygon represents the opposite area: the Earth's surface with a 'hole' in it. "
                    + "Such queries are not supported.)\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "POLYGON ((30.1 10.1, 40 40, 20 40, 10 20, 30.1 10.1))"),
        @Parameter(
            name = "hasCoordinate",
            description =
                "Limits searches to event records which contain a value in both latitude and "
                    + "longitude (i.e. `hasCoordinate=true` limits to event records with coordinate values and "
                    + "`hasCoordinate=false` limits to event records without coordinate values).",
            schema = @Schema(implementation = Boolean.class),
            in = ParameterIn.QUERY,
            example = "true"),
        @Parameter(
            name = "higherGeography",
            description =
                "Geographic name less specific than the information captured in the locality term.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "Argentina"),
        /** Humboldt extension params * */
        @Parameter(
            name = "humboldtAbundanceCap",
            description =
                "The reported maximum number of organisms.\n\n" + API_PARAMETER_RANGE_OR_REPEAT,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = Integer.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "50"),
        @Parameter(
            name = "humboldtAreNonTargetTaxaFullyReported",
            description =
                "Indicates whether there were non-target taxa that were detected, but left unreported",
            schema = @Schema(implementation = Boolean.class),
            in = ParameterIn.QUERY,
            example = "true"),
        @Parameter(
            name = "humboldtCompilationSourceTypes",
            description =
                "The types of data sources contributing to the compilation reported.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "localKnowledge"),
        @Parameter(
            name = "humboldtCompilationTypes",
            description =
                "Specifies the compilation types used to get the data.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "compilationOfExistingSourcesAndSamplingEvents"),
        @Parameter(
            name = "humboldtEventDuration",
            description =
                "Utility parameter to specify the event duration value and unit together. See the humboldtEventDurationUnit "
                    + "and humboldtEventDurationUnit parameters to find the accepted values.\n\n"
                    + API_PARAMETER_RANGE_OR_REPEAT,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "1.5 hours"),
        @Parameter(
            name = "humboldtEventDurationUnit",
            description =
                "The [units](https://github.com/gbif/gbif-api/blob/dev/src/main/java/org/gbif/api/vocabulary/DurationUnit.java) "
                    + "for the humboldtEventDurationValue. To filter by multiple values or use ranges the "
                    + "humboldtEventDuration parameter should be used instead.",
            schema = @Schema(implementation = String.class),
            in = ParameterIn.QUERY,
            example = "hour"),
        @Parameter(
            name = "humboldtEventDurationValue",
            description =
                "The numeric value for the duration of the event. It accepts decimals. To filter by multiple values or use "
                    + "ranges the humboldtEventDuration parameter should be used instead.",
            schema = @Schema(implementation = Double.class),
            in = ParameterIn.QUERY,
            example = "2"),
        @Parameter(name = "humboldtEventDurationValueInMinutes", hidden = true),
        @Parameter(
            name = "humboldtGeospatialScopeAreaUnit",
            description =
                "The units for the humboldtGeospatialScopeAreaValue.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "km²"),
        @Parameter(
            name = "humboldtGeospatialScopeAreaValue",
            description =
                "The numeric value for the total area of the geospatial scope of the event.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = Double.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "100"),
        @Parameter(
            name = "humboldtHasMaterialSamples",
            description = "Indicates if material samples were collected during the event.",
            schema = @Schema(implementation = Boolean.class),
            in = ParameterIn.QUERY,
            example = "true"),
        @Parameter(
            name = "humboldtHasNonTargetOrganisms",
            description =
                "Indicates if there were organisms outside the target organismal scopes that were detected and reported.",
            schema = @Schema(implementation = Boolean.class),
            in = ParameterIn.QUERY,
            example = "true"),
        @Parameter(
            name = "humboldtHasNonTargetTaxa",
            description =
                "Indicates if there were organisms outside the target taxonomic scope that were detected and reported.",
            schema = @Schema(implementation = Boolean.class),
            in = ParameterIn.QUERY,
            example = "true"),
        @Parameter(
            name = "humboldtHasVouchers",
            description = "Indicates if specimen vouchers were collected during the event.",
            schema = @Schema(implementation = Boolean.class),
            in = ParameterIn.QUERY,
            example = "true"),
        @Parameter(
            name = "humboldtInventoryTypes",
            description =
                "The types of search processes used to conduct the inventory.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "openSearch"),
        @Parameter(
            name = "humboldtIsAbsenceReported",
            description = "Indicates if taxonomic absences were reported.",
            schema = @Schema(implementation = Boolean.class),
            in = ParameterIn.QUERY,
            example = "true"),
        @Parameter(
            name = "humboldtIsAbundanceCapReported",
            description = "Indicates if the abundance cap was reported.",
            schema = @Schema(implementation = Boolean.class),
            in = ParameterIn.QUERY,
            example = "true"),
        @Parameter(
            name = "humboldtIsAbundanceReported",
            description = "Indicates if the abundance was reported",
            schema = @Schema(implementation = Boolean.class),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "true"),
        @Parameter(
            name = "humboldtIsDegreeOfEstablishmentScopeFullyReported",
            description =
                "Indicates if the organisms included in the degree of establishment scope that were detected were reported.",
            schema = @Schema(implementation = Boolean.class),
            in = ParameterIn.QUERY,
            example = "true"),
        @Parameter(
            name = "humboldtIsGrowthFormScopeFullyReported",
            description =
                "Indicates if the organisms included in the growth form scope that were detected were reported.",
            schema = @Schema(implementation = Boolean.class),
            in = ParameterIn.QUERY,
            example = "true"),
        @Parameter(
            name = "humboldtIsLeastSpecificTargetCategoryQuantityInclusive",
            description =
                "Indicates if the total detected quantity for a taxon in an event is given explicitly in a single record "
                    + "for that taxon.",
            schema = @Schema(implementation = Boolean.class),
            in = ParameterIn.QUERY,
            example = "true"),
        @Parameter(
            name = "humboldtIsLifeStageScopeFullyReported",
            description =
                "Indicates if the organisms included in the life stage scope that were detected were reported.",
            schema = @Schema(implementation = Boolean.class),
            in = ParameterIn.QUERY,
            example = "true"),
        @Parameter(
            name = "humboldtIsSamplingEffortReported",
            description = "Indicates if the sampling effort of the event was reported.",
            schema = @Schema(implementation = Boolean.class),
            in = ParameterIn.QUERY,
            example = "true"),
        @Parameter(
            name = "humboldtIsTaxonomicScopeFullyReported",
            description =
                "Indicates if the organisms included in the taxonomic scope that were detected were reported.",
            schema = @Schema(implementation = Boolean.class),
            in = ParameterIn.QUERY,
            example = "true"),
        @Parameter(
            name = "humboldtIsVegetationCoverReported",
            description = "Indicates if a vegetation cover metric was reported.",
            schema = @Schema(implementation = Boolean.class),
            in = ParameterIn.QUERY,
            example = "true"),
        @Parameter(
            name = "humboldtMaterialSampleTypes",
            description =
                "The list of material sample types collected during the event.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "skeleton"),
        @Parameter(
            name = "humboldtProtocolNames",
            description =
                "The categorical descriptive names for the methods used during the event-\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "boxTrapping"),
        @Parameter(
            name = "humboldtSamplingEffortUnit",
            description =
                "The units for the humboldtSamplingEffortValue.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "trapHours"),
        @Parameter(
            name = "humboldtSamplingEffortValue",
            description =
                "The numeric value for the sampling effort used during the event.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "100"),
        @Parameter(
            name = "humboldtSamplingPerformedBy",
            description =
                "The person, group, or organization responsible for recording the event.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "North American Butterfly Association"),
        @Parameter(
            name = "humboldtSiteCount",
            description =
                "The total number of individual sites surveyed during the event.\n\n"
                    + API_PARAMETER_RANGE_OR_REPEAT,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = Integer.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "5"),
        @Parameter(
            name = "humboldtTargetDegreeOfEstablishmentScope",
            description =
                "The degrees of establishments targeted during the event.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "native"),
        @Parameter(
            name = "humboldtTargetGrowthFormScope",
            description =
                "The growth forms or habits targeted during the event.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "tree"),
        @Parameter(
            name = "humboldtTargetHabitatScope",
            description =
                "The habitats targeted during the event.\n\n" + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "pineForest"),
        @Parameter(
            name = "humboldtTargetLifeStageScope",
            description =
                "The life stages targeted during the event.\n\n" + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "adult"),
        @Parameter(
            name = "humboldtTargetTaxonomicScopeTaxonKey",
            description =
                "The taxon keys of the taxonomic groups targeted during the event.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(
                    uniqueItems = true,
                    schema = @Schema(implementation = Integer.class, minimum = "0")),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "212"),
        @Parameter(
            name = "humboldtTargetTaxonomicScopeUsageKey",
            description =
                "The usage keys of the taxonomic groups targeted during the event.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(
                    uniqueItems = true,
                    schema = @Schema(implementation = Integer.class, minimum = "0")),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "212"),
        @Parameter(
            name = "humboldtTargetTaxonomicScopeUsageName",
            description =
                "The usage names of the taxonomic groups targeted during the event.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "Aves"),
        @Parameter(
            name = "humboldtTaxonCompletenessProtocols",
            description =
                "The methods used to determine eco:taxonCompletenessReported.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "census"),
        @Parameter(
            name = "humboldtTaxonomicIssue",
            description =
                "A specific taxonomic interpretation issue for the targeted taxonomic scope as defined in our "
                    + "EventIssue enumeration.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(
                    uniqueItems = true,
                    schema =
                        @Schema(
                            type = "string",
                            allowableValues = {
                              "TAXON_MATCH_FUZZY",
                              "TAXON_MATCH_HIGHERRANK",
                              "TAXON_MATCH_AGGREGATE",
                              "TAXON_MATCH_SCIENTIFIC_NAME_ID_IGNORED",
                              "TAXON_MATCH_TAXON_CONCEPT_ID_IGNORED",
                              "TAXON_MATCH_TAXON_ID_IGNORED",
                              "SCIENTIFIC_NAME_ID_NOT_FOUND",
                              "TAXON_CONCEPT_ID_NOT_FOUND",
                              "TAXON_ID_NOT_FOUND",
                              "SCIENTIFIC_NAME_AND_ID_INCONSISTENT",
                              "TAXON_MATCH_NONE"
                            })),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "TAXON_MATCH_HIGHERRANK"),
        @Parameter(
            name = "humboldtTotalAreaSampledUnit",
            description =
                "The units for the humboldtTotalAreaSampledValue.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "km²"),
        @Parameter(
            name = "humboldtTotalAreaSampledValue",
            description =
                "The numeric value for the total area surveyed during the event.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = Double.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "1.6"),
        @Parameter(
            name = "humboldtVerbatimSiteNames",
            description =
                "The sites or locations at which the events took place.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "East coast"),
        @Parameter(
            name = "humboldtVoucherInstitutions",
            description =
                "The names or acronyms of the institutions where vouchers collected during the event were deposited.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "FMNH"),
        /** End Humboldt extension params * */
        @Parameter(
            name = "hasGeospatialIssue",
            description =
                "Includes/excludes event records which contain spatial issues (as determined in our "
                    + "record interpretation), i.e. hasGeospatialIssue=true returns only those records with spatial issues "
                    + "while hasGeospatialIssue=false includes only records without spatial issues.\n\n"
                    + "The absence of this parameter returns any record with or without spatial issues.",
            schema = @Schema(implementation = Boolean.class),
            in = ParameterIn.QUERY,
            example = "true"),
        @Parameter(
            name = "hostingOrganizationKey",
            description =
                "The key (UUID) of the publishing organization whose installation (server) hosts the original "
                    + "dataset.\n\n"
                    + "(This is of little interest to most data users.)\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = UUID.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "fbca90e3-8aed-48b1-84e3-369afbd000ce"),
        @Parameter(
            name = "installationKey",
            description =
                "The event installation key (a UUID).\n\n"
                    + "(This is of little interest to most data users.  It is the identifier for the server that provided the data "
                    + "to GBIF.)\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = UUID.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "17a83780-3060-4851-9d6f-029d5fcb81c9"),
        @Parameter(
            name = "institutionCode",
            description =
                "An identifier of any form assigned by the source to identify the institution the "
                    + "record belongs to. Not guaranteed to be unique.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "K"),
        @Parameter(
            name = "issue",
            description =
                "A specific interpretation issue as defined in our EventIssue enumeration.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(
                    uniqueItems = true,
                    schema = @Schema(implementation = EventIssue.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "COUNTRY_COORDINATE_MISMATCH"),
        @Parameter(
            name = "island",
            description =
                "The name of the island on or near which the location occurs.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "Zanzibar"),
        @Parameter(
            name = "islandGroup",
            description =
                "The name of the island group in which the location occurs.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "Seychelles"),
        @Parameter(
            name = "iucnRedListCategory",
            description =
                "A threat status category from the IUCN Red List.  The two-letter code for the status should "
                    + "be used.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(
                    uniqueItems = true,
                    schema = @Schema(implementation = ThreatStatus.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "EX"),
        @Parameter(
            name = "kingdomKey",
            description = "Kingdom classification key.\n\n" + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(
                    uniqueItems = true,
                    schema = @Schema(implementation = Integer.class, minimum = "0")),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "5"),
        @Parameter(
            name = "lastInterpreted",
            description =
                "This date the record was last modified in GBIF, in ISO 8601 format: yyyy, yyyy-MM, yyyy-MM-dd, "
                    + "or MM-dd.\n\n"
                    + "Note that this is the date the record was last changed in GBIF, not necessarily the date the record was "
                    + "first/last changed by the publisher. Data is re-interpreted when we change the taxonomic backbone, "
                    + "geographic data sources, or interpretation processes.\n\n"
                    + API_PARAMETER_RANGE_OR_REPEAT,
            array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = Date.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "2023-02"),
        @Parameter(
            name = "license",
            description =
                "The licence applied to the dataset or record by the publisher.\n\n"
                    + API_PARAMETER_RANGE_OR_REPEAT,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = License.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "CC0_1_0"),
        @Parameter(
            name = "locality",
            description =
                "The specific description of the place.\n\n" + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY),
        @Parameter(
            name = "mediaType",
            description =
                "The kind of multimedia associated with an event as defined in our MediaType enumeration.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(
                    uniqueItems = true,
                    schema = @Schema(implementation = MediaType.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY),
        @Parameter(
            name = "modified",
            description =
                "The most recent date-time on which the occurrnce was changed, according to the publisher.\n\n"
                    + API_PARAMETER_RANGE_OR_REPEAT,
            array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = Date.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "2023-02-20"),
        @Parameter(
            name = "month",
            description =
                "The month of the year, starting with 1 for January.\n\n"
                    + API_PARAMETER_RANGE_OR_REPEAT,
            array =
                @ArraySchema(
                    uniqueItems = true,
                    schema = @Schema(implementation = Short.class, minimum = "1", maximum = "12")),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "5"),
        @Parameter(
            name = "networkKey",
            description = "The network's GBIF key (a UUID).\n\n" + API_PARAMETER_MAY_BE_REPEATED,
            array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = UUID.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "2b7c7b4f-4d4f-40d3-94de-c28b6fa054a6"),
        @Parameter(
            name = "orderKey",
            description = "Order classification key.\n\n" + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(
                    uniqueItems = true,
                    schema = @Schema(implementation = Integer.class, minimum = "0")),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "1448"),
        @Parameter(
            name = "otherCatalogNumbers",
            description =
                "Previous or alternate fully qualified catalog numbers.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY),
        @Parameter(
            name = "parentEventId",
            description =
                "An identifier for the information associated with a sampling event.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "A 123"),
        @Parameter(
            name = "phylumKey",
            description = "Phylum classification key.\n\n" + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(
                    uniqueItems = true,
                    schema = @Schema(implementation = Integer.class, minimum = "0")),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "44"),
        @Parameter(
            name = "programme",
            description =
                "A group of activities, often associated with a specific funding stream, such as the "
                    + "GBIF BID programme.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "BID"),
        @Parameter(
            name = "projectId",
            description =
                "The identifier for a project, which is often assigned by a funded programme.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "bid-af2020-039-reg"),
        @Parameter(
            name = "protocol",
            description =
                "Protocol or mechanism used to provide the event record.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(
                    uniqueItems = true,
                    schema = @Schema(implementation = EndpointType.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "DWC_ARCHIVE"),
        @Parameter(
            name = "publishingCountry",
            description =
                "The 2-letter country code (as per ISO-3166-1) of the owning organization's country.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = Country.class)),
            explode = Explode.TRUE,
            example = "AD"),
        @Parameter(
            name = "publishedByGbifRegion",
            description =
                "GBIF region based on the owning organization's country.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(
                    uniqueItems = true,
                    schema = @Schema(implementation = GbifRegion.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "AFRICA"),
        @Parameter(
            name = "publishingOrg",
            description =
                "The publishing organization's GBIF key (a UUID).\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array = @ArraySchema(uniqueItems = true, schema = @Schema(implementation = UUID.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "e2e717bf-551a-4917-bdc9-4fa0f342c530"),
        @Parameter(
            name = "recordNumber",
            description =
                "An identifier given to the record at the time it was recorded in the field.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "1"),
        @Parameter(
            name = "repatriated",
            description =
                "Searches for records whose publishing country is different to the country in which the "
                    + "record was recorded.",
            schema = @Schema(implementation = Boolean.class),
            in = ParameterIn.QUERY,
            example = "true"),
        @Parameter(
            name = "sampleSizeUnit",
            description =
                "The unit of measurement of the size (time duration, length, area, or volume) of a sample in a "
                    + "sampling event.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "hectares"),
        @Parameter(
            name = "sampleSizeValue",
            description =
                "A numeric value for a measurement of the size (time duration, length, area, or volume) of a "
                    + "sample in a sampling event.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = Double.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "50.50"),
        @Parameter(
            name = "samplingProtocol",
            description =
                "The name of, reference to, or description of the method or protocol used during a sampling event.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "malaise trap"),
        @Parameter(
            name = "scientificName",
            description =
                "A scientific name from the [GBIF backbone](https://www.gbif.org/dataset/d7dddbf4-2cf0-4f39-9b2a-bb099caae36c) "
                    + "or the specified checklist (see checklistKey parameter). "
                    + "All included and synonym taxa are included in the search.\n\n"
                    + "Under the hood a call to the [species match service](https://www.gbif.org/developer/species#searching) "
                    + "is done first to retrieve a taxonKey. Only unique scientific names will return results, homonyms "
                    + "(many monomials) return nothing! Consider to use the taxonKey parameter instead and the species match "
                    + "service directly.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "Quercus robur"),
        @Parameter(
            name = "speciesKey",
            description = "Species classification key." + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(
                    uniqueItems = true,
                    schema = @Schema(implementation = Integer.class, minimum = "0")),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "2476674"),
        @Parameter(
            name = "startDayOfYear",
            description =
                "The earliest integer day of the year on which the event occurred.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(
                    uniqueItems = true,
                    schema = @Schema(implementation = Short.class, minimum = "1", maximum = "366")),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "5"),
        @Parameter(
            name = "stateProvince",
            description =
                "The name of the next smaller administrative region than country (state, province, canton, "
                    + "department, region, etc.) in which the Location occurs.\n\n"
                    + "This term does not have any data quality checks; see also the GADM parameters.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "Leicestershire"),
        @Parameter(
            name = "subgenusKey",
            hidden = true, // Not yet implemented
            description = "Subgenus classification key.",
            array =
                @ArraySchema(
                    uniqueItems = true,
                    schema = @Schema(implementation = Integer.class, minimum = "0")),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "0"),
        @Parameter(
            name = "taxonKey",
            description =
                "A taxon key from the GBIF backbone or the specified checklist (see checklistKey parameter). All included (child) and synonym taxa are included in the search, so a search for Aves with taxonKey=212 (i.e. [/event/search?taxonKey=212](https://api.gbif.org/v1/event/search?taxonKey=212)) will match all birds, no matter which species."
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(
                    uniqueItems = true,
                    schema = @Schema(implementation = Integer.class, minimum = "0")),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "2476674"),
        @Parameter(
            name = "taxonId",
            description =
                "The taxon identifier provided to GBIF by the data publisher.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "urn:lsid:dyntaxa.se:Taxon:103026"),
        // TODO: creo q hay q quitarlo
        @Parameter(
            name = "taxonomicIssue",
            description =
                "*Experimental.* A specific taxonomic interpretation issue as defined in our "
                    + "EventIssue enumeration.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(
                    uniqueItems = true,
                    schema =
                        @Schema(
                            type = "string",
                            allowableValues = {
                              "TAXON_MATCH_FUZZY",
                              "TAXON_MATCH_HIGHERRANK",
                              "TAXON_MATCH_AGGREGATE",
                              "TAXON_MATCH_SCIENTIFIC_NAME_ID_IGNORED",
                              "TAXON_MATCH_TAXON_CONCEPT_ID_IGNORED",
                              "TAXON_MATCH_TAXON_ID_IGNORED",
                              "SCIENTIFIC_NAME_ID_NOT_FOUND",
                              "TAXON_CONCEPT_ID_NOT_FOUND",
                              "TAXON_ID_NOT_FOUND",
                              "SCIENTIFIC_NAME_AND_ID_INCONSISTENT",
                              "TAXON_MATCH_NONE"
                            })),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "TAXON_CONCEPT_ID_NOT_FOUND"),
        @Parameter(
            name = "taxonomicStatus",
            description =
                "A taxonomic status from our TaxonomicStatus enumeration.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(
                    uniqueItems = true,
                    schema = @Schema(implementation = TaxonomicStatus.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "SYNONYM"),
        @Parameter(
            name = "verbatimScientificName",
            description =
                "The scientific name provided to GBIF by the data publisher, before interpretation and "
                    + "processing by GBIF.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "Quercus robur L."),
        @Parameter(
            name = "waterBody",
            description =
                "The name of the water body in which the Locations occurs.\n\n"
                    + API_PARAMETER_MAY_BE_REPEATED,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = String.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "Lake Michigan"),
        @Parameter(
            name = "year",
            description =
                "The 4 digit year. A year of 98 will be interpreted as AD 98.\n\n"
                    + API_PARAMETER_RANGE_OR_REPEAT,
            array =
                @ArraySchema(uniqueItems = true, schema = @Schema(implementation = Integer.class)),
            explode = Explode.TRUE,
            in = ParameterIn.QUERY,
            example = "1998"),
        @Parameter(
            name = "matchCase",
            description = "*Experimental.* Indicates if the search has to be case sensitive",
            schema = @Schema(implementation = Boolean.class),
            in = ParameterIn.QUERY,
            example = "true"),
        @Parameter(
            name = "shuffle",
            description = "*Experimental.* Seed to sort the results randomly.",
            schema = @Schema(implementation = String.class),
            in = ParameterIn.QUERY,
            example = "abcdefgh"),
        @Parameter(
            name = "hl",
            description =
                "Set `hl=true` to highlight terms matching the query when in full-text search fields. The highlight will "
                    + "be an emphasis tag of class `gbifH1` e.g. "
                    + "[`/search?q=plant&hl=true`](https://api.gbif.org/v1/literature/search?q=plant&hl=true).\n\n"
                    + "Full-text search fields include: title, keyword, country, publishing country, publishing organization "
                    + "title, hosting organization title, and description. One additional full text field is searched which "
                    + "includes information from metadata documents, but the text of this field is not returned in the response.",
            schema = @Schema(implementation = Boolean.class),
            in = ParameterIn.QUERY,
            example = "true"),
        @Parameter(
            name = "q",
            description =
                "Simple full-text search parameter. The value for this parameter can be a simple word or a phrase. "
                    + "Wildcards are not supported",
            schema = @Schema(implementation = String.class),
            in = ParameterIn.QUERY),
        @Parameter(name = "request", hidden = true)
      })
  @CommonFacetParameters
  @CommonOffsetLimitParameters
  public @interface EventSearchParameters {}
}
