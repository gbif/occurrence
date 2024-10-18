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

import com.google.common.collect.ImmutableSet;
import io.swagger.v3.oas.annotations.Hidden;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.extensions.ExtensionProperty;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Builder;
import lombok.Data;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.HiveColumnsUtils;
import org.gbif.occurrence.common.TermUtils;
import org.gbif.occurrence.download.hive.AvroQueries;
import org.gbif.occurrence.download.hive.DownloadTerms;
import org.gbif.occurrence.download.hive.HiveQueries;
import org.gbif.occurrence.download.hive.InitializableField;
import org.springframework.http.MediaType;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.gbif.occurrence.common.HiveColumnsUtils.HIVE_RESERVED_WORDS;

/**
 * Resource to describe file/table formats use in GBIF occurrence downloads.
 *
 * <p>Project-specific exports are not handled by this resource: DownloadFormat.BIONOMIA; DownloadFormat.MAP_OF_LIFE.
 */
@Tag(
  name = "Occurrence download formats",
  description = "This API lists the fields present in the various download formats, their data types and their term identifiers.",
  extensions = @io.swagger.v3.oas.annotations.extensions.Extension(
    name = "Order", properties = @ExtensionProperty(name = "Order", value = "0400"))
)
@RestController
@Validated
@RequestMapping(produces = {MediaType.APPLICATION_JSON_VALUE,
  "application/x-javascript"}, value = "occurrence/download/describe")
public class OccurrenceDownloadDescribeResource {

  // Required fields in downloads
  private static final Set<Term> REQUIRED_FIELD = new ImmutableSet.Builder<Term>()
                                                    .add(GbifTerm.gbifID, GbifTerm.datasetKey)
                                                    .build();

  /**
   * Gets the Hive data type used for a Term.
   */
  private static String fieldType(Term term) {
    String hiveType = HiveColumnsUtils.getHiveType(term);
    if (HiveColumnsUtils.isDate(term)) {
      return "DATE";
    }
    return hiveType;
  }

  /**
   * Additional description about a format used in a field.
   */
  private static String fieldTypeFormat(Term term) {
    if (TermUtils.isInterpretedUtcDateSeconds(term)) {
      return "yyyy-MM-ddTHH:mm:ssZ";
    } else if (TermUtils.isInterpretedUtcDateMilliseconds(term)) {
        return "yyyy-MM-ddTHH:mm:ss.SSSZ";
    } else if(TermUtils.isInterpretedLocalDateSeconds(term)) {
      return "yyyy-MM-ddTHH:mm:ss";
    }
    return null;
  }

  /**
   * Delimiter for array values.
   */
  private static String fieldDelimiter(Term term) {
    if (HiveColumnsUtils.isHiveArray(term)) {
      return ";";
    }
    return null;
  }

  private static final HiveQueries HIVE_QUERIES = new HiveQueries();
  private static final AvroQueries AVRO_QUERIES = new AvroQueries();

  /**
   * Field description of file or download table.
   */
  @Schema(
    description = "Field description of file or download table."
  )
  @Data
  @Builder
  public static class Field {
    @Schema(description = "The column name in download files.")
    private final String name;

    @Schema(description = "The data type.")
    private final String type;

    @Schema(description = "A pattern showing the format of the field data.")
    private String typeFormat;

    @Schema(description = "The character used to delimit array values.")
    private String delimiter;

    @Schema(description = "The URI for the term (e.g. Darwin Core term) for the field.")
    private final Term term;

    @Schema(description = "Whether the field may be null (empty).  If `false` it will be present on all records.")
    private boolean nullable;
  }

  /**
   * Table, a simple wrapper around a list of fields.
   */
  @Builder
  @Data
  public static class Table {
    private final List<Field> fields;
  }

  /**
   * DwCA download description.
   */
  @Data
  public static class DwcDownload {
    private final Table verbatim = Table.builder()
      .fields(toFieldList(HIVE_QUERIES.selectVerbatimFields(), false))
      .build();

    private final Table multimedia = Table.builder()
      .fields(toFieldList(HIVE_QUERIES.selectMultimediaFields(false), true))
      .build();

    private final Table interpreted = Table.builder()
      .fields(toFieldList(HIVE_QUERIES.selectInterpretedFields(false), true))
      .build();

    private final List<String> verbatimExtensions =
      Extension.availableExtensions().stream().map(Extension::getRowType).collect(Collectors.toList());

    /**
     * Transforms Map<String,InitializableField> queryFields into a List<Field>.
     */
    private static List<Field> toFieldList(Map<String,InitializableField> queryFields, boolean interpreted) {
      return queryFields.values().stream()
        .map(initializableField -> {
          Field.FieldBuilder builder = Field.builder()
            .name(initializableField.getTerm().simpleName())
            .type(interpreted ? fieldType(initializableField.getTerm()) : initializableField.getHiveDataType())
            .term(initializableField.getTerm())
            .nullable(!REQUIRED_FIELD.contains(initializableField.getTerm()));
          if (interpreted) {
            builder.typeFormat(fieldTypeFormat(initializableField.getTerm()));
            builder.delimiter(fieldDelimiter(initializableField.getTerm()));
          }
          return builder.build();
        })
        .collect(Collectors.toList());
    }
  }

  // Static cached definitions
  // These are all slightly different!
  private static final DwcDownload DWC_DOWNLOAD = new DwcDownload();

  // Simple CSV — names like "gbifID" and "verbatimScientificName", text delimiters.
  private static final Table SIMPLE_CSV = Table.builder()
    .fields(DownloadTerms.SIMPLE_DOWNLOAD_TERMS
      .stream()
      .map(termPair -> Field.builder()
        .name(DownloadTerms.simpleName(termPair))
        .type(fieldType(termPair.getRight()))
        .typeFormat(fieldTypeFormat(termPair.getRight()))
        .delimiter(fieldDelimiter(termPair.getRight()))
        .term(termPair.getRight())
        .nullable(!REQUIRED_FIELD.contains(termPair.getRight()))
        .build())
      .collect(Collectors.toList()))
    .build();

  // Species List — names like "gbifID", possible delimiters, different required term.
  private static final Table SPECIES_LIST = Table.builder()
    .fields(DownloadTerms.SPECIES_LIST_DOWNLOAD_TERMS
      .stream().map(p -> p.getRight())
      .map(term -> Field.builder()
        .name(term.simpleName())
        .type(fieldType(term))
        .typeFormat(fieldTypeFormat(term))
        .delimiter(fieldDelimiter(term))
        .term(term)
        .nullable(!GbifTerm.taxonKey.equals(term))
        .build())
      .collect(Collectors.toList()))
    .build();

  // Simple Avro — names like "gbifID" and "verbatimScientificName", no text delimiters.
  private static final Table SIMPLE_AVRO = Table.builder()
    .fields(DownloadTerms.SIMPLE_DOWNLOAD_TERMS
      .stream()
      .map(termPair -> Field.builder()
        .name(DownloadTerms.simpleName(termPair))
        .type(fieldType(termPair.getRight()))
        .typeFormat(fieldTypeFormat(termPair.getRight()))
        .term(termPair.getRight())
        .nullable(!REQUIRED_FIELD.contains(termPair.getRight()))
        .build())
      .collect(Collectors.toList()))
    .build();

  // Simple Parquet — names like "gbifID" and "verbatimScientificName", no text delimiters.
  private static final Table SIMPLE_PARQUET = Table.builder()
    .fields(DownloadTerms.SIMPLE_DOWNLOAD_TERMS
      .stream()
      .map(termPair -> Field.builder()
        .name(DownloadTerms.simpleName(termPair))
        .type(fieldType(termPair.getRight()))
        .typeFormat(fieldTypeFormat(termPair.getRight()))
        .term(termPair.getRight())
        .nullable(!REQUIRED_FIELD.contains(termPair.getRight()))
        .build())
      .collect(Collectors.toList()))
    .build();

  // Simple with verbatim Avro — names like "gbifID" and "v_scientificName", no text delimiters.
  // TODO: This isn't quite right, as it is lowercasing the names.
  private static final Table SIMPLE_WITH_VERBATIM_AVRO = Table.builder()
    .fields(AVRO_QUERIES.simpleWithVerbatimAvroQueryFields(false).values()
      .stream()
      .map(initializableField -> Field.builder()
        .name(initializableField.getTerm().simpleName())
        .type(fieldType(initializableField.getTerm()))
        .typeFormat(fieldTypeFormat(initializableField.getTerm()))
        .term(initializableField.getTerm())
        .nullable(!REQUIRED_FIELD.contains(initializableField.getTerm()))
        .build())
      .collect(Collectors.toList()))
    .build();

  /**
   * Transforms Map<String,InitializableField> queryFields into a List<Field> for the SQL fields.
   */
  private static List<Field> toTypedFieldList(Map<String,InitializableField> queryFields, boolean interpreted) {
    return queryFields.entrySet().stream()
      .filter(field -> !(interpreted && GbifTerm.verbatimScientificName == field.getValue().getTerm()))
      .map(field -> {
        InitializableField initializableField = field.getValue();
        String columnName = field.getKey().toLowerCase(Locale.ENGLISH);
        if (HIVE_RESERVED_WORDS.contains(columnName)) {
          columnName += '_';
        }
        if (GbifTerm.verbatimScientificName == initializableField.getTerm()) {
          columnName = "v_" + DwcTerm.scientificName.simpleName().toLowerCase();
        }
        Field.FieldBuilder builder = Field.builder()
          .name(columnName)
          .type(interpreted ? fieldType(initializableField.getTerm()) : initializableField.getHiveDataType())
          .term(initializableField.getTerm())
          .nullable(!REQUIRED_FIELD.contains(initializableField.getTerm()));
        return builder.build();
      })
      .collect(Collectors.toList());
  }

  private static final Table SQL = Table.builder()
    .fields(ImmutableSet.<Field>builder()
      .addAll(toTypedFieldList(HIVE_QUERIES.selectInterpretedFields(false), true))
      .addAll(toTypedFieldList(HIVE_QUERIES.selectInternalSearchFields(false), true))
      .addAll(toTypedFieldList(HIVE_QUERIES.selectVerbatimFields(), false))
      .build()
      .asList()
    ).build();
  // End of static cached definitions

  @Operation(
    operationId = "describeDwcaDownload",
    summary = "**Experimental.** Describes the fields present in a Darwin Core Archive format download")
  @ApiResponses(
    value = {
      @ApiResponse(
        responseCode = "200",
        description = "Field description",
        content = {
          @Content(
            mediaType = "application/json",
            schema = @Schema(implementation = DwcDownload.class))
        })
    })
  @GetMapping("dwca")
  public DwcDownload dwca() {
    return DWC_DOWNLOAD;
  }

  @Operation(
    operationId = "describeSimpleCsvDownload",
    summary = "**Experimental.** Describes the fields present in a Simple CSV format download")
  @ApiResponses(
    value = {
      @ApiResponse(
        responseCode = "200",
        description = "Field description",
        content = {
          @Content(
            mediaType = "application/json",
            schema = @Schema(implementation = Table.class))
        })
    })
  @GetMapping("simpleCsv")
  public Table simpleCsv() {
    return SIMPLE_CSV;
  }

  @Operation(
    operationId = "describeSpeciesListDownload",
    summary = "**Experimental.** Describes the fields present in a Species List format download")
  @ApiResponses(
    value = {
      @ApiResponse(
        responseCode = "200",
        description = "Field description",
        content = {
          @Content(
            mediaType = "application/json",
            schema = @Schema(implementation = Table.class))
        })
    })
  @GetMapping("speciesList")
  public Table speciesList() {
    return SPECIES_LIST;
  }

  @Operation(
    operationId = "describeSimpleAvroDownload",
    summary = "**Experimental.** Describes the fields present in a Simple Avro format download")
  @ApiResponses(
    value = {
      @ApiResponse(
        responseCode = "200",
        description = "Field description",
        content = {
          @Content(
            mediaType = "application/json",
            schema = @Schema(implementation = Table.class))
        })
    })
  @GetMapping("simpleAvro")
  public Table simpleAvro() {
    return SIMPLE_AVRO;
  }

  @Hidden // Not yet correct.
  @Operation(
    operationId = "describeSimpleWithVerbatimAvroDownload",
    summary = "**Experimental.** Describes the fields present in a Simple With Verbatim Avro format download")
  @ApiResponses(
    value = {
      @ApiResponse(
        responseCode = "200",
        description = "Field description",
        content = {
          @Content(
            mediaType = "application/json",
            schema = @Schema(implementation = Table.class))
        })
    })
  @GetMapping("simpleWithVerbatimAvro")
  public Table simpleWithVerbatimAvro() {
    return SIMPLE_WITH_VERBATIM_AVRO;
  }

  @Operation(
    operationId = "describeSimpleParquetDownload",
    summary = "**Experimental.** Describes the fields present in a Simple Parquet format download")
  @ApiResponses(
    value = {
      @ApiResponse(
        responseCode = "200",
        description = "Field description",
        content = {
          @Content(
            mediaType = "application/json",
            schema = @Schema(implementation = Table.class))
        })
    })
  @GetMapping("simpleParquet")
  public Table simpleParquet() {
    return SIMPLE_PARQUET;
  }

  @Operation(
    operationId = "describeSqlDownload",
    summary = "**Very experimental.** Describes the fields available for searching or download when using an SQL query.")
  @ApiResponses(
    value = {
      @ApiResponse(
        responseCode = "200",
        description = "Field description",
        content = {
          @Content(
            mediaType = "application/json",
            schema = @Schema(implementation = Table.class))
        })
    })
  @GetMapping("sql")
  public Table sql() {
    return SQL;
  }
}
