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

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.gbif.api.model.common.search.SearchResponse;
import org.gbif.api.vocabulary.Country;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.IucnTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.occurrence.common.HiveColumnsUtils;
import org.gbif.occurrence.common.TermUtils;
import org.gbif.occurrence.download.hive.AvroQueries;
import org.gbif.occurrence.download.hive.DownloadTerms;
import org.gbif.occurrence.download.hive.HiveQueries;
import org.gbif.occurrence.download.hive.InitializableField;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.springframework.http.MediaType;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.collect.ImmutableSet;

import lombok.Builder;
import lombok.Data;

/**
 * Resource to describe file/table formats use in GBIF occurrence downloads.
 * Project specific exports are not handled by this resource: DownloadFormat.BIONOMIA; DownloadFormat.IUCN, DownloadFormat.MAP_OF_LIFE.
 */
@Tag(
  name = "Occurrence download formats",
  description = "This API lists the fields present in the various download formats, their data types and their term identifiers.")
@RestController
@Validated
@RequestMapping(produces = {MediaType.APPLICATION_JSON_VALUE,
  "application/x-javascript"}, value = "occurrence/download/describe")
public class OccurrenceDownloadDescribeResource {

  //Required fields in downloads
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
    if (TermUtils.isInterpretedUtcDate(term)) {
      return "yyyy-MM-ddTHH:mm:ssZ";
    } else if(TermUtils.isInterpretedLocalDate(term)) {
      return "yyyy-MM-ddTHH:mm:ss";
    } else if( HiveColumnsUtils.isHiveArray(term)) {
      return "Array elements are delimited by ;";
    }
    return null;
  }

  /**
   * Transforms a InitializableField to Field.
   */
  private static Field toTableField(InitializableField initializableField, boolean interpreted) {
    Field.FieldBuilder builder = Field.builder()
                                  .name(initializableField.getHiveField())
                                  .type(interpreted? fieldType(initializableField.getTerm()) : initializableField.getHiveDataType())
                                  .term(initializableField.getTerm())
                                  .required(REQUIRED_FIELD.contains(initializableField.getTerm()));
    if (interpreted) {
      builder.typeFormat(fieldTypeFormat(initializableField.getTerm()));
    }
    return builder.build();
  }

  /**
   * Transforms Map<String,InitializableField> queryFields into a List<Field>.
   */
  private static List<Field> toFieldList(Map<String,InitializableField> queryFields, boolean interpreted) {
    return queryFields.values().stream()
      .map(initializableField -> toTableField(initializableField, interpreted))
      .collect(Collectors.toList());
  }

  private static final HiveQueries HIVE_QUERIES = new HiveQueries();
  private static final AvroQueries AVRO_QUERIES = new AvroQueries();

  /**
   * Field description of file or download table.
   */
  @Data
  @Builder
  public static class Field {
    private final String name;
    private final String type;
    private String typeFormat;
    private final Term term;
    private boolean required;
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
   * DwcA download description.
   */
  @Data
  public static class DwcDownload {

    private final Table verbatim = Table.builder()
                                     .fields(toFieldList(HIVE_QUERIES.selectVerbatimFields(), false))
                                     .build();

    private final Table interpreted = Table.builder()
                                        .fields(toFieldList(HIVE_QUERIES.selectInterpretedFields(false), true))
                                        .build();
  }

  //Static cached definitions
  private static final DwcDownload DWC_DOWNLOAD = new DwcDownload();

  private static final Table SIMPLE_CSV = Table.builder()
                                            .fields(toFieldList(HIVE_QUERIES.selectSimpleDownloadFields(false), true))
                                            .build();

  private static final Table SPECIES_LIST = Table.builder()
    .fields(DownloadTerms.SPECIES_LIST_DOWNLOAD_TERMS
              .stream().map(p -> Field.builder()
                                  .type(fieldType(p.getRight()))
                                  .typeFormat(fieldTypeFormat(p.getRight()))
                                  .term(p.getRight())
                                  .name(HiveColumnsUtils.getHiveQueryColumn(p.getRight()))
                                  .required(GbifTerm.taxonKey.equals(p.getRight()))
                                  .build())
              .filter(f -> f.getTerm() != IucnTerm.iucnRedListCategory)
              .collect(Collectors.toList()))
      .build();

  private static final Table SIMPLE_AVRO = Table.builder()
                                            .fields(toFieldList(AVRO_QUERIES.selectSimpleDownloadFields(false), true))
                                            .build();

  private static final Table SIMPLE_WITH_VERBATIM_AVRO = Table.builder()
                                                          .fields(toFieldList(AVRO_QUERIES.simpleWithVerbatimAvroQueryFields(false), true))
                                                          .build();

  //End of static cached definitions

  @Operation(
    operationId = "describeDwcaDownload",
    summary = "Describes the fields present in a Darwin Core Archive format download")
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
    summary = "Describes the fields present in a Simple CSV format download")
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
    summary = "Describes the fields present in a Species List format download")
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
    summary = "Describes the fields present in a Simple Avro format download")
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

  @Operation(
    operationId = "describeSimpleWithVerbatimAvroDownload",
    summary = "Describes the fields present in a Simple With Verbatim Avro format download")
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
    summary = "Describes the fields present in a Simple Parquet format download")
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
    return SIMPLE_CSV;
  }
}
