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
package org.gbif.occurrence.download.hive;

import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.pipelines.io.avro.extension.ac.AudubonTable;
import org.gbif.pipelines.io.avro.extension.dwc.ChronometricAgeTable;
import org.gbif.pipelines.io.avro.extension.dwc.IdentificationTable;
import org.gbif.pipelines.io.avro.extension.dwc.MeasurementOrFactTable;
import org.gbif.pipelines.io.avro.extension.dwc.ResourceRelationshipTable;
import org.gbif.pipelines.io.avro.extension.gbif.DnaDerivedDataTable;
import org.gbif.pipelines.io.avro.extension.gbif.IdentifierTable;
import org.gbif.pipelines.io.avro.extension.gbif.ImageTable;
import org.gbif.pipelines.io.avro.extension.gbif.MultimediaTable;
import org.gbif.pipelines.io.avro.extension.gbif.ReferenceTable;
import org.gbif.pipelines.io.avro.extension.germplasm.GermplasmAccessionTable;
import org.gbif.pipelines.io.avro.extension.germplasm.GermplasmMeasurementScoreTable;
import org.gbif.pipelines.io.avro.extension.germplasm.GermplasmMeasurementTraitTable;
import org.gbif.pipelines.io.avro.extension.germplasm.GermplasmMeasurementTrialTable;
import org.gbif.pipelines.io.avro.extension.ggbn.AmplificationTable;
import org.gbif.pipelines.io.avro.extension.ggbn.CloningTable;
import org.gbif.pipelines.io.avro.extension.ggbn.GelImageTable;
import org.gbif.pipelines.io.avro.extension.ggbn.LoanTable;
import org.gbif.pipelines.io.avro.extension.ggbn.MaterialSampleTable;
import org.gbif.pipelines.io.avro.extension.ggbn.PermitTable;
import org.gbif.pipelines.io.avro.extension.ggbn.PreparationTable;
import org.gbif.pipelines.io.avro.extension.ggbn.PreservationTable;
import org.gbif.pipelines.io.avro.extension.obis.ExtendedMeasurementOrFactTable;

import java.lang.reflect.InvocationTargetException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.avro.Schema;

import com.google.common.base.CaseFormat;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.reflect.Reflection;

import static org.gbif.occurrence.download.hive.HiveColumns.cleanDelimitersInitializer;

/**
 * Utility class used in the Freemarker template that generates  Hive tables for extensions.
 */
public class ExtensionTable {

  private static final TermFactory TERM_FACTORY = TermFactory.instance();

  private static final String EXT_PACKAGE = "org.gbif.pipelines.io.avro.extension";

  //Default fields
  public static final String DATASET_KEY_FIELD = "datasetkey";
  public static final String GBIFID_FIELD = "gbifid";

  private static final BiMap<String, Extension> EXTENSION_TABLES = ImmutableBiMap.<String, Extension>builder()
    //ac
    .put(AudubonTable.class.getName(), Extension.AUDUBON)
    //dwc
    .put(ChronometricAgeTable.class.getName(), Extension.CHRONOMETRIC_AGE)
    .put(IdentificationTable.class.getName(), Extension.IDENTIFICATION)
    .put(MeasurementOrFactTable.class.getName(), Extension.MEASUREMENT_OR_FACT)
    .put(ResourceRelationshipTable.class.getName(), Extension.RESOURCE_RELATIONSHIP)
    //gbif
    .put(DnaDerivedDataTable.class.getName(), Extension.DNA_DERIVED_DATA)
    .put(IdentifierTable.class.getName(), Extension.IDENTIFIER)
    .put(ImageTable.class.getName(), Extension.IMAGE)
    .put(MultimediaTable.class.getName(), Extension.MULTIMEDIA)
    .put(ReferenceTable.class.getName(), Extension.REFERENCE)
    //germplas
    .put(GermplasmAccessionTable.class.getName(), Extension.GERMPLASM_ACCESSION)
    .put(GermplasmMeasurementScoreTable.class.getName(), Extension.GERMPLASM_MEASUREMENT_SCORE)
    .put(GermplasmMeasurementTraitTable.class.getName(), Extension.GERMPLASM_MEASUREMENT_TRAIT)
    .put(GermplasmMeasurementTrialTable.class.getName(), Extension.GERMPLASM_MEASUREMENT_TRIAL)
    //ggbn
    .put(AmplificationTable.class.getName(), Extension.AMPLIFICATION)
    .put(CloningTable.class.getName(), Extension.CLONING)
    .put(GelImageTable.class.getName(), Extension.GEL_IMAGE)
    .put(LoanTable.class.getName(), Extension.LOAN)
    .put(MaterialSampleTable.class.getName(), Extension.MATERIAL_SAMPLE)
    .put(PermitTable.class.getName(), Extension.PERMIT)
    .put(PreparationTable.class.getName(), Extension.PREPARATION)
    .put(PreservationTable.class.getName(), Extension.PRESERVATION)
    //obis
    .put(ExtendedMeasurementOrFactTable.class.getName(), Extension.EXTENDED_MEASUREMENT_OR_FACT)
    .build();

  //Simple class name
  private final String simpleClassName;

  //fully qualified class name
  private final String className;

  //Section used to distinguish class names in the same packages
  private final String leafNamespace;

  private final Schema schema;

  private final Term term;

  public static Set<Extension> getSupportedExtensions() {
    return EXTENSION_TABLES.values();
  }

  public static List<ExtensionTable> tableExtensions() {
      return EXTENSION_TABLES.values()
        .stream()
        .map(ExtensionTable::new)
        .collect(Collectors.toList());
  }

  public ExtensionTable(Extension extension) {
    className = EXTENSION_TABLES.inverse().get(extension);
    if (Objects.isNull(className)) {
      throw new IllegalArgumentException("Extension " + extension + " not supported");
    }
    String packageName = Reflection.getPackageName(className);
    simpleClassName = className.substring(packageName.length() + 1);
    leafNamespace = packageName.replace(EXT_PACKAGE + '.', "").replace('.', '_');
    schema = loadAvroSchema();
    term = TERM_FACTORY.findTerm(extension.getRowType());
  }

  public Extension getExtension() {
    return EXTENSION_TABLES.get(className);
  }

  private String getLeafNamespace() {
    return leafNamespace;
  }

  public String getHiveTableName() {
    return leafNamespace + '_' + simpleClassName.toLowerCase().replace("table", "");
  }

  public String getDirectoryTableName() {
    return simpleClassName.toLowerCase();
  }

  public String getAvroSchemaFileName() {
    return leafNamespace + '_' + CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, simpleClassName) + ".avsc";
  }

  private String columnName(Schema.Field field) {
    return HiveColumns.escapeColumnName(field.name());
  }

  private String initializer(Schema.Field field) {
    String fieldName = field.name();
    String hiveColumn = columnName(field);
    if (fieldName.equalsIgnoreCase(GBIFID_FIELD) || fieldName.equalsIgnoreCase(DATASET_KEY_FIELD)) {
      return hiveColumn;
    } else {
      return cleanDelimitersInitializer(hiveColumn);
    }
  }

  public List<String> getFields() {
    return schema.getFields().stream().map(this::initializer).collect(Collectors.toList());
  }

  public Schema getSchema() {
    return schema;
  }

  public Term getTerm() {
    return term;
  }

  public Schema loadAvroSchema() {
    try {
      return (Schema) getClass().getClassLoader()
        .loadClass(className)
        .getDeclaredMethod("getClassSchema")
        .invoke(null, null);
    } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
      throw new RuntimeException(ex);
    }
  }

  public Set<String> getInterpretedFields() {
    Set<String> interpretedFields = new LinkedHashSet<>();
    interpretedFields.add(GBIFID_FIELD);
    interpretedFields.add(DATASET_KEY_FIELD);
    interpretedFields.addAll(schema.getFields()
                               .stream()
                               .map(Schema.Field::name)
                               .filter(fieldName -> !fieldName.startsWith("v_")
                                                    && !fieldName.equalsIgnoreCase(GBIFID_FIELD)
                                                    && !fieldName.equalsIgnoreCase(DATASET_KEY_FIELD))
                               .collect(Collectors.toSet()));
    return interpretedFields;
  }

  public List<Term> getInterpretedFieldsAsTerms() {
    return getInterpretedFields().stream().map(TERM_FACTORY::findPropertyTerm).collect(Collectors.toList());
  }

  public Set<String> getVerbatimFields() {
    return schema.getFields()
      .stream()
      .map(Schema.Field::name)
      .filter(field -> field.startsWith("v_"))
      .collect(Collectors.toSet());
  }

}
