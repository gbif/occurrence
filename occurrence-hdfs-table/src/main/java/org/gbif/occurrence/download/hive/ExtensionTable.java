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
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.avro.Schema;

import com.google.common.base.CaseFormat;

import lombok.ToString;

import static org.gbif.occurrence.download.hive.HiveColumns.cleanDelimitersInitializer;
import static org.gbif.occurrence.download.hive.HiveColumns.hiveColumnName;

/**
 * Utility class used in the Freemarker template that generates  Hive tables for extensions.
 */
@ToString
public class ExtensionTable {

  private static final TermFactory TERM_FACTORY = TermFactory.instance();

  private static final String EXT_PACKAGE = "org.gbif.pipelines.io.avro.extension";

  //Default fields
  public static final String DATASET_KEY_FIELD = "datasetkey";
  public static final String GBIFID_FIELD = "gbifid";

  private static final Map<Extension,Schema> EXTENSION_TABLES = ExtensionSchemasLoader.extensionTablesMap();

  //Section used to distinguish class names in the same packages
  private final String leafNamespace;

  private final Schema schema;

  private final Extension extension;

  private final Term term;

  public static Set<Extension> getSupportedExtensions() {
    return EXTENSION_TABLES.keySet();
  }

  public static List<ExtensionTable> tableExtensions() {
      return EXTENSION_TABLES.keySet()
        .stream()
        .map(ExtensionTable::new)
        .collect(Collectors.toList());
  }

  public ExtensionTable(Extension extension) {
    schema = EXTENSION_TABLES.get(extension);
    if (Objects.isNull(schema)) {
      throw new IllegalArgumentException("Extension " + extension + " not supported");
    }
    this.extension = extension;
    leafNamespace = schema.getNamespace().replace(EXT_PACKAGE + '.', "").replace('.', '_');
    term = TERM_FACTORY.findTerm(extension.getRowType());
  }

  public Extension getExtension() {
    return extension;
  }

  public String getHiveTableName() {
    return leafNamespace + '_' + schema.getName().toLowerCase().replace("table", "");
  }

  public String getDirectoryTableName() {
    return schema.getName().toLowerCase();
  }

  public String getAvroSchemaFileName() {
    return leafNamespace + '_' + CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, schema.getName()) + ".avsc";
  }

  private String initializer(Schema.Field field) {
    String fieldName = field.name();
    String hiveColumn = HiveColumns.hiveColumnName(field.name());
    if (fieldName.equalsIgnoreCase(GBIFID_FIELD) || fieldName.equalsIgnoreCase(DATASET_KEY_FIELD)) {
      return hiveColumn;
    } else {
      return cleanDelimitersInitializer(field.name(), hiveColumn);
    }
  }

  public List<String> getFieldNames() {
    return schema.getFields().stream().map(f -> hiveColumnName(f.name())).collect(Collectors.toList());
  }

  public List<String> getFieldInitializers() {
    return schema.getFields().stream().map(this::initializer).collect(Collectors.toList());
  }

  public Schema getSchema() {
    return schema;
  }

  public Term getTerm() {
    return term;
  }

  public Set<String> getInterpretedFields() {
    Set<String> interpretedFields = new LinkedHashSet<>();
    interpretedFields.add(GBIFID_FIELD);
    interpretedFields.add(DATASET_KEY_FIELD);
    interpretedFields.addAll(schema.getFields()
                               .stream()
                               .map(f -> hiveColumnName(f.name()))
                               .filter(fieldName -> !fieldName.startsWith("v_")
                                                    && !fieldName.equalsIgnoreCase(GBIFID_FIELD)
                                                    && !fieldName.equalsIgnoreCase(DATASET_KEY_FIELD))
                               .collect(Collectors.toList()));
    return interpretedFields;
  }

  public List<Term> getInterpretedFieldsAsTerms() {
    List<Term> interpretedFields = new ArrayList<>();
    interpretedFields.add(GbifTerm.gbifID);
    interpretedFields.add(GbifTerm.datasetKey);
    interpretedFields.addAll(schema.getFields()
      .stream()
      .filter(field -> !field.name().startsWith("v_")
        && !field.name().equalsIgnoreCase(GBIFID_FIELD)
        && !field.name().equalsIgnoreCase(DATASET_KEY_FIELD))
        .map(f -> TERM_FACTORY.findPropertyTerm(f.doc()))
        .collect(Collectors.toList()));
    return interpretedFields;
  }

  public Set<String> getVerbatimFields() {
    Set<String> verbatimFields = new LinkedHashSet<>();
    verbatimFields.add(GBIFID_FIELD);
    verbatimFields.add(DATASET_KEY_FIELD);
    verbatimFields.addAll(schema.getFields()
      .stream()
      .map(f -> hiveColumnName(f.name()))
      .filter(fieldName -> fieldName.startsWith("v_")
        || fieldName.equalsIgnoreCase(GBIFID_FIELD)
        || fieldName.equalsIgnoreCase(DATASET_KEY_FIELD))
      .collect(Collectors.toSet()));
    return verbatimFields;
  }

  public static void main(String[] args) {
    tableExtensions().forEach(t -> System.out.println(t.toString()));
  }
}
