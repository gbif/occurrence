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
import java.lang.reflect.InvocationTargetException;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.ToString;
import org.apache.avro.Schema;

import com.google.common.base.CaseFormat;
import com.google.common.collect.BiMap;

import static org.gbif.occurrence.download.hive.HiveColumns.cleanDelimitersInitializer;

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
    this.extension = extension;
    schema = EXTENSION_TABLES.get(extension);
    leafNamespace = schema.getNamespace().replace(EXT_PACKAGE + '.', "").replace('.', '_');
    term = TERM_FACTORY.findTerm(extension.getRowType());
  }

  public Extension getExtension() {
    return extension;
  }

  private String getLeafNamespace() {
    return leafNamespace;
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

  public static void main(String[] args) {
    tableExtensions().forEach(t -> System.out.println(t.toString()));
  }
}
