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

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

/**
 * Utility class to generate an Avro scheme from the Occurrence HDFS Table schema.
 */
public class OccurrenceAvroHdfsTableDefinition {

  /**
   * Private constructor.
   */
  private OccurrenceAvroHdfsTableDefinition() {
    //DO NOTHING
  }

  /**
   * Generates an Avro Schema based on the Occurrence HDFS table.
   */
  public static Schema avroDefinition() {
    SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder
      .record("OccurrenceHdfsRecord")
      .namespace("org.gbif.pipelines.io.avro").fields();
    OccurrenceHDFSTableDefinition.definition().forEach(initializableField -> avroField(builder, initializableField));
    return builder.endRecord();
  }

  /**
   * Generates an Avro field according to the Hive data type
   */
  public static void avroField(SchemaBuilder.FieldAssembler<Schema> builder, InitializableField initializableField) {
    switch (initializableField.getHiveDataType()) {
      case HiveDataTypes.TYPE_INT:
        builder.name(initializableField.getColumnName()).type().nullable().intType().noDefault();
        break;
      case HiveDataTypes.TYPE_BIGINT:
        builder.name(initializableField.getColumnName()).type().nullable().longType().noDefault();
        break;
      case HiveDataTypes.TYPE_BOOLEAN:
        builder.name(initializableField.getColumnName()).type().nullable().booleanType().noDefault();
        break;
      case HiveDataTypes.TYPE_DOUBLE:
        builder.name(initializableField.getColumnName()).type().nullable().doubleType().noDefault();
        break;
      case HiveDataTypes.TYPE_ARRAY_STRING:
        builder.name(initializableField.getColumnName()).type().nullable().array().items().nullable().stringType().noDefault();
        break;
      case HiveDataTypes.TYPE_ARRAY_INT:
        builder.name(initializableField.getColumnName()).type().nullable().array().items().nullable().intType().noDefault();
        break;
      case HiveDataTypes.TYPE_MAP_STRUCT:
        builder.name(initializableField.getColumnName()).type().nullable().map().values().array().items().stringType().noDefault();
        break;
      case HiveDataTypes.TYPE_MAP_OF_MAP_STRUCT:
        builder.name(initializableField.getColumnName()).type().nullable().map().values().map().values().stringType().noDefault();
        break;
      case HiveDataTypes.TYPE_VOCABULARY_STRUCT:
        builder.name(initializableField.getColumnName()).type().nullable().record(getTypeRecordName(initializableField))
          .fields()
          .requiredString("concept")
          .name("lineage").type().nullable().array().items().nullable().stringType().noDefault()
          .endRecord()
        .noDefault();
        break;
      case HiveDataTypes.TYPE_VOCABULARY_ARRAY_STRUCT:
        builder.name(initializableField.getColumnName()).type().nullable().record(getTypeRecordName(initializableField))
          .fields()
          .name("concepts").type().array().items().nullable().stringType().noDefault()
          .name("lineage").type().nullable().array().items().nullable().stringType().noDefault()
          .endRecord()
          .noDefault();
        break;
      case  HiveDataTypes.TYPE_ARRAY_PARENT_STRUCT:
        builder.name(initializableField.getColumnName()).type().nullable().array().items().nullable().record(getTypeRecordName(initializableField))
          .fields()
          .requiredString("id")
          .optionalString("eventtype")
          .endRecord()
          .noDefault();
        break;
      case HiveDataTypes.GEOLOGICAL_RANGE_STRUCT:
        builder.name(initializableField.getColumnName()).type().nullable().record(getTypeRecordName(initializableField))
          .fields()
          .requiredFloat("gt")
          .requiredFloat("lte")
          .endRecord()
          .noDefault();
        break;
      default:
        if (initializableField.getColumnName().equalsIgnoreCase("gbifid")) {
          builder.name(initializableField.getColumnName()).type().stringType().noDefault();
        } else if (!initializableField.getColumnName().equalsIgnoreCase("humboldttargettaxonclassifications")) {
          // ignore humboldtItem since this is populated from the ext_humboldt json field
          builder.name(initializableField.getColumnName()).type().nullable().stringType().noDefault();
        }
        break;
    }
  }

  /**
   * Extract the term name and transforms it into Upper camel-case format.
   */
  private static String getTypeRecordName(InitializableField initializableField) {
    return initializableField.getTerm().simpleName().substring(0,1).toUpperCase() + initializableField.getTerm().simpleName().substring(1);
  }

  public static void main(String[] args) {
    System.out.println(avroDefinition().toString(true));
  }
}
