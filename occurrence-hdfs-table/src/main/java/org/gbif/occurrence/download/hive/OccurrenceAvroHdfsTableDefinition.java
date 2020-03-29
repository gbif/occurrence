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
        builder.name(initializableField.getHiveField()).type().nullable().intType().noDefault();
        break;
      case HiveDataTypes.TYPE_BIGINT:
        if (initializableField.getHiveField().equalsIgnoreCase("gbifid")) {
          builder.name(initializableField.getHiveField()).type().longType().noDefault();
        } else {
          builder.name(initializableField.getHiveField()).type().nullable().longType().noDefault();
        }
        break;
      case HiveDataTypes.TYPE_BOOLEAN:
        builder.name(initializableField.getHiveField()).type().nullable().booleanType().noDefault();
        break;
      case HiveDataTypes.TYPE_DOUBLE:
        builder.name(initializableField.getHiveField()).type().nullable().doubleType().noDefault();
        break;
      case HiveDataTypes.TYPE_ARRAY_STRING:
        builder.name(initializableField.getHiveField()).type().nullable().array().items().nullable().stringType().noDefault();
        break;
      default:
        builder.name(initializableField.getHiveField()).type().nullable().stringType().noDefault();
        break;
    }
  }
}
