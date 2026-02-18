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

import static org.gbif.occurrence.download.hive.AvroDataTypes.avroField;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

/** Utility class to generate an Avro scheme from the Event HDFS Table schema. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class EventAvroHdfsTableDefinition {

  /** Generates an Avro Schema based on the Event HDFS table. */
  public static Schema avroDefinition() {
    SchemaBuilder.FieldAssembler<Schema> builder =
        SchemaBuilder.record("EventHdfsRecord").namespace("org.gbif.pipelines.io.avro").fields();
    EventHDFSTableDefinition.definition()
        .forEach(initializableField -> avroField(builder, initializableField));
    return builder.endRecord();
  }

  public static void main(String[] args) {
    System.out.println(avroDefinition().toString(true));
  }
}
