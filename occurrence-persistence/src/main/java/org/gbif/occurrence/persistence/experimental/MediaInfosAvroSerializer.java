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
package org.gbif.occurrence.persistence.experimental;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.xerial.snappy.Snappy;

public class MediaInfosAvroSerializer {

  // Cells are raw Avro binary without an embedded writer schema, so the schema each cell was written
  // with is implied by its HBase qualifier (see OccurrenceSpeciesMultimediaServiceImpl).
  // v1 ("media_infos"): original fields.
  private static final String MEDIA_INFO_V1_SCHEMA_JSON = """
    {
      "type": "record",
      "name": "MediaInfo",
      "fields": [
        {"name": "identifier",   "type": ["null", "string"], "default": null},
        {"name": "title",        "type": ["null", "string"], "default": null},
        {"name": "occurrenceKey","type": ["null", "string"], "default": null},
        {"name": "rightsHolder", "type": ["null", "string"], "default": null},
        {"name": "license",      "type": ["null", "string"], "default": null}
      ]
    }
    """;

  // v2 ("media_infos_v2"): adds datasetKey.
  private static final String MEDIA_INFO_V2_SCHEMA_JSON = """
    {
      "type": "record",
      "name": "MediaInfo",
      "fields": [
        {"name": "identifier",   "type": ["null", "string"], "default": null},
        {"name": "title",        "type": ["null", "string"], "default": null},
        {"name": "occurrenceKey","type": ["null", "string"], "default": null},
        {"name": "rightsHolder", "type": ["null", "string"], "default": null},
        {"name": "license",      "type": ["null", "string"], "default": null},
        {"name": "datasetKey",   "type": ["null", "string"], "default": null}
      ]
    }
    """;

  static final Schema MEDIA_INFO_ARRAY_V1_SCHEMA = arraySchema(MEDIA_INFO_V1_SCHEMA_JSON);
  static final Schema MEDIA_INFO_ARRAY_V2_SCHEMA = arraySchema(MEDIA_INFO_V2_SCHEMA_JSON);

  private static Schema arraySchema(String itemSchemaJson) {
    return new Schema.Parser().parse("{\"type\": \"array\", \"items\": " + itemSchemaJson + "}");
  }

  /** Reads a v1 ("media_infos") cell; datasetKey is returned as null. */
  public static List<Map<String,Object>> deserializeMediaInfosV1Avro(byte[] data) throws IOException {
    return deserialize(data, MEDIA_INFO_ARRAY_V1_SCHEMA);
  }

  /** Reads a v2 ("media_infos_v2") cell. */
  public static List<Map<String,Object>> deserializeMediaInfosV2Avro(byte[] data) throws IOException {
    return deserialize(data, MEDIA_INFO_ARRAY_V2_SCHEMA);
  }

  // Reader (on the API side). Always resolves to the latest schema so the response shape is stable.
  private static List<Map<String,Object>> deserialize(byte[] data, Schema writerSchema) throws IOException {
    byte[] avroBytes = Snappy.uncompress(data);
    GenericDatumReader<Array<GenericRecord>> reader =
        new GenericDatumReader<>(writerSchema, MEDIA_INFO_ARRAY_V2_SCHEMA);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(avroBytes, null);
    Array<GenericRecord> array = reader.read(null, decoder);
    return array.stream().map(MediaInfosAvroSerializer::genericRecordToMap).toList();
  }

  public static Map<String, Object> genericRecordToMap(GenericRecord record) {
    if (record == null) return null;
    Map<String, Object> map = new LinkedHashMap<>();
    for (Schema.Field f : record.getSchema().getFields()) {
      Object raw = record.get(f.name());
      map.put(f.name(), convertAvroObject(raw));
    }
    return map;
  }

  @SuppressWarnings("unchecked")
  private static Object convertAvroObject(Object o) {
    if (o == null) return null;
    if (o instanceof Utf8) return o.toString();               // Utf8 -> String
    if (o instanceof org.apache.avro.util.Utf8) return o.toString();
    if (o instanceof CharSequence) return o.toString();
    if (o instanceof GenericRecord) return genericRecordToMap((GenericRecord) o);
    if (o instanceof GenericData.EnumSymbol) return o.toString();
    if (o instanceof GenericData.Fixed) return ((GenericData.Fixed) o).bytes(); // or Base64 encode if needed
    if (o instanceof ByteBuffer) {                            // convert bytebuffer to byte[] or base64
      ByteBuffer bb = (ByteBuffer) o;
      byte[] bytes = new byte[bb.remaining()];
      bb.get(bytes);
      return bytes; // or Base64.getEncoder().encodeToString(bytes)
    }
    if (o instanceof GenericData.Array) {
      List<Object> list = new ArrayList<>();
      for (Object item : (GenericData.Array<?>) o) {
        list.add(convertAvroObject(item));
      }
      return list;
    }
    if (o instanceof Map) {
      Map<Object, Object> result = new LinkedHashMap<>();
      Map<Object, Object> rawMap = (Map<Object, Object>) o;
      for (Map.Entry<Object, Object> e : rawMap.entrySet()) {
        result.put(convertAvroObject(e.getKey()), convertAvroObject(e.getValue()));
      }
      return result;
    }
    // primitives (Integer, Long, Double, Boolean, String, etc.)
    return o;
  }


}
