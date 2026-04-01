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

  // Define schema once (can also load from a .avsc file)
  private static final String MEDIA_INFO_SCHEMA_JSON = """
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

  private static final String MEDIA_INFO_ARRAY_SCHEMA_JSON =
      "{\"type\": \"array\", \"items\": " + MEDIA_INFO_SCHEMA_JSON + "}";

  private static final Schema MEDIA_INFO_ARRAY_SCHEMA = new Schema.Parser().parse(MEDIA_INFO_ARRAY_SCHEMA_JSON);

  // Reader (on the API side)
  public static List<Map<String,Object>> deserializeMediaInfosAvro(byte[] data) throws IOException {
    byte[] avroBytes = Snappy.uncompress(data);
    GenericDatumReader<Array<GenericRecord>> reader =
        new GenericDatumReader<>(MEDIA_INFO_ARRAY_SCHEMA);
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
