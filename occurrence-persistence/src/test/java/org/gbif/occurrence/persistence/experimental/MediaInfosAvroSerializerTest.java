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

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MediaInfosAvroSerializerTest {

  // Encodes cells the same way occurrence-species-multimedia-table does: raw Avro binary + Snappy.
  private static byte[] write(Schema arraySchema, int count, boolean withDatasetKey) throws Exception {
    Schema itemSchema = arraySchema.getElementType();
    GenericData.Array<GenericRecord> array = new GenericData.Array<>(count, arraySchema);
    for (int i = 0; i < count; i++) {
      GenericRecord rec = new GenericData.Record(itemSchema);
      rec.put("identifier", "https://example.org/" + i);
      rec.put("title", "title-" + i);
      rec.put("occurrenceKey", String.valueOf(i));
      rec.put("rightsHolder", "rh-" + i);
      rec.put("license", "CC0");
      if (withDatasetKey) {
        rec.put("datasetKey", "ds-" + i);
      }
      array.add(rec);
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
    new GenericDatumWriter<GenericData.Array<GenericRecord>>(arraySchema).write(array, encoder);
    encoder.flush();
    return Snappy.compress(baos.toByteArray());
  }

  @Test
  void readsV2CellsWithDatasetKey() throws Exception {
    byte[] cell = write(MediaInfosAvroSerializer.MEDIA_INFO_ARRAY_V2_SCHEMA, 3, true);

    List<Map<String, Object>> mediaInfos = MediaInfosAvroSerializer.deserializeMediaInfosV2Avro(cell);

    assertEquals(3, mediaInfos.size());
    for (int i = 0; i < 3; i++) {
      assertEquals("https://example.org/" + i, mediaInfos.get(i).get("identifier"));
      assertEquals("ds-" + i, mediaInfos.get(i).get("datasetKey"));
    }
  }

  @Test
  void readsLegacyV1CellsWithNullDatasetKey() throws Exception {
    byte[] cell = write(MediaInfosAvroSerializer.MEDIA_INFO_ARRAY_V1_SCHEMA, 3, false);

    List<Map<String, Object>> mediaInfos = MediaInfosAvroSerializer.deserializeMediaInfosV1Avro(cell);

    assertEquals(3, mediaInfos.size());
    for (int i = 0; i < 3; i++) {
      assertEquals("https://example.org/" + i, mediaInfos.get(i).get("identifier"));
      assertEquals("CC0", mediaInfos.get(i).get("license"));
      assertTrue(mediaInfos.get(i).containsKey("datasetKey"));
      assertNull(mediaInfos.get(i).get("datasetKey"));
    }
  }
}
