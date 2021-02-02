package org.apache.avro.file;

import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;

/**
 * Tool to split an Avro file into N chunks, preserving the schema.
 *
 * Usage: ParallelAvroSplitter filename outputFileFormat numberChunks
 */
public class ParallelAvroSplitter {

  public static void main(String... args) throws Exception{

    InputStream is;
    if (args[0].equals("-")) {
      is = System.in;
    } else {
      is = new FileInputStream(args[0]);
    }

    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
    ReflectDatumWriter<GenericContainer> rdw = new ReflectDatumWriter<>(GenericContainer.class);

    try (DataFileStream<GenericRecord> dfr = new DataFileStream(is, datumReader)) {

      int files = Integer.parseInt(args[2]);
      RawDataFileWriter<GenericContainer>[] dfws = new RawDataFileWriter[files];
      for (int i = 0; i < files; i++) {
        FileOutputStream output = new FileOutputStream(String.format(args[1], i));
        dfws[i] = new RawDataFileWriter<>(rdw);
        dfws[i].setCodec(CodecFactory.deflateCodec(6));
        dfws[i].setFlushOnEveryBlock(false);
        dfws[i].create(dfr.getSchema(), output);
      }

      int o = 0;
      while (dfr.hasNextBlock()) {
        DataFileStream.DataBlock nextBlockRaw = null;
        nextBlockRaw = dfr.nextRawBlock(nextBlockRaw);
        dfws[o%files].writeRawBlock(nextBlockRaw);

        o++;
      }

      for (RawDataFileWriter<GenericContainer> dfw : dfws) {
        dfw.close();
      }
    }
  }
}
