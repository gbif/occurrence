package org.apache.avro.file;

import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;

/**
 * Splits an Avro file into chunks of a particular length (approximately), and sends them to a
 * consumer.  The schema is preserved.
 *
 * The operation is done serially, as it is expected that the consumer is slower than this producer.
 */
public class SerialAvroSplitter {
  private static final Logger LOG = LoggerFactory.getLogger(SerialAvroSplitter.class);

  private static final long DEFAULT_CHUNK_SIZE = 100_000_000; // 100 MB

  private final InputStream inputStream;
  private final Consumer<File> fileConsumer;
  private final long chunkSize;

  public SerialAvroSplitter(InputStream inputStream, Consumer<File> fileConsumer) {
    this.inputStream = inputStream;
    this.fileConsumer = fileConsumer;
    this.chunkSize = DEFAULT_CHUNK_SIZE;
  }

  public void split() throws IOException {

    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
    ReflectDatumWriter<GenericContainer> rdw = new ReflectDatumWriter<>(GenericContainer.class);
    File output = null;
    RawDataFileWriter<GenericContainer> dfw = null;

    int blockCount = 0;
    int fileCount = 0;

    LOG.debug("Starting to read input stream");
    try (DataFileStream<GenericRecord> dfr = new DataFileStream(inputStream, datumReader)) {

      while (dfr.hasNextBlock()) {
        if (blockCount == 0 || output.length() > chunkSize) {
          if (dfw != null) {
            dfw.close();
            LOG.debug("Completed file {} of length {}B, passing to consumer", output, output.length());
            fileConsumer.accept(output);
            LOG.debug("File {} consumed, deleting", output);
            output.delete();
          }

          // Start a new file
          fileCount++;
          output = File.createTempFile("avro-splitter-"+fileCount+"-", ".avro");
          output.deleteOnExit();
          LOG.debug("Copying Avro data to new file {}", output.getAbsolutePath());

          dfw = new RawDataFileWriter<>(rdw);
          dfw.setCodec(CodecFactory.deflateCodec(8)); // TODO: Configure compression?
          dfw.setFlushOnEveryBlock(false);
          dfw.create(dfr.getSchema(), output);
        }

        DataFileStream.DataBlock nextBlockRaw = null;
        nextBlockRaw = dfr.nextRawBlock(nextBlockRaw);
        dfw.writeRawBlock(nextBlockRaw);

        blockCount++;
      }

      dfw.close();
      LOG.debug("Completed file {} of length {}B, passing to consumer", output, output.length());
      fileConsumer.accept(output);
      LOG.debug("File {} consumed, deleting", output);
      output.delete();

      LOG.info("Input was split to {} files with {} blocks", fileCount, blockCount);
    }
  }
}
