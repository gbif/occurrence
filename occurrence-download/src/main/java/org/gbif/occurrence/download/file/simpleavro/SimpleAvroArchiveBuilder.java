package org.gbif.occurrence.download.file.simpleavro;

import com.google.common.base.Throwables;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.occurrence.download.file.common.DownloadFileUtils;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
import org.gbif.utils.file.properties.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Utility class that creates a single Avro file from a directory that stores Avro data (of a Hive table or SOLR queries).
 */
public class SimpleAvroArchiveBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleAvroArchiveBuilder.class);

  // Occurrences file name
  private static final String AVRO_EXTENSION = ".avro";

  /**
   * Merges the content of sourceFS:sourcePath into targetFS:outputPath in a file called downloadKey.avro.
   */
  public static void mergeToSingleAvro(
    final FileSystem sourceFS,
    FileSystem targetFS,
    String sourcePath,
    String targetPath,
    String downloadKey
  ) throws IOException {

    Path outputPath = new Path(targetPath, downloadKey + AVRO_EXTENSION);

    ReflectDatumWriter<GenericContainer> rdw = new ReflectDatumWriter<>(GenericContainer.class);
    ReflectDatumReader<GenericContainer> rdr = new ReflectDatumReader<>(GenericContainer.class);
    boolean first = false;

    try (
      FSDataOutputStream zipped = targetFS.create(outputPath, true);
      DataFileWriter<GenericContainer> dfw = new DataFileWriter(rdw)
    ) {

      final Path inputPath = new Path(sourcePath);

      FileStatus[] hdfsFiles = sourceFS.listStatus(inputPath);

      for (FileStatus fs : hdfsFiles) {
        InputStream is = sourceFS.open(fs.getPath());
        DataFileStream<GenericContainer> dfs = new DataFileStream(is, rdr);

        if (!first) {
          dfw.setCodec(CodecFactory.deflateCodec(-1));
          dfw.setFlushOnEveryBlock(false);
          dfw.create(dfs.getSchema(), zipped);
          first = true;
        }

        dfw.appendAllFrom(dfs, false);
      }

      dfw.flush();
      dfw.close();
      zipped.flush();

    } catch (Exception ex) {
      LOG.error("Error combining Avro files", ex);
      throw Throwables.propagate(ex);
    }
  }

  /**
   * Executes the archive creation process.
   * The expected parameters are:
   * 0. sourcePath: HDFS path to the directory that contains the data files.
   * 1. targetPath: HDFS path where the resulting file will be copied.
   * 2. downloadKey: occurrence download key.
   */
  public static void main(String[] args) throws IOException {
    Properties properties = PropertiesUtil.loadProperties(DownloadWorkflowModule.CONF_FILE);
    FileSystem sourceFileSystem = DownloadFileUtils.getHdfs(properties.getProperty(DownloadWorkflowModule.DefaultSettings.NAME_NODE_KEY));
    mergeToSingleAvro(sourceFileSystem,
                      sourceFileSystem,
                      args[0],
                      args[1],
                      args[2]);
  }

  /**
   * Private constructor.
   */
  private SimpleAvroArchiveBuilder() {
    //do nothing
  }
}
