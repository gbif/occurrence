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
package org.gbif.occurrence.download.file.simpleavro;

import org.gbif.occurrence.download.file.common.DownloadFileUtils;
import org.gbif.occurrence.download.action.DownloadWorkflowModule;
import org.gbif.utils.file.properties.PropertiesUtil;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class that creates a single Avro file from a directory that stores Avro data (of a Hive table or search queries).
 */
public class SimpleAvroArchiveBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleAvroArchiveBuilder.class);

  // Occurrences file name
  private static final String AVRO_EXTENSION = ".avro";

  /**
   * Merges the content of sourceFS:sourcePath into targetFS:outputPath in a file called downloadKey.avro.
   */
  public static void mergeToSingleAvro(final FileSystem sourceFS, FileSystem targetFS, String sourcePath,
                                       String targetPath, String downloadKey) throws IOException {

    Path outputPath = new Path(targetPath, downloadKey + AVRO_EXTENSION);

    ReflectDatumWriter<GenericContainer> rdw = new ReflectDatumWriter<>(GenericContainer.class);
    ReflectDatumReader<GenericContainer> rdr = new ReflectDatumReader<>(GenericContainer.class);
    boolean first = false;

    try (
      FSDataOutputStream zipped = targetFS.create(outputPath, true);
      DataFileWriter<GenericContainer> dfw = new DataFileWriter<>(rdw)
    ) {

      final Path inputPath = new Path(sourcePath);

      FileStatus[] hdfsFiles = sourceFS.listStatus(inputPath);

      for (FileStatus fs : hdfsFiles) {
        try(InputStream is = sourceFS.open(fs.getPath());
            DataFileStream<GenericContainer> dfs = new DataFileStream<>(is, rdr)) {
          if (!first) {
            dfw.setCodec(CodecFactory.deflateCodec(-1));
            dfw.setFlushOnEveryBlock(false);
            dfw.create(dfs.getSchema(), zipped);
            first = true;
          }

          dfw.appendAllFrom(dfs, false);
        }

      }

      dfw.flush();
      dfw.close();
      zipped.flush();

    } catch (Exception ex) {
      LOG.error("Error combining Avro files", ex);
      throw new RuntimeException(ex);
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
