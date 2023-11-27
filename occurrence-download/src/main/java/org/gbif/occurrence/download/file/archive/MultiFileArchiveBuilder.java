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
package org.gbif.occurrence.download.file.archive;

import org.gbif.hadoop.compress.d2.D2CombineInputStream;
import org.gbif.hadoop.compress.d2.D2Utils;
import org.gbif.hadoop.compress.d2.zip.ModalZipOutputStream;
import org.gbif.hadoop.compress.d2.zip.ZipEntry;
import org.gbif.occurrence.download.file.common.DownloadFileUtils;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
import org.gbif.utils.file.properties.PropertiesUtil;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.zip.ZipOutputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;

import static org.gbif.occurrence.download.file.d2.D2Utils.copyToCombinedStream;
import static org.gbif.occurrence.download.file.d2.D2Utils.setDataFromInputStream;

/**
 * Utility class that creates a Zip file from one or more directories containing data of one or more Hive tables.
 *
 * Each directory of data files is combined into a single entry in the Zip file.
 * See MultiDirectoryArchiveBuilder for the alternative.
 *
 * TODO: citation file.
 */
public class MultiFileArchiveBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(MultiFileArchiveBuilder.class);

  private static final String ZIP_EXTENSION = ".zip";

  private static final String ERROR_ZIP_MSG = "Error creating zip file";
  // Header file is named '0' to appear first when listing the content of the directory.
  private static final String HEADER_FILE_NAME = "0";

  private final List<ZipEntrySource> sources;

  /**
   * Data structure representing a Hive table (or other location) → Zip file entry
   */
  private class ZipEntrySource {
    String name;
    String header;
    String path;
  }

  /**
   * Set the Zip file entries
   */
  public static MultiFileArchiveBuilder withEntries(String... sources) {
    return new MultiFileArchiveBuilder(sources);
  }

  /**
   * Merge the content of source entries on the sourceFS into targetFS:outputPath in a file called downloadKey.zip.
   */
  public void mergeAllToZip(final FileSystem sourceFS, FileSystem targetFS, String targetPath, String downloadKey,
                            ModalZipOutputStream.MODE mode) {
    Path outputPath = new Path(targetPath, downloadKey + ZIP_EXTENSION);
    try (
      FSDataOutputStream zipped = targetFS.create(outputPath, true);
    ) {
      if (ModalZipOutputStream.MODE.PRE_DEFLATED == mode) {
        // Use Hadoop-compress for pre_deflated files
        try (ModalZipOutputStream zos = new ModalZipOutputStream(new BufferedOutputStream(zipped))) {
          for (ZipEntrySource source : sources) {
            zipPreDeflated(zos, sourceFS, source);
          }
        }
      } else {
        // Use standard Java libraries for uncompressed input
        try (ZipOutputStream zos = new ZipOutputStream(zipped)) {
          for (ZipEntrySource source : sources) {
            zipDefault(zos, sourceFS, source);
          }
        }
      }

    } catch (Exception ex) {
      LOG.error(ERROR_ZIP_MSG, ex);
      throw Throwables.propagate(ex);
    }
  }

  /**
   * Merge file using the standard Java library java.util.zip.
   */
  private void zipDefault(ZipOutputStream zos, final FileSystem sourceFS, final ZipEntrySource source) throws IOException {
    LOG.info("Zipping uncompressed source {}/{} as entry {}", sourceFS, source.path, source.name);

    Path inputPath = new Path(source.path);
    // append the header file
    if (!Strings.isNullOrEmpty(source.header)) {
      // append the header file
      appendHeaderFile(sourceFS, inputPath, ModalZipOutputStream.MODE.DEFAULT, source.header);
    }

    // Get all the files inside the directory and create a list of InputStreams.
    List<InputStream> is = Arrays.stream(sourceFS.listStatus(inputPath)).sorted().map(fileStatus -> {
        try {
          return sourceFS.open(fileStatus.getPath());
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }).collect(Collectors.toList());

    java.util.zip.ZipEntry ze = new java.util.zip.ZipEntry(source.name);
    zos.putNextEntry(ze);
    for (InputStream fileInZipInputStream : is) {
      ByteStreams.copy(fileInZipInputStream, zos);
      zos.flush();
      fileInZipInputStream.close();
    }
    zos.closeEntry();
  }

  /**
   * Merges the pre-deflated content using the Hadoop-compress library.
   */
  private void zipPreDeflated(ModalZipOutputStream zos, final FileSystem sourceFS, final ZipEntrySource source) throws IOException {
    LOG.info("Zipping pre-compressed source {}/{} as entry {}", sourceFS, source.path, source.name);

    Path inputPath = new Path(source.path);
    // append the header file
    if (!Strings.isNullOrEmpty(source.header)) {
      // append the header file
      appendHeaderFile(sourceFS, inputPath, ModalZipOutputStream.MODE.PRE_DEFLATED, source.header);
    }

    ZipEntry ze = new ZipEntry(source.name);
    zos.putNextEntry(ze, ModalZipOutputStream.MODE.PRE_DEFLATED);
    // Get all the files inside the directory and create a list of InputStreams.
    D2CombineInputStream in = copyToCombinedStream(inputPath, sourceFS, zos);
    setDataFromInputStream(ze, in);
    zos.closeEntry();
  }

  /**
   * Creates a compressed file named '0' that contains a header line.
   */
  private void appendHeaderFile(FileSystem fileSystem, Path dir, ModalZipOutputStream.MODE mode, String header)
    throws IOException {
    String headerLine = header.endsWith("\n") ? header : header + "\n";
    try (FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(dir, HEADER_FILE_NAME))) {
      if (ModalZipOutputStream.MODE.PRE_DEFLATED == mode) {
        D2Utils.compress(new ByteArrayInputStream(headerLine.getBytes()), fsDataOutputStream);
      } else {
        fsDataOutputStream.write(headerLine.getBytes());
      }
    }
  }

  /**
   * Executes the Zip archive creation process.
   * <p>The expected parameters are:
   * <ol>
   *   <li>0. targetPath: HDFS path where the resulting file will be copied.
   *   <li>1. targetFilename: name of resulting archive, without extension (e.g. downloadKey).
   *   <li>2. MODE: ModalZipOutputStream.MODE of input files.
   *   <li>3. sourcePath1: HDFS path to the directory that contains the data files.
   *   <li>4. zipPartFileName1: filename to use in the Zip file for this file.
   *   <li>5. header1: TSV header for sourcePath1
   *   <li>6+7+8… sourcePath2+zipPartFileName2+header2 etc.
   * </ol>
   */
  public static void main(String... args) throws IOException {
    Properties properties = PropertiesUtil.loadProperties(DownloadWorkflowModule.CONF_FILE);

    FileSystem sourceFileSystem =
      DownloadFileUtils.getHdfs(properties.getProperty(DownloadWorkflowModule.DefaultSettings.NAME_NODE_KEY));

    MultiFileArchiveBuilder.withEntries(Arrays.copyOfRange(args, 3, args.length))
      .mergeAllToZip(sourceFileSystem, sourceFileSystem, args[0], args[1],
        ModalZipOutputStream.MODE.valueOf(args[2]));
  }

  /**
   * Private constructor.
   */
  private MultiFileArchiveBuilder(String... sources) {
    ImmutableList.Builder<ZipEntrySource> sourcesBuilder = ImmutableList.builder();

    for (int i = 0; i < sources.length; i+=3) {
      ZipEntrySource source = new ZipEntrySource();
      source.path = sources[i];
      source.name = sources[i+1];
      source.header = sources[i+2];
      sourcesBuilder.add(source);
    }

    this.sources = sourcesBuilder.build();
  }
}
