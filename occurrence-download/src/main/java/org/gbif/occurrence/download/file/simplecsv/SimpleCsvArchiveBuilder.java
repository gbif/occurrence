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
package org.gbif.occurrence.download.file.simplecsv;

import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.dwc.terms.Term;
import org.gbif.hadoop.compress.d2.D2CombineInputStream;
import org.gbif.hadoop.compress.d2.D2Utils;
import org.gbif.hadoop.compress.d2.zip.ModalZipOutputStream;
import org.gbif.hadoop.compress.d2.zip.ZipEntry;
import org.gbif.occurrence.download.action.DownloadWorkflowModule;
import org.gbif.occurrence.download.file.common.DownloadFileUtils;
import org.gbif.occurrence.download.hive.DownloadTerms;
import org.gbif.utils.file.properties.PropertiesUtil;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipOutputStream;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.shaded.com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;

import lombok.extern.slf4j.Slf4j;

import static org.gbif.occurrence.download.file.d2.D2Utils.copyToCombinedStream;
import static org.gbif.occurrence.download.file.d2.D2Utils.setDataFromInputStream;

/**
 * Utility class that creates zip file from a directory that stores the data of a Hive table.
 */
@Slf4j
public class SimpleCsvArchiveBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleCsvArchiveBuilder.class);

  //Occurrences file name
  private static final String CSV_EXTENSION = ".csv";

  private static final String ZIP_EXTENSION = ".zip";

  private static final String ERROR_ZIP_MSG = "Error creating zip file";
  //Header file is named '0' to appear first when listing the content of the directory.
  private static final String HEADER_FILE_NAME = "0";
  //String that contains the file HEADER for the simple table format.
  private final String header;

  /**
   * Creates the file HEADER.
   * It was moved to a function because a bug in javac https://bugs.openjdk.java.net/browse/JDK-8077605.
   */
  public static SimpleCsvArchiveBuilder withHeader(Set<Pair<DownloadTerms.Group, Term>> downloadTermsHeader) {
    String header = downloadTermsHeader.stream()
      .map(termPair -> DownloadTerms.simpleName(termPair).replaceAll("_", ""))
      .collect(Collectors.joining("\t")) + '\n';
    return new SimpleCsvArchiveBuilder(header);
  }

  /**
   * Creates the file HEADER.
   * It was moved to a function because a bug in javac https://bugs.openjdk.java.net/browse/JDK-8077605.
   */
  public static SimpleCsvArchiveBuilder withHeader(String downloadTermsHeader) {
    return new SimpleCsvArchiveBuilder(downloadTermsHeader+"\n");
  }

  /**
   * Merges the content of sourceFS:sourcePath into targetFS:outputPath in a file called downloadKey.zip.
   * The HEADER file is added to the directory hiveTableInputPath so it appears in the resulting zip file.
   */
  public void mergeToZip(final FileSystem sourceFS, FileSystem targetFS, String sourcePath,
                                String targetPath, String downloadKey, ModalZipOutputStream.MODE mode) throws IOException {
    Path outputPath = new Path(targetPath, downloadKey + ZIP_EXTENSION);
    log.info("Creating zip file with  sourcePath:{} targetPath:{} downloadKey:{} mode:{}",
             sourcePath, targetPath, downloadKey, mode);
    if (ModalZipOutputStream.MODE.PRE_DEFLATED == mode) {
      //Use hadoop-compress for pre_deflated files
      zipPreDeflated(sourceFS, targetFS, sourcePath, outputPath, downloadKey);
    } else {
      //Use standard Java libraries for uncompressed input
      zipDefault(sourceFS, targetFS, sourcePath, outputPath, downloadKey);
    }
  }

  /**
   * Merges the file using the standard java libraries java.util.zip.
   */
  private void zipDefault(final FileSystem sourceFS, final FileSystem targetFS, String sourcePath,
                                 Path outputPath,String downloadKey) {
    try (
      FSDataOutputStream zipped = targetFS.create(outputPath, true);
      ZipOutputStream zos = new ZipOutputStream(zipped)
    ) {
      //appends the header file
      appendHeaderFile(sourceFS, new Path(sourcePath), ModalZipOutputStream.MODE.DEFAULT);
      java.util.zip.ZipEntry ze = new java.util.zip.ZipEntry(Paths.get(downloadKey + CSV_EXTENSION).toString());
      zos.putNextEntry(ze);
      //files are sorted by name
      File[] files = new File(sourcePath).listFiles();
      Arrays.sort(files);
      for (File fileInZip : files) {
        FileInputStream fileInZipInputStream = new FileInputStream(fileInZip);
        ByteStreams.copy(fileInZipInputStream, zos);
        zos.flush();
        fileInZipInputStream.close();
      }
      zos.closeEntry();
    } catch (Exception ex) {
      LOG.error(ERROR_ZIP_MSG, ex);
      throw new RuntimeException(ex);
    }
  }

  /**
   * Merges the pre-deflated content using the hadoop-compress library.
   */

  private void zipPreDeflated(final FileSystem sourceFS, FileSystem targetFS, String sourcePath,
                                     Path outputPath, String downloadKey) throws IOException {
    try (
      FSDataOutputStream zipped = targetFS.create(outputPath, true);
      ModalZipOutputStream zos = new ModalZipOutputStream(new BufferedOutputStream(zipped))
    ) {
      Path inputPath = new Path(sourcePath);
      //appends the header file
      appendHeaderFile(sourceFS, inputPath, ModalZipOutputStream.MODE.PRE_DEFLATED);

      //Get all the files inside the directory and creates a list of InputStreams.
      try {
        ZipEntry ze = new ZipEntry(Paths.get(downloadKey + CSV_EXTENSION).toString());
        zos.putNextEntry(ze, ModalZipOutputStream.MODE.PRE_DEFLATED);
        // Get all the files inside the directory and create a list of InputStreams.
        D2CombineInputStream in = copyToCombinedStream(inputPath, sourceFS, zos);
        setDataFromInputStream(ze, in);
        zos.closeEntry();
      } catch (Exception ex) {
        LOG.error(ERROR_ZIP_MSG, ex);
        throw new RuntimeException(ex);
      }
    }
  }

  /**
   * Creates a compressed file named '0' that contains the content of the file HEADER.
   */
  private void appendHeaderFile(FileSystem fileSystem, Path dir, ModalZipOutputStream.MODE mode)
    throws IOException {
    try (FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(dir, HEADER_FILE_NAME))) {
      if (ModalZipOutputStream.MODE.PRE_DEFLATED == mode) {
        D2Utils.compress(new ByteArrayInputStream(header.getBytes()), fsDataOutputStream);
      } else {
        fsDataOutputStream.write(header.getBytes());
      }
    }
  }

  /**
   * Executes the archive/zip creation process.
   * The expected parameters are:
   * 0. sourcePath: HDFS path to the directory that contains the data files.
   * 1. targetPath: HDFS path where the resulting file will be copied.
   * 2. downloadKey: occurrence download key.
   * 3. MODE: ModalZipOutputStream.MODE of input files.
   * 4. download-format : Download format
   * 5. sql_header: tab separated SQL header in case of SQL Download
   */
  public static void main(String[] args) throws IOException {
    Properties properties = PropertiesUtil.loadProperties(DownloadWorkflowModule.CONF_FILE);
    String downloadFormat = Preconditions.checkNotNull(args[4]).trim();

    FileSystem sourceFileSystem =
      DownloadFileUtils.getHdfs(properties.getProperty(DownloadWorkflowModule.DefaultSettings.NAME_NODE_KEY));

    Set<Pair<DownloadTerms.Group, Term>> downloadTerms =
      DownloadFormat.valueOf(downloadFormat).equals(DownloadFormat.SPECIES_LIST)
        ? DownloadTerms.SPECIES_LIST_DOWNLOAD_TERMS
        : DownloadTerms.SIMPLE_DOWNLOAD_TERMS;
    SimpleCsvArchiveBuilder.withHeader(downloadTerms)
      .mergeToZip(sourceFileSystem, sourceFileSystem, args[0], args[1], args[2],
        ModalZipOutputStream.MODE.valueOf(args[3]));

  }

  /**
   * Private constructor.
   */
  private SimpleCsvArchiveBuilder(String header) {
    this.header = header;
  }
}
