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
package org.gbif.occurrence.download.file.dwca.archive;

import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.hadoop.compress.d2.D2CombineInputStream;
import org.gbif.hadoop.compress.d2.D2Utils;
import org.gbif.hadoop.compress.d2.zip.ModalZipOutputStream;
import org.gbif.hadoop.compress.d2.zip.ZipEntry;
import org.gbif.occurrence.download.conf.DownloadJobConfiguration;
import org.gbif.occurrence.download.conf.WorkflowConfiguration;
import org.gbif.occurrence.download.hive.ExtensionTable;
import org.gbif.occurrence.download.util.DownloadRequestUtils;
import org.gbif.occurrence.download.util.HeadersFileUtil;

import java.io.*;
import java.util.Collection;
import java.util.List;

import static org.gbif.occurrence.download.file.dwca.archive.DwcDownloadsConstants.*;
import static org.gbif.occurrence.download.util.ArchiveFileUtils.cleanupFS;

/**
 * Creates a DWC archive for occurrence downloads based on the hive query result files generated
 * during the Oozie workflow. It creates a local archive folder with an occurrence data file and a
 * dataset sub-folder that contains an EML metadata file per dataset involved.
 */
@Slf4j
@Builder
public class DownloadArchiveBuilder {

  private final Download download;
  private final File archiveDir;
  private final DownloadJobConfiguration configuration;
  private final WorkflowConfiguration workflowConfiguration;
  private final FileSystem sourceFs;
  private final FileSystem targetFs;
  private final CitationFileReader citationFileReader;

  /** Creates the archive descriptor. */
  private void createDescriptor() {
    if (DwcTerm.Event == configuration.getCoreTerm()) {
      DwcArchiveUtils.createEventArchiveDescriptor(
          archiveDir, DownloadRequestUtils.getVerbatimExtensions(download.getRequest()));
    } else {
      DwcArchiveUtils.createOccurrenceArchiveDescriptor(
          archiveDir, DownloadRequestUtils.getVerbatimExtensions(download.getRequest()));
    }
  }

  /** Zips archive content. */
  private void zipArchive() throws IOException {
    String zipFileName = download.getKey() + ".zip";
    // zip up
    Path hdfsTmpZipPath = new Path(workflowConfiguration.getHdfsTempDir(), zipFileName);
    log.info("Zipping archive {} to HDFS temporary location {}", archiveDir, hdfsTmpZipPath);

    try (FSDataOutputStream zipped = targetFs.create(hdfsTmpZipPath, true);
        ModalZipOutputStream zos =
            new ModalZipOutputStream(new BufferedOutputStream(zipped, 10 * 1024 * 1024))) {
      zipLocalFiles(zos);

      // add the large download data files to the zip stream
      if (!configuration.isSmallDownload()) {
        appendPreCompressedFiles(zos);
      }

      zos.finish();
    }

    log.info("Moving Zip from HDFS temporary location to final destination.");
    targetFs.rename(
        hdfsTmpZipPath, new Path(workflowConfiguration.getHdfsOutputPath(), zipFileName));
  }

  /** Main method to assemble the DwC archive and do all the work until we have a final zip file. */
  public void buildArchive() {
    log.info("Start building the archive for {} ", download.getKey());
    try {

      citationFileReader.read();

      // meta.xml
      createDescriptor();

      // Zip contents in to the final archive
      zipArchive();

    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      // cleanUp temp dir
      cleanupFS(archiveDir);
    }
  }

  /** Merges the file using the standard java libraries java.util.zip. */
  private void zipLocalFiles(ModalZipOutputStream zos) {
    try {
      Collection<File> files = org.apache.commons.io.FileUtils.listFiles(archiveDir, null, true);

      for (File f : files) {
        log.debug("Adding local file {} to archive", f);
        try (FileInputStream fileInZipInputStream = new FileInputStream(f)) {
          String zipPath =
              StringUtils.removeStart(
                  f.getAbsolutePath(), archiveDir.getAbsolutePath() + File.separator);
          zos.putNextEntry(new ZipEntry(zipPath), ModalZipOutputStream.MODE.DEFAULT);
          ByteStreams.copy(fileInZipInputStream, zos);
        }
      }
      zos.closeEntry();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private String getInterpretedFileName() {
    return DwcTerm.Event == configuration.getCoreTerm()
        ? EVENT_INTERPRETED_FILENAME
        : OCCURRENCE_INTERPRETED_FILENAME;
  }

  private void appendExtensionFiles(ModalZipOutputStream out) throws IOException {
    if (DownloadRequestUtils.hasVerbatimExtensions(download.getRequest())) {
      for (Extension extension :
          DownloadRequestUtils.getVerbatimExtensions(download.getRequest())) {
        ExtensionTable extensionTable = new ExtensionTable(extension);
        appendPreCompressedFile(
            out,
            new Path(configuration.getExtensionDataFileName(extensionTable)),
            extensionTable.getHiveTableName() + ".txt",
            HeadersFileUtil.getExtensionInterpretedHeader(extensionTable));
      }
    }
  }

  /** Append the pre-compressed content to the zip stream */
  private void appendPreCompressedFiles(ModalZipOutputStream out) throws IOException {
    log.info("Appending pre-compressed occurrence content to the Zip");

    // NOTE: hive lower-cases all the paths
    appendPreCompressedFile(
        out,
        new Path(configuration.getInterpretedDataFileName()),
        getInterpretedFileName(),
        HeadersFileUtil.getInterpretedTableHeader());

    appendPreCompressedFile(
        out,
        new Path(configuration.getVerbatimDataFileName()),
        VERBATIM_FILENAME,
        HeadersFileUtil.getVerbatimTableHeader());

    appendPreCompressedFile(
        out,
        new Path(configuration.getMultimediaDataFileName()),
        MULTIMEDIA_FILENAME,
        HeadersFileUtil.getMultimediaTableHeader());

    appendExtensionFiles(out);
  }

  /** Appends the compressed files found within the directory to the zip stream as the named file */
  private void appendPreCompressedFile(
      ModalZipOutputStream out, Path dir, String filename, String headerRow) throws IOException {
    RemoteIterator<LocatedFileStatus> files = sourceFs.listFiles(dir, false);
    List<InputStream> parts = Lists.newArrayList();

    // Add the header first, which must also be compressed
    ByteArrayOutputStream header = new ByteArrayOutputStream();
    D2Utils.compress(new ByteArrayInputStream(headerRow.getBytes()), header);
    parts.add(new ByteArrayInputStream(header.toByteArray()));

    // Locate the streams to the compressed content on HDFS
    while (files.hasNext()) {
      LocatedFileStatus fs = files.next();
      Path path = fs.getPath();
      if (path.toString().endsWith(D2Utils.FILE_EXTENSION)) {
        log.info("Deflated content to merge: {} ", path);
        parts.add(sourceFs.open(path));
      }
    }

    // create the Zip entry, and write the compressed bytes
    ZipEntry ze = new ZipEntry(filename);
    out.putNextEntry(ze, ModalZipOutputStream.MODE.PRE_DEFLATED);
    try (D2CombineInputStream in = new D2CombineInputStream(parts)) {
      ByteStreams.copy(in, out);
      in.close(); // important so counts are accurate
      ze.setSize(in.getUncompressedLength()); // important to set the sizes and CRC
      ze.setCompressedSize(in.getCompressedLength());
      ze.setCrc(in.getCrc32());
    } finally {
      out.closeEntry();
    }
  }
}
