package org.gbif.occurrence.download.file.simplecsv;

import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.dwc.terms.Term;
import org.gbif.hadoop.compress.d2.D2CombineInputStream;
import org.gbif.hadoop.compress.d2.D2Utils;
import org.gbif.hadoop.compress.d2.zip.ModalZipOutputStream;
import org.gbif.hadoop.compress.d2.zip.ZipEntry;
import org.gbif.occurrence.download.file.common.DownloadFileUtils;
import org.gbif.occurrence.download.hive.DownloadTerms;
import org.gbif.occurrence.download.hive.HiveColumns;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
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

import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class that creates zip file from a directory that stores the data of a Hive table.
 */
public class SimpleCsvArchiveBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleCsvArchiveBuilder.class);

  //Occurrences file name
  private static final String CSV_EXTENSION = ".csv";

  private static final String ZIP_EXTENSION = ".zip";

  private static final String ERROR_ZIP_MSG = "Error creating zip file";
  //Header file is named '0' to appear first when listing the content of the directory.
  private static final String HEADER_FILE_NAME = "0";
  //String that contains the file HEADER for the simple table format.
  private final String HEADER;
  
  /**
   * Creates the file HEADER.
   * It was moved to a function because a bug in javac https://bugs.openjdk.java.net/browse/JDK-8077605.
   */
  public static SimpleCsvArchiveBuilder withHeader(Set<Term> downloadTermsHeader) {
    String header =  downloadTermsHeader.stream()
      .map(term -> HiveColumns.columnFor(term).replaceAll("_", ""))
      .collect(Collectors.joining("\t")) + '\n';
    return new SimpleCsvArchiveBuilder(header);
  }
  /**
   * Merges the content of sourceFS:sourcePath into targetFS:outputPath in a file called downloadKey.zip.
   * The HEADER file is added to the directory hiveTableInputPath so it appears in the resulting zip file.
   */
  public void mergeToZip(final FileSystem sourceFS, FileSystem targetFS, String sourcePath,
                                String targetPath, String downloadKey, ModalZipOutputStream.MODE mode) throws IOException {
    Path outputPath = new Path(targetPath, downloadKey + ZIP_EXTENSION);
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
      ZipOutputStream zos = new ZipOutputStream(zipped);
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
      throw Throwables.propagate(ex);
    }
  }

  /**
   * Merges the pre-deflated content using the hadoop-compress library.
   */
  private void zipPreDeflated(final FileSystem sourceFS, FileSystem targetFS, String sourcePath,
                                     Path outputPath, String downloadKey) throws IOException {
    try (
      FSDataOutputStream zipped = targetFS.create(outputPath, true);
      ModalZipOutputStream zos = new ModalZipOutputStream(new BufferedOutputStream(zipped));
    ) {
      final Path inputPath = new Path(sourcePath);
      //appends the header file
      appendHeaderFile(sourceFS, inputPath, ModalZipOutputStream.MODE.PRE_DEFLATED);

      //Get all the files inside the directory and creates a list of InputStreams.
      try {
        D2CombineInputStream in =
          new D2CombineInputStream(Arrays.stream(sourceFS.listStatus(inputPath)).map(fileStatus -> {
            try {
              return sourceFS.open(fileStatus.getPath());
            } catch (IOException ex) {
              throw Throwables.propagate(ex);
            }
          }).collect(Collectors.toList()));
        ZipEntry ze = new ZipEntry(Paths.get(downloadKey + CSV_EXTENSION).toString());
        zos.putNextEntry(ze, ModalZipOutputStream.MODE.PRE_DEFLATED);
        ByteStreams.copy(in, zos);
        in.close(); // required to get the sizes
        ze.setSize(in.getUncompressedLength()); // important to set the sizes and CRC
        ze.setCompressedSize(in.getCompressedLength());
        ze.setCrc(in.getCrc32());
        zos.closeEntry();
      } catch (Exception ex) {
        LOG.error(ERROR_ZIP_MSG, ex);
        throw Throwables.propagate(ex);
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
        D2Utils.compress(new ByteArrayInputStream(HEADER.getBytes()), fsDataOutputStream);
      } else {
        fsDataOutputStream.write(HEADER.getBytes());
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
   */
  public static void main(String[] args) throws IOException {
    Properties properties = PropertiesUtil.loadProperties(DownloadWorkflowModule.CONF_FILE);
    String downloadFormat=properties.getProperty(DownloadWorkflowModule.DynamicSettings.DOWNLOAD_FORMAT_KEY);
    FileSystem sourceFileSystem =
      DownloadFileUtils.getHdfs(properties.getProperty(DownloadWorkflowModule.DefaultSettings.NAME_NODE_KEY));
    SimpleCsvArchiveBuilder archiveBuilder;
    if(DownloadFormat.valueOf(downloadFormat).equals(DownloadFormat.SPECIES_LIST))
      archiveBuilder = SimpleCsvArchiveBuilder.withHeader(DownloadTerms.SPECIES_LIST_DOWNLOAD_TERMS);
    else
      archiveBuilder = SimpleCsvArchiveBuilder.withHeader(DownloadTerms.SIMPLE_DOWNLOAD_TERMS);
    archiveBuilder.mergeToZip(sourceFileSystem, sourceFileSystem, args[0], args[1], args[2],ModalZipOutputStream.MODE.valueOf(args[3]));
  }

  /**
   * Private constructor.
   */
  private SimpleCsvArchiveBuilder(String header) {
    this.HEADER = header;
  }
}
