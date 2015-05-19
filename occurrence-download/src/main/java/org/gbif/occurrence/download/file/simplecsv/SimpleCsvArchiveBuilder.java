package org.gbif.occurrence.download.file.simplecsv;

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
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.zip.ZipOutputStream;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
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
  private static final String OCCURRENCES_FILE_NAME = "occurrences.csv";

  private static final String ZIP_EXTENSION = ".zip";

  private static final String ERROR_ZIP_MSG = "Error creating zip file";

  /**
   * Private constructor.
   */
  private SimpleCsvArchiveBuilder(){
    //do nothing
  }

  //Header file is named '0' to appear first when listing the content of the directory.
  private static final String HEADER_FILE_NAME = "0";

  //String that contains the file HEADER for the simple table format.
  private static final String  HEADER = Joiner.on('\t').join(Iterables.transform(DownloadTerms.SIMPLE_DOWNLOAD_TERMS,
                                                                                 new Function<Term, String>() {
                                                                                   @Nullable
                                                                                   @Override
                                                                                   public String apply(
                                                                                     @Nullable Term input
                                                                                   ) {
                                                                                     return HiveColumns.columnFor(input);
                                                                                   }
                                                                                 })) + '\n';

  /**
   * Merges the content of sourceFileSystem:sourcePath into targetFileSystem:outputPath in a file called downloadKey.zip.
   * The HEADER file is added to the directory hiveTableInputPath so it appears in the resulting zip file.
   */
   public static void mergeToZip(final FileSystem sourceFileSystem, FileSystem targetFileSystem, String sourcePath, String targetPath, String downloadKey, ModalZipOutputStream.MODE mode) throws IOException {

     Path outputPath = new Path(targetPath, downloadKey + ZIP_EXTENSION);
     if(ModalZipOutputStream.MODE.PRE_DEFLATED == mode) {
       //Use hadoop-compress for pre_deflated files
       zipPreDeflated(sourceFileSystem, targetFileSystem, sourcePath, outputPath, downloadKey);
     } else {
       //Use standard Java libraries for uncompressed input
       zipDefault(sourceFileSystem, targetFileSystem, sourcePath, outputPath, downloadKey);
     }

  }

  /**
   * Merges the file using the standard java libraries java.util.zip.
   */
  private static void zipDefault(final FileSystem sourceFileSystem, final FileSystem targetFileSystem, String sourcePath, Path outputPath, String downloadKey) throws IOException {
    try (
      FSDataOutputStream zipped = targetFileSystem.create(outputPath,true);
      ZipOutputStream zos = new ZipOutputStream(zipped);
    ) {
      //appends the header file
      appendHeaderFile(sourceFileSystem, new Path(sourcePath), ModalZipOutputStream.MODE.DEFAULT);
      java.util.zip.ZipEntry ze = new java.util.zip.ZipEntry(Paths.get(downloadKey,OCCURRENCES_FILE_NAME).toString());
      zos.putNextEntry(ze);
      for (File fileInZip : new File(sourcePath).listFiles()) {
        FileInputStream fileInZipInputStream = new FileInputStream(fileInZip);
        ByteStreams.copy(fileInZipInputStream, zos);
        zos.flush();
        fileInZipInputStream.close();
      }
      zos.closeEntry();
    } catch (Exception ex) {
      LOG.error(ERROR_ZIP_MSG,ex);
      throw Throwables.propagate(ex);
    }
  }

  /**
   * Merges the pre-deflated content using the hadoop-compress library.
   */
  private static void zipPreDeflated(final FileSystem sourceFileSystem, FileSystem targetFileSystem, String sourcePath, Path outputPath, String downloadKey) throws IOException {
    try (
      FSDataOutputStream zipped = targetFileSystem.create(outputPath,true);
      ModalZipOutputStream zos = new ModalZipOutputStream(new BufferedOutputStream(zipped));
    ) {
      final Path inputPath = new Path(sourcePath);
      //appends the header file
      appendHeaderFile(sourceFileSystem, inputPath, ModalZipOutputStream.MODE.PRE_DEFLATED);

      //Get all the files inside the directory and creates a list of InputStreams.
      try {
        D2CombineInputStream in = new D2CombineInputStream(Lists.transform(Lists.newArrayList(sourceFileSystem.listStatus(inputPath)), new Function<FileStatus, InputStream>() {
          @Nullable
          @Override
          public InputStream apply(@Nullable FileStatus input) {
            try {
              return sourceFileSystem.open(input.getPath());
            } catch (IOException ex){
              throw Throwables.propagate(ex);
            }
          }
        }));
        ZipEntry ze = new ZipEntry(Paths.get(downloadKey,OCCURRENCES_FILE_NAME).toString());
        zos.putNextEntry(ze,  ModalZipOutputStream.MODE.PRE_DEFLATED);
        ByteStreams.copy(in, zos);
        in.close(); // required to get the sizes
        ze.setSize(in.getUncompressedLength()); // important to set the sizes and CRC
        ze.setCompressedSize(in.getCompressedLength());
        ze.setCrc(in.getCrc32());
        zos.closeEntry();
      } catch (Exception ex){
        LOG.error(ERROR_ZIP_MSG,ex);
        throw Throwables.propagate(ex);
      }
    }
  }

  /**
   * Creates a compressed file named '0' that contains the content of the file HEADER.
   */
  private static void appendHeaderFile(FileSystem targetFileSystem, Path dir, ModalZipOutputStream.MODE mode) throws IOException {
    try(FSDataOutputStream fsDataOutputStream = targetFileSystem.create(new Path(dir,HEADER_FILE_NAME))) {
      if(ModalZipOutputStream.MODE.PRE_DEFLATED == mode ) {
        D2Utils.compress(new ByteArrayInputStream(HEADER.getBytes()), fsDataOutputStream);
      } else {
        fsDataOutputStream.write(HEADER.getBytes());
      }
    }
  }

  /**
   * Executes the archive/zip creation process.
   * The expected parameters are:
   *  0. sourcePath: HDFS path to the directory that contains the data files.
   *  1. targetPath: HDFS path where the resulting file will be copied.
   *  2. downloadKey: occurrence download key.
   *  3. MODE: ModalZipOutputStream.MODE of input files.
   */
  public static void main(String[] args) throws IOException {
    Properties properties = PropertiesUtil.loadProperties(DownloadWorkflowModule.CONF_FILE);
    FileSystem sourceFileSystem = DownloadFileUtils.getHdfs(properties.getProperty(DownloadWorkflowModule.DefaultSettings.NAME_NODE_KEY));
    mergeToZip(sourceFileSystem,sourceFileSystem,args[0],args[1],args[2],ModalZipOutputStream.MODE.valueOf(args[3]));
  }
}
