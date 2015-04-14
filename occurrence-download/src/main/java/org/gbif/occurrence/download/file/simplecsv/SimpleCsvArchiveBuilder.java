package org.gbif.occurrence.download.file.simplecsv;

import org.gbif.dwc.terms.Term;
import org.gbif.hadoop.compress.d2.D2CombineInputStream;
import org.gbif.hadoop.compress.d2.D2Utils;
import org.gbif.hadoop.compress.d2.zip.ModalZipOutputStream;
import org.gbif.hadoop.compress.d2.zip.ZipEntry;
import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.download.file.common.DownloadFileUtils;
import org.gbif.occurrence.download.hive.DownloadTerms;
import org.gbif.occurrence.download.hive.HiveColumns;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
import org.gbif.utils.file.properties.PropertiesUtil;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Utility class that creates zip file from a directory that stores the data of a Hive table.
 */
public class SimpleCsvArchiveBuilder {

  /**
   * Private constructor.
   */
  private SimpleCsvArchiveBuilder(){
    //do nothing
  }

  //Header file is named '0' to appear first when listing the content of the directory.
  private static final String HEADER_FILE_NAME = "0";

  //String that contains the file HEADER for the simple table format.
  private static final String  HEADER = Joiner.on('\t').join(Iterables.transform(DownloadTerms.SimpleDownload.SIMPLE_DOWNLOAD_TERMS,
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
   * Merges the content of the hiveTableInputPath (directory) into a zip file in  hdfsOutputPath.
   * The HEADER file is added to the directory hiveTableInputPath so it appears in the resulting zip file.
   */
   public static void mergeToZip(final FileSystem sourceFileSystem, FileSystem targetFileSystem, String sourcePath, String targetPath, String workflowKey, ModalZipOutputStream.MODE mode) throws IOException {

     Path outputPath = new Path(targetPath, workflowKey + ".zip");
     try (
       FSDataOutputStream zipped = targetFileSystem.create(outputPath,true);
       ModalZipOutputStream zos = new ModalZipOutputStream(new BufferedOutputStream(zipped));
     ) {
       final Path inputPath = new Path(sourcePath);
       //appends the header file
       appendHeaderFile(sourceFileSystem,inputPath,mode);

       ZipEntry ze = new ZipEntry(Files.getNameWithoutExtension(outputPath.getName()) + ".txt");
       zos.putNextEntry(ze, mode);
       Closer closer = Closer.create();
       //Get all the files inside the directory and creates a list of InputStreams.
       try {
         D2CombineInputStream in = closer.register(new D2CombineInputStream(Lists.transform(Lists.newArrayList(sourceFileSystem.listStatus(inputPath)), new Function<FileStatus, InputStream>() {
           @Nullable
           @Override
           public InputStream apply(@Nullable FileStatus input) {
             try {
               return sourceFileSystem.open(input.getPath());
             } catch (IOException ex){
               Throwables.propagate(ex);
               return null;
             }
           }
         })));
         ByteStreams.copy(in, zos);
         in.close(); // required to get the sizes
         ze.setSize(in.getUncompressedLength()); // important to set the sizes and CRC
         ze.setCompressedSize(in.getCompressedLength());
         ze.setCrc(in.getCrc32());
       } finally {
         closer.close();
       }

       zos.closeEntry();
       // add an entry to compress
       zos.close();
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

  public static void main(String[] args) throws IOException {
    Properties properties = PropertiesUtil.loadProperties(DownloadWorkflowModule.CONF_FILE);
    FileSystem sourceFileSystem = DownloadFileUtils.getHdfs(properties.getProperty(DownloadWorkflowModule.DefaultSettings.NAME_NODE_KEY));
    mergeToZip(sourceFileSystem,sourceFileSystem,args[0],args[1],DownloadUtils.workflowToDownloadId(args[2]),ModalZipOutputStream.MODE.valueOf(args[3]));
  }
}
