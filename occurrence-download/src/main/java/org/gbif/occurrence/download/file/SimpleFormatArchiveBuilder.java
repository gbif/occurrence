package org.gbif.occurrence.download.file;

import org.gbif.dwc.terms.Term;
import org.gbif.hadoop.compress.d2.D2CombineInputStream;
import org.gbif.hadoop.compress.d2.D2Utils;
import org.gbif.hadoop.compress.d2.zip.ModalZipOutputStream;
import org.gbif.hadoop.compress.d2.zip.ZipEntry;
import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.download.hive.DownloadTerms;
import org.gbif.occurrence.download.hive.HiveColumns;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Utility class that creates zip file from a directory that stores the data of a Hive table.
 */
public class SimpleFormatArchiveBuilder {

  /**
   * Private constructor.
   */
  private SimpleFormatArchiveBuilder(){
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
   public static void mergeToZip(String nameNode, String hiveTableInputPath, String hdfsOutputPath, String workflowId) throws IOException {
     Configuration conf = new Configuration();
     conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, nameNode);
     Path outputPath = new Path(hdfsOutputPath, DownloadUtils.workflowToDownloadId(workflowId) + ".zip");
     try (
       FileSystem hdfs = FileSystem.get(conf);
       FSDataOutputStream zipped = hdfs.create(outputPath,true);
       ModalZipOutputStream zos = new ModalZipOutputStream(new BufferedOutputStream(zipped));
     ) {
       final Path inputPath = new Path(hiveTableInputPath);
       //appends the header file
       appendHeaderFile(hdfs,inputPath);

       ZipEntry ze = new ZipEntry(Files.getNameWithoutExtension(outputPath.getName()) + ".txt");
       zos.putNextEntry(ze, ModalZipOutputStream.MODE.PRE_DEFLATED);
       //Get all the files inside the directory and creates a list of InputStreams.
       try (D2CombineInputStream in = new D2CombineInputStream(Lists.transform(Lists.newArrayList(hdfs.listStatus(inputPath)), new Function<FileStatus, InputStream>() {
         @Nullable
         @Override
         public InputStream apply(@Nullable FileStatus input) {
           try {
             return hdfs.open(input.getPath());
           } catch (IOException ex){
             Throwables.propagate(ex);
             return null;
           }
         }
       }))) {
         ByteStreams.copy(in, zos);
         in.close(); // required to get the sizes
         ze.setSize(in.getUncompressedLength()); // important to set the sizes and CRC
         ze.setCompressedSize(in.getCompressedLength());
         ze.setCrc(in.getCrc32());
       }

       zos.closeEntry();
       // add an entry to compress
       zos.close();
     }
   }

  /**
   * Creates a compressed file named '0' that contains the content of the file HEADER.
   */
  private static void appendHeaderFile(FileSystem hdfs, Path dir) throws IOException {
    try(FSDataOutputStream fsDataOutputStream = hdfs.create(new Path(dir,HEADER_FILE_NAME))) {
      D2Utils.compress(new ByteArrayInputStream(HEADER.getBytes()), fsDataOutputStream);
    }
  }

  public static void main(String[] args) throws IOException {
     mergeToZip(args[0],args[1],args[2],args[3]);
  }
}
