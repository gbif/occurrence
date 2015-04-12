package org.gbif.occurrence.download.file.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;

import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DownloadFileUtils {

  private static final Logger LOG = LoggerFactory.getLogger(DownloadFileUtils.class);


  /**
   * Copies data files into the destination path.
   */
  public static void copyDataFiles(String hdfsPath, String nameNode, String... dataFiles) throws IOException {
    FileSystem fileSystem = getHdfs(nameNode);
    for (String dataFile : dataFiles) {
      if (new File(dataFile).exists()) {
        fileSystem.copyFromLocalFile(true, new Path(dataFile),
                                     new Path(hdfsPath + Path.SEPARATOR + dataFile + Path.SEPARATOR + dataFile));
      }
    }
  }

  /**
   * Utility method that creates a instance of the HDFS FileSystem class.
   */
  public static FileSystem getHdfs(String nameNode) throws IOException {
    // filesystem configs
    final Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, nameNode);
    return FileSystem.get(conf);
  }


  /**
   * Appends a result file to the output file.
   */
  public static void appendAndDelete(String inputFileName, OutputStream outputFileStreamWriter)
    throws IOException {
    File inputFile = new File(inputFileName);
    try (FileInputStream fileReader = new FileInputStream(inputFile)) {
      ByteStreams.copy(fileReader, outputFileStreamWriter);
    } catch (FileNotFoundException e) {
      LOG.info("Error creating occurrence files", e);
      Throwables.propagate(e);
    } finally {
      inputFile.delete();
    }
  }
}
