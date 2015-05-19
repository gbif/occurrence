package org.gbif.occurrence.download.file.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;

import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for file operation in occurrence downloads.
 */
public final class DownloadFileUtils {

  private static final Logger LOG = LoggerFactory.getLogger(DownloadFileUtils.class);

  /**
   * Utility method that creates a instance of the HDFS FileSystem class.
   */
  public static FileSystem getHdfs(String nameNode) throws IOException {
    // filesystem configs
    final Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, nameNode);
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
      throw Throwables.propagate(e);
    } finally {
      inputFile.delete();
    }
  }

  /**
   * Hidden constructor.
   */
  private DownloadFileUtils(){
    //empty constructor
  }
}
