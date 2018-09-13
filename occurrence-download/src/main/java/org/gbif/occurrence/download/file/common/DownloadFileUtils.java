package org.gbif.occurrence.download.file.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, nameNode);
    return FileSystem.get(conf);
  }

  /**
   * Appends a result file to the output file.
   */
  public static void appendAndDelete(String inputFileName, OutputStream outputFileStreamWriter) throws IOException {
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
  
  public static long readSpeciesCount(String nameNode,String path) throws NumberFormatException, IOException {
    FileSystem hdfs = DownloadFileUtils.getHdfs(nameNode);
    for (FileStatus fs : hdfs.listStatus(new Path(path))) {
      if (!fs.isDirectory()) {
        try (BufferedReader countReader = new BufferedReader(new InputStreamReader(hdfs.open(fs.getPath()),
                                                                                      StandardCharsets.UTF_8))) {
          return Long.parseLong(countReader.readLine());
        }
      }
    }
    LOG.warn("Could not read count in {}",path);
    return 0;
  }

  /**
   * Hidden constructor.
   */
  private DownloadFileUtils() {
    //empty constructor
  }
}
