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
package org.gbif.occurrence.download.file.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;

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
  
  /**
   * Reads count from table path. Helps in utilities for Species list download and SQL Download.
   * @param nameNode namenode of hdfs.
   * @param path species count table path.
   * @return species count.
   * @throws IOException
   */
  public static long readCount(String nameNode, String path) throws IOException {
    FileSystem fs = getHdfs(nameNode);
    return Arrays.stream(fs.listStatus(new Path(path))).filter(FileStatus::isFile).findFirst()
        .map(file -> {
          try (BufferedReader countReader = new BufferedReader(new InputStreamReader(fs.open(file.getPath()), StandardCharsets.UTF_8))) {
            return Long.parseLong(countReader.readLine());
          } catch (IOException e) {
            LOG.error("Couldnot read count from table", e);
            throw Throwables.propagate(e);
          }
        }).orElse(0L);
  }

  /**
   * Hidden constructor.
   */
  private DownloadFileUtils() {
    //empty constructor
  }
}
