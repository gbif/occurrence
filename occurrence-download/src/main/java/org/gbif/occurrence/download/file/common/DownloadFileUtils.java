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

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;

import lombok.experimental.UtilityClass;

/**
 * Utility class for file operation in occurrence downloads.
 */
@UtilityClass
public final class DownloadFileUtils {

  /**
   * Name of the environment variable that specifies the file copy buffer size. This variable is
   * used to customize the buffer size for file I/O operations during file handling processes such
   * as appending or copying data.
   */
  private static final String FILE_COPY_BUFFER_SIZE_ENV_VAR = "FILE_COPY_BUFFER_SIZE";

  /**
   * Default size of the buffer used for copying files, represented in bytes. This constant defines
   * the maximum size of the data chunks that are read and written during file copying operations. A
   * larger buffer size can improve efficiency by reducing the number of I/O operations, whereas a
   * smaller size can conserve memory at the cost of potentially higher I/O overhead.
   */
  private static final int DEFAULT_FILE_COPY_BUFFER_SIZE = 32768;

  private static final Logger LOG = LoggerFactory.getLogger(DownloadFileUtils.class);

  /**
   * Utility method that creates an instance of the HDFS FileSystem class.
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
    if (inputFile.exists()) {
      try (FileInputStream fileReader = new FileInputStream(inputFile)) {
        LOG.info("Appending file {}", inputFileName);
        IOUtils.copy(fileReader, outputFileStreamWriter, DownloadFileUtils.DEFAULT_FILE_COPY_BUFFER_SIZE);
      } catch (FileNotFoundException e) {
        LOG.info("Error appending file {}", inputFileName, e);
        throw Throwables.propagate(e);
      }
      if (!inputFile.delete()) {
        LOG.info("Input file can't be removed {}", inputFileName);
      }
    } else {
      LOG.warn("File not found {}", inputFileName);
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
            LOG.error("Could not read count from table", e);
            throw Throwables.propagate(e);
          }
        }).orElse(0L);
  }

  public static int getFileCopyBufferSize() {
    String bufferSizeEnv = System.getenv(FILE_COPY_BUFFER_SIZE_ENV_VAR);
    if (bufferSizeEnv != null && !bufferSizeEnv.isEmpty()) {
      try {
        return Integer.parseInt(bufferSizeEnv);
      } catch (NumberFormatException e) {
        System.err.println(
            "Invalid file copy buffer size provided in environment variable. Using default of "
                + DEFAULT_FILE_COPY_BUFFER_SIZE);
      }
    }
    return DEFAULT_FILE_COPY_BUFFER_SIZE;
  }
}
