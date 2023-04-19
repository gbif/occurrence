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
package org.gbif.occurrence.download.file.dwca.akka;

import org.gbif.api.vocabulary.Extension;
import org.gbif.occurrence.download.conf.DownloadJobConfiguration;
import org.gbif.occurrence.download.file.Result;
import org.gbif.occurrence.download.file.common.DownloadFileUtils;
import org.gbif.occurrence.download.hive.ExtensionTable;

import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.occurrence.download.util.HeadersFileUtil.appendHeaders;
import static org.gbif.occurrence.download.util.HeadersFileUtil.getExtensionInterpretedHeader;

@Slf4j
public class ExtensionFilesWriter implements Closeable {

  private final Map<Extension, FileOutputStream> filesMap = new HashMap<>();

  private final Map<Extension, ExtensionTable> tableMap = new HashMap<>();

  public ExtensionFilesWriter(DownloadJobConfiguration configuration) {
    configuration.getExtensions().forEach(extension -> {
      filesMap.put(extension, extensionOutput(extension, configuration));
      tableMap.put(extension, new ExtensionTable(extension));
    });
  }

  @SneakyThrows
  private static FileOutputStream extensionOutput(Extension extension, DownloadJobConfiguration configuration) {
      String outFile = configuration.getExtensionDataFileName(new ExtensionTable(extension));
      log.info("Aggregating extension file {}", outFile);
      return new FileOutputStream(outFile, true);
  }

  @Override
  public void close() throws IOException {
    for (FileOutputStream fileOutputStream : filesMap.values()) {
      fileOutputStream.close();
    }
  }

  @SneakyThrows
  private void appendExtensionHeaders(Extension extension, FileOutputStream output) {
    appendHeaders(output, getExtensionInterpretedHeader(tableMap.get(extension)));
  }

  @SneakyThrows
  public void writerHeaders() {
    filesMap.forEach(this::appendExtensionHeaders);
  }

  /**
   * Creates the job file name for the extension.
   */
  private String extensionJobFileName(Result result, Extension extension) {
    return result.getDownloadFileWork().getJobDataFileName() + '_' + tableMap.get(extension).getHiveTableName();
  }

  @SneakyThrows
  private void appendResult(Result result, Extension extension, FileOutputStream fileOutputStream) {
    DownloadFileUtils.appendAndDelete(extensionJobFileName(result, extension), fileOutputStream);
  }
  @SneakyThrows
  public void appendAndDelete(Result result) {
    filesMap.forEach((extension, fileOutput) -> appendResult(result, extension, fileOutput));
  }
}
