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

import static org.gbif.occurrence.download.util.HeadersFileUtil.appendHeaders;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.vocabulary.Extension;
import org.gbif.occurrence.download.conf.DownloadJobConfiguration;
import org.gbif.occurrence.download.file.Result;
import org.gbif.occurrence.download.file.TableSuffixes;
import org.gbif.occurrence.download.file.common.DownloadFileUtils;
import org.gbif.occurrence.download.file.dwca.archive.DwcDownloadsConstants;
import org.gbif.occurrence.download.util.HeadersFileUtil;

@Slf4j
public class InterpretedExtensionFilesWriter implements Closeable {

  private static final Map<Extension, String> EXTENSION_HEADERS = new HashMap<>();
  private static final Map<Extension, String> EXTENSION_SUFFIXES = new HashMap<>();
  private static final Map<Extension, String> EXTENSION_FILE_NAMES = new HashMap<>();

  static {
    EXTENSION_HEADERS.put(Extension.DNA_DERIVED_DATA, HeadersFileUtil.getDnaTableHeader());
    EXTENSION_FILE_NAMES.put(Extension.DNA_DERIVED_DATA, DwcDownloadsConstants.DNA_FILENAME);
    EXTENSION_SUFFIXES.put(Extension.DNA_DERIVED_DATA, TableSuffixes.DNA_SUFFIX);
  }

  private final Map<Extension, FileOutputStream> filesMap = new HashMap<>();

  public InterpretedExtensionFilesWriter(DownloadJobConfiguration configuration) {
    configuration
        .getInterpretedExtensions()
        .forEach(
            extension -> {
              filesMap.put(extension, extensionOutput(extension, configuration));
            });
  }

  @SneakyThrows
  private static FileOutputStream extensionOutput(
      Extension extension, DownloadJobConfiguration configuration) {
    File outFile =
        new File(configuration.getDownloadTempDir() + EXTENSION_FILE_NAMES.get(extension));
    log.info("Aggregating interpreted extension file {}", outFile);
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
    appendHeaders(output, EXTENSION_HEADERS.get(extension));
  }

  @SneakyThrows
  public void writerHeaders() {
    filesMap.forEach(this::appendExtensionHeaders);
  }

  /** Creates the job file name for the extension. */
  private String extensionJobFileName(Result result, Extension extension) {
    return result.getDownloadFileWork().getJobDataFileName()
        + '_'
        + EXTENSION_SUFFIXES.get(extension);
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
