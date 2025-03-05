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
package org.gbif.occurrence.download.file.dwca.archive;

import org.gbif.api.model.registry.DatasetCitation;
import org.gbif.utils.file.FileUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.function.Consumer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static org.gbif.occurrence.download.file.dwca.archive.DwcDownloadsConstants.RIGHTS_FILENAME;

@Slf4j
public class ConstituentsRightsWriter implements Closeable, Consumer<DatasetCitation> {

  private final Writer writer;

  private int count;

  public static final String RIGHTS_LINE_TEMPLATE = "Dataset: %s " +
                                                     "\nRights as supplied: %s";

  @SneakyThrows
  public ConstituentsRightsWriter(File archiveDir) {
    writer = FileUtils.startNewUtf8File(new File(archiveDir, RIGHTS_FILENAME));
  }

  public static String datasetRights(DatasetCitation datasetCitation) {
    return String.format(RIGHTS_LINE_TEMPLATE, datasetCitation.getTitle(),
      datasetCitation.getLicense() != null ?  datasetCitation.getLicense().getLicenseUrl() : "Not supplied");
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

  @Override
  @SneakyThrows
  public void accept(DatasetCitation dataset) {
    if (count > 0) {
      writer.append('\n');
    }
    writer.write(datasetRights(dataset));
    count++;
  }
}
