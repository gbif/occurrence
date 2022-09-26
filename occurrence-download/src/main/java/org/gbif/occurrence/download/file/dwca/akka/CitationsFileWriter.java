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

import org.gbif.api.model.common.search.Facet;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.commons.io.output.FileWriterWithEncoding;
import org.supercsv.cellprocessor.ParseLong;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvBeanWriter;
import org.supercsv.io.ICsvBeanWriter;
import org.supercsv.prefs.CsvPreference;

import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility class that creates a dataset citations file from Map that contains the dataset usages (record count).
 * The output file contains a list of dataset keys/uuids and its counts of occurrence records.
 */
@UtilityClass
@Slf4j
public class CitationsFileWriter {

  // Java fields for facet counts that are used to create the citations file.
  private static final String[] HEADER = {"name", "count"};

  // Processors used to create the citations file.
  private static final CellProcessor[] PROCESSORS = {new NotNull(), new ParseLong()};

  /**
   * Creates the dataset citation file using the search query response.
   *
   * @param datasetUsages          record count per dataset
   * @param citationFileName       output file name
   */
  @SneakyThrows
  public static void createCitationsFile(Map<UUID, Long> datasetUsages, String citationFileName) {
    if (datasetUsages != null && !datasetUsages.isEmpty()) {
      try (ICsvBeanWriter beanWriter = new CsvBeanWriter(new FileWriterWithEncoding(citationFileName, StandardCharsets.UTF_8),
                                                         CsvPreference.TAB_PREFERENCE)) {
        for (Entry<UUID, Long> entry : datasetUsages.entrySet()) {
          if (entry.getKey() != null) {
            beanWriter.write(new Facet.Count(entry.getKey().toString(), entry.getValue()), HEADER, PROCESSORS);
          }
        }
        beanWriter.flush();
      }
    }
  }
}
