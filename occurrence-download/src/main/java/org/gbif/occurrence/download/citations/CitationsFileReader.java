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
package org.gbif.occurrence.download.citations;

import org.gbif.api.vocabulary.License;
import org.gbif.occurrence.download.file.common.DownloadFileUtils;
import org.gbif.occurrence.download.inject.DownloadWorkflowModule;
import org.gbif.utils.file.properties.PropertiesUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.function.BiConsumer;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;

/**
 * Reads a dataset's citations file for consumption.
 */
public abstract class CitationsFileReader {

  private static final Splitter TAB_SPLITTER = Splitter.on('\t').trimResults();

  /**
   * Transforms tab-separated-line into a DatasetOccurrenceDownloadUsage instance.
   */
  private static Map.Entry<UUID,Long> toDatasetOccurrenceDownloadUsage(String tsvLine) {
    Iterator<String> tsvLineIterator = TAB_SPLITTER.split(tsvLine).iterator();
    return new AbstractMap.SimpleImmutableEntry<>(UUID.fromString(tsvLineIterator.next()), Long.parseLong(tsvLineIterator.next()));
  }

  /**
   * Transforms tab-separated-line into a DatasetOccurrenceDownloadUsage instance.
   */
  private static Map.Entry<UUID,License> toDatasetOccurrenceDownloadLicense(String tsvLine) {
    Iterator<String> tsvLineIterator = TAB_SPLITTER.split(tsvLine).iterator();
    String datasetKey = tsvLineIterator.next();
    tsvLineIterator.next();
    String licenseStr = tsvLineIterator.next();
    Optional<License> license = License.fromString(licenseStr);

    return new AbstractMap.SimpleImmutableEntry<>(UUID.fromString(datasetKey), license.isPresent()? license.get(): null);
  }

  /**
   * Reads a dataset citations file with the form 'datasetkeyTABnumberOfRecords' and applies the listed predicates.
   * Each line in read from the TSV file is transformed into a DatasetOccurrenceDownloadUsage.
   *
   * @param nameNode     Hadoop name node uri
   * @param citationPath path to the directory that contains the citation table files
   * @param consumer   consumer that processes bulk of usages
   */
  public static void readCitationsAndUpdateLicense(String nameNode, String citationPath,
                                   BiConsumer<Map<UUID,Long>,Map<UUID,License>> consumer) throws IOException {
    Map<UUID,Long> datasetsCitation = new HashMap<>();
    Map<UUID, License> datasetLicenseCollector = new HashMap<>();
    FileSystem hdfs = DownloadFileUtils.getHdfs(nameNode);
    for (FileStatus fs : hdfs.listStatus(new Path(citationPath))) {
      if (!fs.isDirectory()) {
        try (BufferedReader citationReader =
               new BufferedReader(new InputStreamReader(hdfs.open(fs.getPath()), StandardCharsets.UTF_8))) {
          for (String tsvLine = citationReader.readLine(); tsvLine != null; tsvLine = citationReader.readLine()) {
            if (!Strings.isNullOrEmpty(tsvLine)) {
              // Prepare citation object and add it to list
              // Cope with occurrences within one dataset having different license
              Map.Entry<UUID, Long> citationEntry = toDatasetOccurrenceDownloadUsage(tsvLine);
              Map.Entry<UUID, License> licenseEntry = toDatasetOccurrenceDownloadLicense(tsvLine);
              datasetsCitation.merge(citationEntry.getKey(), citationEntry.getValue(), Long::sum);
              datasetLicenseCollector.merge(licenseEntry.getKey(), licenseEntry.getValue(),
                (a,b) -> License.getMostRestrictive(a, b, b));
            }
          }
        }
      }
    }
    consumer.accept(datasetsCitation, datasetLicenseCollector);
  }

  public static void main(String[] args) throws IOException {
    Properties properties = PropertiesUtil.loadProperties(DownloadWorkflowModule.CONF_FILE);

    readCitationsAndUpdateLicense(
      properties.getProperty(DownloadWorkflowModule.DefaultSettings.NAME_NODE_KEY),
      Preconditions.checkNotNull(args[0]),
      (t,u) -> {});
  }
}
