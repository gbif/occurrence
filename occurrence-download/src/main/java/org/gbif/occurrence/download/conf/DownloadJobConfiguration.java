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
package org.gbif.occurrence.download.conf;

import org.gbif.api.model.occurrence.*;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.event.search.es.EventEsField;
import org.gbif.occurrence.common.download.DownloadUtils;
import org.gbif.occurrence.download.file.TableSuffixes;
import org.gbif.occurrence.download.file.dwca.archive.DwcDownloadsConstants;
import org.gbif.occurrence.download.hive.ExtensionTable;
import org.gbif.occurrence.download.predicate.PredicateUtil;
import org.gbif.occurrence.download.util.DownloadRequestUtils;
import org.gbif.occurrence.search.es.OccurrenceBaseEsFieldMapper;
import org.gbif.occurrence.search.es.OccurrenceEsField;

import java.util.Set;

import org.apache.hadoop.fs.Path;

import lombok.Builder;
import lombok.Data;

/** Configuration of a small download execution. */
@Data
public class DownloadJobConfiguration {

  /** Occurrence download key/identifier. */
  private final String downloadKey;

  /** Download table/file name. */
  private final String downloadTableName;

  /** Predicate filter. */
  private final String filter;

  /** User that requested the download. */
  private final String user;

  /** Flag that sets if it's a small or big download. */
  private final boolean isSmallDownload;

  /** Directory where the data files are stored, it can be either a local or a hdfs path. */
  private final String sourceDir;

  /** Search query, translation of the query filter. */
  private final String searchQuery;

  /** Requested download format. */
  private final DownloadFormat downloadFormat;

  /** Requested download core. */
  private final DwcTerm coreTerm;

  /** Requested extensions. */
  private final Set<Extension> extensions;

  @Builder
  private DownloadJobConfiguration(
      String downloadKey,
      String downloadTableName,
      String filter,
      String user,
      boolean isSmallDownload,
      String sourceDir,
      String searchQuery,
      DownloadFormat downloadFormat,
      DwcTerm coreTerm,
      Set<Extension> extensions) {
    this.downloadKey = downloadKey;
    this.filter = filter;
    this.user = user;
    this.isSmallDownload = isSmallDownload;
    this.sourceDir = sourceDir;
    this.searchQuery = searchQuery;
    this.downloadTableName = downloadTableName;
    this.downloadFormat = downloadFormat;
    this.coreTerm = coreTerm;
    this.extensions = extensions;
  }

  public static DownloadJobConfiguration forSqlDownload(Download download, String sourceDir) {
    return DownloadJobConfiguration.builder()
        .downloadKey(download.getKey())
        .downloadFormat(download.getRequest().getFormat())
        .coreTerm(download.getRequest().getType().getCoreTerm())
        .extensions(DownloadRequestUtils.getVerbatimExtensions(download.getRequest()))
        .filter(((SqlDownloadRequest) download.getRequest()).getSql())
        .downloadTableName(DownloadUtils.downloadTableName(download.getKey()))
        .isSmallDownload(false)
        .sourceDir(sourceDir)
        .build();
  }

  public static OccurrenceBaseEsFieldMapper esFieldMapper(Download download) {
    return DownloadType.OCCURRENCE == download.getRequest().getType()
        ? OccurrenceEsField.buildFieldMapper()
        : EventEsField.buildFieldMapper();
  }

  /**
   * Interpreted table/file name. This is used for DwcA downloads only, it varies if it's a small or
   * big download. - big downloads format: sourceDir/downloadTableName_interpreted/ - small
   * downloads format: sourceDir/downloadKey/interpreted
   */
  public String getInterpretedDataFileName() {
    return isSmallDownload
        ? getDownloadTempDir()
            + (DwcTerm.Event == coreTerm
                ? DwcDownloadsConstants.EVENT_INTERPRETED_FILENAME
                : DwcDownloadsConstants.OCCURRENCE_INTERPRETED_FILENAME)
        : getDownloadTempDir(TableSuffixes.INTERPRETED_SUFFIX);
  }

  /**
   * Verbatim table/file name. This is used for DwcA downloads only, it varies if it's a small or
   * big download. - big downloads format: sourceDir/downloadTableName_verbatim/ - small downloads
   * format: sourceDir/downloadKey/verbatim
   */
  public String getVerbatimDataFileName() {
    return isSmallDownload
        ? getDownloadTempDir() + DwcDownloadsConstants.VERBATIM_FILENAME
        : getDownloadTempDir(TableSuffixes.VERBATIM_SUFFIX);
  }

  /**
   * Citation table/file name. This is used for DwcA downloads only, it varies if it's a small or
   * big download. - big downloads format: sourceDir/downloadTableName_citation/ - small downloads
   * format: sourceDir/downloadKey/citation
   */
  public String getCitationDataFileName() {
    return isSmallDownload
        ? getDownloadTempDir()
            + DownloadUtils.downloadTableName(downloadKey)
            + '_'
            + DwcDownloadsConstants.CITATIONS_FILENAME
        : getDownloadTempDir(TableSuffixes.CITATION_SUFFIX);
  }

  /**
   * Multimedia table/file name. This is used for DwcA downloads only, it varies if it's a small or
   * big download. - big downloads format: sourceDir/downloadTableName_multimedia/ - small downloads
   * format: sourceDir/downloadKey/multimedia
   */
  public String getMultimediaDataFileName() {
    return isSmallDownload
        ? getDownloadTempDir() + DwcDownloadsConstants.MULTIMEDIA_FILENAME
        : getDownloadTempDir(TableSuffixes.MULTIMEDIA_SUFFIX);
  }

  /**
   * Directory where downloads files will be temporary stored. The output varies for small and big
   * downloads: - small downloads: sourceDir/downloadKey(suffix)/ - big downloads:
   * sourceDir/downloadTableName(suffix)/
   */
  public String getDownloadTempDir(String suffix) {
    return (sourceDir
            + Path.SEPARATOR
            + (isSmallDownload ? downloadKey : downloadTableName)
            + suffix
            + Path.SEPARATOR)
        .toLowerCase();
  }

  public String getExtensionDataFileName(ExtensionTable extensionTable) {
    return isSmallDownload
        ? getDownloadTempDir() + extensionTable.getHiveTableName() + ".txt"
        : getDownloadTempDir("_ext_" + extensionTable.getHiveTableName());
  }

  /**
   * Directory where downloads files will be temporary stored. The output varies for small and big
   * downloads: - small downloads: sourceDir/downloadKey/ - big downloads:
   * sourceDir/downloadTableName/
   */
  public String getDownloadTempDir() {
    return getDownloadTempDir("");
  }
}
