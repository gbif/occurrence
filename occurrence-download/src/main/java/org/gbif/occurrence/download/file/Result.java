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
package org.gbif.occurrence.download.file;

import org.gbif.api.vocabulary.License;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nullable;

import com.google.common.base.Objects;

/**
 * Communicates the result of a file creation job.
 */
public class Result implements Comparable<Result> {

  // FileJob associated to this Result instance.
  private final DownloadFileWork downloadFileWork;

  private final Map<UUID, Long> datasetUsages;
  private final Set<License> datasetLicenses;

  /**
   * Default constructor.
   */
  public Result(DownloadFileWork downloadFileWork, Map<UUID, Long> datasetUsages) {
    this(downloadFileWork, datasetUsages, null);
  }

  public Result(DownloadFileWork downloadFileWork, Map<UUID, Long> datasetUsages,
                @Nullable Set<License> datasetLicenses) {
    this.downloadFileWork = downloadFileWork;
    this.datasetUsages = datasetUsages;
    this.datasetLicenses = datasetLicenses;
  }

  /**
   * Results are ordered by the fileJob field.
   */
  @Override
  public int compareTo(Result that) {
    return downloadFileWork.compareTo(that.downloadFileWork);
  }

  /**
   * @return the datasetUsages map
   */
  public Map<UUID, Long> getDatasetUsages() {
    return datasetUsages;
  }

  /**
   *
   * @return may be null
   */
  public Set<License> getDatasetLicenses() {
    return datasetLicenses;
  }

  /**
   * @return the fileJob
   */
  public DownloadFileWork getDownloadFileWork() {
    return downloadFileWork;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(downloadFileWork);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof Result)) {
      return false;
    }

    Result that = (Result) obj;
    return Objects.equal(downloadFileWork, that.downloadFileWork);
  }
}
