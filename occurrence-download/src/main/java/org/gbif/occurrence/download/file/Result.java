package org.gbif.occurrence.download.file;

import java.util.Map;
import java.util.UUID;

import com.google.common.base.Objects;

/**
 * Communicates the result of a file creation job.
 */
public class Result implements Comparable<Result> {

  // FileJob associated to this Result instance.
  private final DownloadFileWork downloadFileWork;

  private final Map<UUID, Long> datasetUsages;

  /**
   * Default constructor.
   */
  public Result(DownloadFileWork downloadFileWork, Map<UUID, Long> datasetUsages) {
    this.downloadFileWork = downloadFileWork;
    this.datasetUsages = datasetUsages;
  }

  /**
   * Results are ordered by the fileJob field.
   */
  @Override
  public int compareTo(Result that) {
    return downloadFileWork.compareTo(that.downloadFileWork);
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

  /**
   * @return the datasetUsages map
   */
  public Map<UUID, Long> getDatasetUsages() {
    return datasetUsages;
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
}
