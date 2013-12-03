package org.gbif.occurrencestore.download.file;

import java.util.Map;
import java.util.UUID;

import com.google.common.base.Objects;


/**
 * Communicates the result of a file creation job.
 */
public class Result implements Comparable<Result> {

  // FileJob associated to this Result instance.
  private final FileJob fileJob;

  private final Map<UUID, Long> datasetUsages;

  /**
   * Default constructor.
   */
  public Result(FileJob fileJob, Map<UUID, Long> datasetUsages) {
    this.fileJob = fileJob;
    this.datasetUsages = datasetUsages;
  }

  /**
   * Results are ordered by the fileJob field.
   */
  @Override
  public int compareTo(Result that) {
    return this.fileJob.compareTo(that.fileJob);
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
    return Objects.equal(this.fileJob, that.fileJob);
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
  public FileJob getFileJob() {
    return fileJob;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(fileJob);
  }
}
