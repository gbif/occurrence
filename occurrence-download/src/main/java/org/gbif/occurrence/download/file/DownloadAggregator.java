package org.gbif.occurrence.download.file;

import java.util.List;

/**
 * Aggregates results of small downloads files.
 */
public interface DownloadAggregator {

  /**
   * Takes each individual result and combine then in one archive.
   */
  void aggregate(List<Result> results);
}
