package org.gbif.occurrence.download.inject;

import org.gbif.occurrence.download.file.DownloadAggregator;
import org.gbif.occurrence.download.file.Result;
import java.util.List;

/**
 * Combine the parts created by actor and combine them into single file.
 */
public class NotSupportedDownloadAggregator implements DownloadAggregator {

  /**
   * Collects the results of each job.
   * Iterates over the list of futures to collect individual results.
   */
  @Override
  public void aggregate(List<Result> results) {
    throw new IllegalStateException("Downloads of this format not supported as small downloads.");
  }
}
