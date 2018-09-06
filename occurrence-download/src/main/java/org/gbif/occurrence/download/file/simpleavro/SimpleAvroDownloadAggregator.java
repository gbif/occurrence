package org.gbif.occurrence.download.file.simpleavro;

import org.gbif.occurrence.download.file.DownloadAggregator;
import org.gbif.occurrence.download.file.Result;
import java.util.List;

/**
 * Combine the parts created by actor and combine them into single Avro file.
 */
public class SimpleAvroDownloadAggregator implements DownloadAggregator {

  /**
   * Collects the results of each job.
   * Iterates over the list of futures to collect individual results.
   */
  @Override
  public void aggregate(List<Result> results) {
    throw new IllegalStateException("Small Avro downloads not supported as small downloads.");
  }
}
