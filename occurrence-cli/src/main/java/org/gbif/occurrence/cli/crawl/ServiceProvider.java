package org.gbif.occurrence.cli.crawl;

/**
 * Allows to provide an Object and release it after usage.
 * Mostly used by scheduled tasks to release resources between intervals.
 */
interface ServiceProvider<T> {
  T acquire();
  void handleReport(Object report);
  void release();
}
