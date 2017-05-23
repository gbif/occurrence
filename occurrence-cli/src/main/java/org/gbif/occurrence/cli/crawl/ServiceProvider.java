package org.gbif.occurrence.cli.crawl;

/**
 *
 */
interface ServiceProvider<T> {
  T acquire();
  void handleReport(Object report);
  void release();
}
