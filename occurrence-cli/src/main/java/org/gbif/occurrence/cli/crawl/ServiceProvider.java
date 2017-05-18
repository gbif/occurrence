package org.gbif.occurrence.cli.crawl;

/**
 *
 */
public interface ServiceProvider<T> {
  T aquire();
  void release();
}
