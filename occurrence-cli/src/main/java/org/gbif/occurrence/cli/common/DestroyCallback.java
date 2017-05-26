package org.gbif.occurrence.cli.common;

/**
 * Simple callback function used to handle destruction.
 */
@FunctionalInterface
public interface DestroyCallback {
  void destroy();
}
