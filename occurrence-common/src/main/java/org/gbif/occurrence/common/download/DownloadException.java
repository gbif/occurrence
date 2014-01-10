package org.gbif.occurrence.common.download;

/**
 * An exception wrapping any reason why a download can fail.
 */
public class DownloadException extends RuntimeException {

  public DownloadException(Exception source) {
    super(source);
  }

  public DownloadException(String message) {
    super(message);
  }
}
