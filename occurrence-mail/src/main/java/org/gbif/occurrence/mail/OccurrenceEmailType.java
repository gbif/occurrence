package org.gbif.occurrence.mail;

/**
 * Type of emails related to occurrence
 */
public enum OccurrenceEmailType implements EmailType {

  SUCCESSFUL_DOWNLOAD("successfulDownload"),

  FAILED_DOWNLOAD("failedDownload");

  private final String key;

  OccurrenceEmailType(String key) {
    this.key = key;
  }

  @Override
  public String getKey() {
    return key;
  }
}
