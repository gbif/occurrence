package org.gbif.occurrence.download.service;

import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

/**
 * Contains common constants used by the occurrence dowload classes.
 */
public final class Constants {

  // oozie job properties
  public static final String NOTIFICATION_PROPERTY = "gbif_notification_addresses";
  public static final String USER_PROPERTY = "gbif_user";
  public static final String FILTER_PROPERTY = "gbif_filter";
  public static final String SEND_NOTIFICATION_PROPERTY = "gbif_send_notification";
  // guice occurrence-download.xyz properties
  public static final String OOZIE_USER = "occurrence-download";

  // Log messages marked with this Marker should trigger an email notification
  public static final Marker NOTIFY_ADMIN = MarkerFactory.getMarker("NOTIFY_ADMIN");

  /**
   * Private constructor.
   */
  private Constants() {
    throw new UnsupportedOperationException("Can't initialize class");
  }

}
