package org.gbif.occurrence.mail.util;

import com.google.common.base.Splitter;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

public final class OccurrenceMailUtils {

  public static final Splitter EMAIL_SPLITTER = Splitter.on(';').omitEmptyStrings().trimResults();
  public static final Marker NOTIFY_ADMIN = MarkerFactory.getMarker("NOTIFY_ADMIN");

  private OccurrenceMailUtils() {}
}
