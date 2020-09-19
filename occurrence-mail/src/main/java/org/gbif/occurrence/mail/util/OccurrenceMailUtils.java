package org.gbif.occurrence.mail.util;

import com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import java.util.ArrayList;
import java.util.List;

public final class OccurrenceMailUtils {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceMailUtils.class);

  public static final Splitter EMAIL_SPLITTER = Splitter.on(';').omitEmptyStrings().trimResults();
  public static final Marker NOTIFY_ADMIN = MarkerFactory.getMarker("NOTIFY_ADMIN");

  private OccurrenceMailUtils() {}

  /**
   * Transforms a iterable of string into a list of email addresses.
   */
  public static List<InternetAddress> toInternetAddresses(Iterable<String> strEmails) {
    List<InternetAddress> emails = new ArrayList<>();
    for (String address : strEmails) {
      try {
        emails.add(new InternetAddress(address));
      } catch (AddressException e) {
        // bad address?
        LOG.warn("Ignore corrupt email address {}", address);
      }
    }
    return emails;
  }
}
