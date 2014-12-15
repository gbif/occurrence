package org.gbif.occurrence.download.service;

import org.gbif.api.model.common.User;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.service.checklistbank.NameUsageService;
import org.gbif.api.service.common.UserService;
import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.util.occurrence.HumanFilterBuilder;

import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ResourceBundle;
import java.util.Set;

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.occurrence.common.download.DownloadUtils.downloadLink;
import static org.gbif.occurrence.download.service.Constants.NOTIFY_ADMIN;


/**
 * Utility class that sends notification emails of occurrence downloads.
 */
public class DownloadEmailUtils {


  // TODO: This does not do i18n because we don't pass in a Locale
  private static final ResourceBundle RESOURCES = ResourceBundle.getBundle("email");

  private static final Splitter EMAIL_SPLITTER = Splitter.on(';').omitEmptyStrings().trimResults();

  private static final Joiner COMMA_JOINER = Joiner.on(',');

  private final UserService userService;

  private final Set<Address> bccAddresses;

  private final String wsUrl;

  private final String dataUseUrl;

  private final Session session;

  private final DatasetService datasetService;

  private final NameUsageService nameUsageService;

  private final static String DATE_FMT = "yyyy-MM-dd HH:mm:ss z";

  private static final Logger LOG = LoggerFactory.getLogger(DownloadEmailUtils.class);

  @Inject
  public DownloadEmailUtils(@Named("mail.bcc") String bccAddresses, @Named("ws.url") String wsUrl,
    @Named("datause.url") String dataUseUrl,
    UserService userService, Session session, DatasetService datasetService, NameUsageService nameUsageService) {
    this.userService = userService;
    this.bccAddresses = Sets.newHashSet(toInternetAddresses(EMAIL_SPLITTER.split(bccAddresses)));
    this.wsUrl = wsUrl;
    this.session = session;
    this.datasetService = datasetService;
    this.nameUsageService = nameUsageService;
    this.dataUseUrl = dataUseUrl;

  }

  /**
   * Converts the byte size into human-readable format.
   * Support both SI and byte format.
   */
  private static String humanReadableByteCount(long bytes, boolean si) {
    int unit = si ? 1000 : 1024;
    if (bytes < unit) {
      return bytes + " B";
    }
    int exp = (int) (Math.log(bytes) / Math.log(unit));
    String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
    return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
  }

  /**
   * Sends an email notifying that an error occurred while creating the download file.
   */
  public void sendErrorNotificationMail(Download d) {
    // Gets and prepares the E-Mail body text
    // TODO: formatter.setLocale()
    sendNotificationMail(d, RESOURCES.getString("error.subject"),
      MessageFormat.format(RESOURCES.getString("error.text"), d.getKey()));
  }

  /**
   * Sends an email notifying that the occurrence download is ready.
   */
  public void sendSuccessNotificationMail(Download d) {
    // Gets and prepares the E-Mail body text
    // TODO: formatter.setLocale()
    final MessageFormat formatter = new MessageFormat(RESOURCES.getString("success.text"));
    final Object[] bodyParams =
      new Object[] {d.getKey(), downloadLink(wsUrl, d.getKey()), humanReadableByteCount(d.getSize(), true),
        d.getTotalRecords(), d.getNumberDatasets(), dataUseUrl};
    sendNotificationMail(d, RESOURCES.getString("success.subject"), formatter.format(bodyParams));
  }

  /**
   * Returns a formatted string with download details: date created and filter used.
   */
  private String getDownloadDetails(Download download) {
    String date = new SimpleDateFormat(DATE_FMT).format(download.getCreated());
    return MessageFormat.format(RESOURCES.getString("detail"), date, getHumanReadableFilter(download));
  }

  private String getDoiDetails(Download download){
    if(download != null) {
      return '\n' + MessageFormat.format(RESOURCES.getString("doi"), download.getDoi().getUrl());
    }
    return "";
  }

  /**
   * Gets a human readable version of the occurrence search filter used.
   */
  private String getHumanReadableFilter(Download download) {
    // TODO: should escapeXml be false here?
    HumanFilterBuilder filter = new HumanFilterBuilder(RESOURCES, datasetService, nameUsageService, false);
    if (download.getRequest().getPredicate() != null) {
      StringBuilder stringBuilder = new StringBuilder();
      Map<OccurrenceSearchParameter, LinkedList<String>> params =
        filter.humanFilter(download.getRequest().getPredicate());
      for (Iterator<Entry<OccurrenceSearchParameter, LinkedList<String>>> paramEntryIt = params.entrySet().iterator(); paramEntryIt
        .hasNext();) {
        Entry<OccurrenceSearchParameter, LinkedList<String>> paramEntry = paramEntryIt.next();
        stringBuilder.append('\t');
        stringBuilder.append(paramEntry.getKey().name() + ": ");
        stringBuilder.append(COMMA_JOINER.join(paramEntry.getValue()));
        if (paramEntryIt.hasNext()) {
          stringBuilder.append('\n');
        }
      }
      return stringBuilder.toString();
    }
    return RESOURCES.getString("filter.allrecords");
  }

  /**
   * Gets the list of notification addresses from the download object.
   * If the list of addresses is empty, the email of the creator is used.
   */
  private List<Address> getNotificationAddresses(Download download) {
    List<Address> emails = Lists.newArrayList();
    if (download.getRequest().getNotificationAddresses() == null
      || download.getRequest().getNotificationAddresses().isEmpty()) {
      User user = userService.get(download.getRequest().getCreator());
      if (user != null) {
        try {
          emails.add(new InternetAddress(user.getEmail()));
        } catch (AddressException e) {
          // bad address?
          LOG.warn("Ignore corrupt email address {}", user.getEmail());
        }
      }
    } else {
      emails = toInternetAddresses(download.getRequest().getNotificationAddresses());
    }
    return emails;
  }


  /**
   * Utility method that sends a notification email.
   */
  private void sendNotificationMail(Download d, String subject, String body) {
    List<Address> emails = getNotificationAddresses(d);
    if (emails.isEmpty() && bccAddresses.isEmpty()) {
      LOG.warn("No valid notification addresses given for download {}", d.getKey());
      return;
    }
    try {
      // Send E-Mail
      MimeMessage msg = new MimeMessage(session);
      msg.setFrom();
      msg.setRecipients(Message.RecipientType.TO, emails.toArray(new Address[emails.size()]));
      msg.setRecipients(Message.RecipientType.BCC, bccAddresses.toArray(new Address[bccAddresses.size()]));
      msg.setSubject(subject);
      msg.setSentDate(new Date());
      msg.setText(body + '\n' + getDownloadDetails(d) + getDoiDetails(d));
      Transport.send(msg);
    } catch (MessagingException e) {
      LOG.error(NOTIFY_ADMIN, "Sending of notification Mail for download [{}] failed", d.getKey(), e);
    }
  }

  /**
   * Transforms a iterable of string into a list of email addresses.
   */
  private List<Address> toInternetAddresses(Iterable<String> strEmails) {
    List<Address> emails = Lists.newArrayList();
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
