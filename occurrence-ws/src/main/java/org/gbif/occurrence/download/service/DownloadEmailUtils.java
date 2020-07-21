package org.gbif.occurrence.download.service;

import org.gbif.api.model.common.GbifUser;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.service.common.IdentityAccessService;
import org.gbif.occurrence.query.HumanPredicateBuilder;
import org.gbif.occurrence.query.TitleLookupService;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TimeZone;
import javax.mail.Address;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import static org.gbif.occurrence.download.service.Constants.NOTIFY_ADMIN;

import static freemarker.template.Configuration.VERSION_2_3_25;

/**
 * Utility class that sends notification emails of occurrence downloads.
 */
@Component
public class DownloadEmailUtils {
  private static final Logger LOG = LoggerFactory.getLogger(DownloadEmailUtils.class);
  private static final Splitter EMAIL_SPLITTER = Splitter.on(';').omitEmptyStrings().trimResults();
  private static final String SUCCESS_SUBJECT = "Your GBIF data download is ready";
  private static final String ERROR_SUBJECT = "Your GBIF data download failed";

  private final Configuration freemarker = new Configuration(VERSION_2_3_25);
  private final IdentityAccessService identityAccessService;
  private final Set<Address> bccAddresses;
  private final URI portalUrl;
  private final Session session;
  private final TitleLookupService titleLookup;

  @Autowired
  public DownloadEmailUtils(@Value("${occurrence.download.mail.bcc}") String bccAddresses, @Value("${occurrence.download.portal.url}") String portalUrl,
                            IdentityAccessService identityAccessService, Session session, TitleLookupService titleLookup) {
    this.identityAccessService = identityAccessService;
    this.titleLookup = titleLookup;
    this.bccAddresses = Sets.newHashSet(toInternetAddresses(EMAIL_SPLITTER.split(bccAddresses)));
    this.session = session;
    this.portalUrl = URI.create(portalUrl);
    setupFreemarker();
  }

  private void setupFreemarker() {
    freemarker.setDefaultEncoding("UTF-8");
    freemarker.setLocale(Locale.UK);
    freemarker.setTimeZone(TimeZone.getTimeZone("GMT"));
    freemarker.setNumberFormat("0.####");
    freemarker.setDateFormat("d MMMM yyyy");
    freemarker.setTimeFormat("HH:mm:ss");
    freemarker.setDateTimeFormat("HH:mm:ss d MMMM yyyy");
    freemarker.setClassForTemplateLoading(DownloadEmailUtils.class, "/email");
  }

  /**
   * Sends an email notifying that an error occurred while creating the download file.
   */
  public void sendErrorNotificationMail(Download download) {
    sendNotificationMail(download, ERROR_SUBJECT, "error.ftl");
  }

  /**
   * Sends an email notifying that the occurrence download is ready.
   */
  public void sendSuccessNotificationMail(Download download) {
    sendNotificationMail(download, SUCCESS_SUBJECT, "success.ftl");
  }

  @VisibleForTesting
  protected String buildBody(Download download, String bodyTemplate) throws IOException, TemplateException {
    // Prepare the E-Mail body text
    StringWriter contentBuffer = new StringWriter();
    Template template = freemarker.getTemplate(bodyTemplate);
    template.process(new EmailModel(download, portalUrl, getHumanQuery(download)), contentBuffer);
    return contentBuffer.toString();
  }

  /**
   * Gets a human readable version of the occurrence search query used.
   */
  public String getHumanQuery(Download download) {
    try {
      String query =
        new HumanPredicateBuilder(titleLookup).humanFilterString(((PredicateDownloadRequest) download.getRequest()).getPredicate());
      if (query.length() > 1000) {
        query = query.substring(0, 1000) + "\n… abbreviated, view the full query on the landing page.";
      }
      return "        " + query.replace("\n", "\n        ");
    } catch (Exception e) {
      return "        The query is too complex.  View it on the landing page.";
    }
  }

  /**
   * Gets the list of notification addresses from the download object.
   * If the list of addresses is empty, the email of the creator is used.
   */
  private List<Address> getNotificationAddresses(Download download) {
    List<Address> emails = Lists.newArrayList();
    if (download.getRequest().getNotificationAddresses() == null
        || download.getRequest().getNotificationAddresses().isEmpty()) {
      GbifUser user = identityAccessService.get(download.getRequest().getCreator());
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
  private void sendNotificationMail(Download download, String subject, String bodyTemplate) {
    List<Address> emails = getNotificationAddresses(download);
    if (emails.isEmpty() && bccAddresses.isEmpty()) {
      LOG.warn("No valid notification addresses given for download {}", download.getKey());
      return;
    }
    try {
      // Send E-Mail
      MimeMessage msg = new MimeMessage(session);
      msg.setFrom();
      msg.setRecipients(Message.RecipientType.TO, emails.toArray(new Address[0]));
      msg.setRecipients(Message.RecipientType.BCC, bccAddresses.toArray(new Address[0]));
      msg.setSubject(subject);
      msg.setSentDate(new Date());
      msg.setText(buildBody(download, bodyTemplate));
      Transport.send(msg);

    } catch (TemplateException | IOException e) {
      LOG.error(NOTIFY_ADMIN, "Rendering of notification Mail for download [{}] failed", download.getKey(), e);
    } catch (MessagingException e) {
      LOG.error(NOTIFY_ADMIN, "Sending of notification Mail for download [{}] failed", download.getKey(), e);
    }
  }

  /**
   * Transforms a iterable of string into a list of email addresses.
   */
  private static List<Address> toInternetAddresses(Iterable<String> strEmails) {
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
