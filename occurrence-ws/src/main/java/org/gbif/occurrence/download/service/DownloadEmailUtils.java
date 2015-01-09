package org.gbif.occurrence.download.service;

import org.gbif.api.model.common.User;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.service.common.UserService;
import org.gbif.occurrence.download.service.freemarker.NiceDateTemplateMethodModel;
import org.gbif.occurrence.query.HumanFilterBuilder;
import org.gbif.occurrence.query.TitleLookup;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Set;
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
import com.google.inject.Inject;
import com.google.inject.name.Named;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.gbif.occurrence.download.service.Constants.NOTIFY_ADMIN;


/**
 * Utility class that sends notification emails of occurrence downloads.
 */
public class DownloadEmailUtils {
  private static final Logger LOG = LoggerFactory.getLogger(DownloadEmailUtils.class);
  private static final Splitter EMAIL_SPLITTER = Splitter.on(';').omitEmptyStrings().trimResults();
  private static final String SUCCESS_SUBJECT = "Your GBIF data download is ready";
  private static final String ERROR_SUBJECT = "Your GBIF data download failed";

  private final Configuration freemarker = new Configuration();
  private final UserService userService;
  private final Set<Address> bccAddresses;
  private final URI portalUrl;
  private final Session session;
  private final TitleLookup titleLookup;

  @Inject
  public DownloadEmailUtils(@Named("mail.bcc") String bccAddresses, @Named("portal.url") String portalUrl,
    UserService userService, Session session, TitleLookup titleLookup) {
    this.userService = userService;
    this.titleLookup = titleLookup;
    this.bccAddresses = Sets.newHashSet(toInternetAddresses(EMAIL_SPLITTER.split(bccAddresses)));
    this.session = session;
    this.portalUrl = URI.create(portalUrl);
    setupFreemarker();
  }

  private void setupFreemarker() {
    freemarker.setDefaultEncoding("UTF-8");
    freemarker.setLocale(Locale.US);
    freemarker.setNumberFormat("0.####");
    freemarker.setDateFormat("yyyy-mm-dd");
    // create custom rendering for relative dates
    freemarker.setSharedVariable("niceDate", new NiceDateTemplateMethodModel());
    freemarker.setClassForTemplateLoading(DownloadEmailUtils.class, "/email");
  }

  /**
   * Sends an email notifying that an error occurred while creating the download file.
   */
  public void sendErrorNotificationMail(Download d) {
    sendNotificationMail(d, ERROR_SUBJECT, "error.ftl");
  }

  /**
   * Sends an email notifying that the occurrence download is ready.
   */
  public void sendSuccessNotificationMail(Download d) {
    sendNotificationMail(d, SUCCESS_SUBJECT, "success.ftl");
  }

  @VisibleForTesting
  protected String buildBody(Download d, String bodyTemplate) throws IOException, TemplateException {
    // Prepare the E-Mail body text
    StringWriter contentBuffer = new StringWriter();
    Template template = freemarker.getTemplate(bodyTemplate);
    template.process(new EmailModel(d, portalUrl, getHumanQuery(d)), contentBuffer);
    return contentBuffer.toString();
  }

  /**
   * Gets a human readable version of the occurrence search query used.
   */
  public String getHumanQuery(Download download) {
    return new HumanFilterBuilder(titleLookup).humanFilterString(download.getRequest().getPredicate());
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
  private void sendNotificationMail(Download d, String subject, String bodyTemplate) {
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
      msg.setText(buildBody(d, bodyTemplate));
      Transport.send(msg);

    } catch (TemplateException | IOException e) {
      LOG.error(NOTIFY_ADMIN, "Rendering of notification Mail for download [{}] failed", d.getKey(), e);
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
