package org.gbif.occurrence.mail;

import freemarker.template.TemplateException;
import org.gbif.api.model.common.AbstractGbifUser;
import org.gbif.api.model.common.GbifUser;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.service.common.IdentityAccessService;
import org.gbif.occurrence.query.HumanPredicateBuilder;
import org.gbif.occurrence.query.TitleLookupService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.mail.Address;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.ResourceBundle;

import static org.gbif.occurrence.mail.util.OccurrenceMailUtils.NOTIFY_ADMIN;
import static org.gbif.occurrence.mail.util.OccurrenceMailUtils.toInternetAddresses;

/**
 * Manager handling the different types of email related to occurrence downloads.
 * Responsibilities (with the help of (via {@link FreemarkerEmailTemplateProcessor}): - decide where
 * to send the email (which address) - generate the body of the email
 */
@Service
public class OccurrenceEmailManager {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceEmailManager.class);

  // supported locales
  private static final List<String> SUPPORTED_LOCALES = Arrays.asList("en", "ru");

  private final EmailTemplateProcessor emailTemplateProcessor;
  private final IdentityAccessService identityAccessService;
  private final TitleLookupService titleLookup;

  public OccurrenceEmailManager(
      @Qualifier("occurrenceEmailTemplateProcessor")
          EmailTemplateProcessor emailTemplateProcessor,
      @Qualifier("baseIdentityAccessService")
          IdentityAccessService identityAccessService,
      TitleLookupService titleLookup) {
    Objects.requireNonNull(emailTemplateProcessor, "emailTemplateProcessor shall be provided");
    this.identityAccessService = identityAccessService;
    this.titleLookup = titleLookup;
    this.emailTemplateProcessor = emailTemplateProcessor;
  }

  public BaseEmailModel generateSuccessfulDownloadEmailModel(Download download, String portal) {
    GbifUser creator = getCreator(download);
    Locale locale = getLocale(creator);
    String downloadCreatedDate = String.format(Locale.ENGLISH, "%te %<tB %<tY", download.getCreated());

    try {
      DownloadTemplateDataModel dataModel =
          new DownloadTemplateDataModel(download, new URL(portal), getHumanQuery(download, locale), downloadCreatedDate);

      return emailTemplateProcessor.buildEmail(
          OccurrenceEmailType.SUCCESSFUL_DOWNLOAD, getNotificationAddresses(download, creator), dataModel, locale);
    } catch (TemplateException | IOException e) {
      LOG.error(
          NOTIFY_ADMIN,
          "Rendering of notification Mail for download [{}] failed", download.getKey(), e);
    }

    return null;
  }

  public BaseEmailModel generateFailedDownloadEmailModel(Download download, String portal) {
    GbifUser creator = getCreator(download);
    Locale locale = getLocale(creator);
    String downloadCreatedDate = String.format(Locale.ENGLISH, "%te %<tB %<tY", download.getCreated());

    try {
      DownloadTemplateDataModel dataModel =
          new DownloadTemplateDataModel(download, new URL(portal), getHumanQuery(download, locale), downloadCreatedDate);

      return emailTemplateProcessor.buildEmail(
          OccurrenceEmailType.FAILED_DOWNLOAD, getNotificationAddresses(download, creator), dataModel, locale);
    } catch (TemplateException | IOException e) {
      LOG.error(
          NOTIFY_ADMIN,
          "Rendering of notification Mail for download [{}] failed", download.getKey(), e);
    }

    return null;
  }

  private Locale getLocale(GbifUser creator) {
    return Optional.ofNullable(creator)
        .map(AbstractGbifUser::getLocale)
        .map(this::findSuitableLocaleTagAmongAvailable)
        .map(Locale::forLanguageTag)
        .orElse(Locale.ENGLISH);
  }

  /**
   * Gets a human readable version of the occurrence search query used.
   */
  public String getHumanQuery(Download download, Locale locale) {
    ResourceBundle bundle = ResourceBundle.getBundle("email/occurrence/messages", locale);
    try {
      String query =
          new HumanPredicateBuilder(titleLookup)
              .humanFilterString(((PredicateDownloadRequest) download.getRequest()).getPredicate());

      if ("{ }".equals(query)) {
        LOG.debug("Empty query was used");
        query = bundle.getString("download.query.all");
      }

      if (query.length() > 1000) {
        LOG.debug("Query is too long, abbreviate");
        query = query.substring(0, 1000) + bundle.getString("download.query.abbreviated");
      }
      return query;
    } catch (Exception e) {
      LOG.debug("Exception while getting human query: {}", e.getMessage());
      return bundle.getString("download.query.complex");
    }
  }

  /**
   * Gets the list of notification addresses from the download object.
   * If the list of addresses is empty, the email of the creator is used.
   */
  private List<Address> getNotificationAddresses(Download download, GbifUser creator) {
    List<Address> emails = new ArrayList<>();
    if (download.getRequest().getNotificationAddresses() == null
        || download.getRequest().getNotificationAddresses().isEmpty()) {
      if (creator != null) {
        try {
          emails.add(new InternetAddress(creator.getEmail()));
        } catch (AddressException e) {
          LOG.warn("Ignore corrupt email address {}", creator.getEmail());
        }
      }
    } else {
      emails = toInternetAddresses(download.getRequest().getNotificationAddresses());
    }
    return emails;
  }

  private GbifUser getCreator(Download download) {
    return identityAccessService.get(download.getRequest().getCreator());
  }

  private String findSuitableLocaleTagAmongAvailable(Locale locale) {
    return Locale.lookupTag(Locale.LanguageRange.parse(locale.toLanguageTag()), SUPPORTED_LOCALES);
  }
}
