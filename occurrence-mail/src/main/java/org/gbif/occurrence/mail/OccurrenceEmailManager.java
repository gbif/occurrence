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
import org.springframework.context.support.ResourceBundleMessageSource;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.gbif.occurrence.mail.util.OccurrenceMailUtils.NOTIFY_ADMIN;

/**
 * Manager handling the different types of email related to occurrence downloads.
 * Responsibilities (with the help of (via {@link FreemarkerEmailTemplateProcessor}): - decide where
 * to send the email (which address) - generate the body of the email
 */
@Service
public class OccurrenceEmailManager {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceEmailManager.class);

  private static final ResourceBundleMessageSource MESSAGE_SOURCE;

  // supported locales
  private static final List<String> SUPPORTED_LOCALES = Arrays.asList("en", "ru", "es");

  private final EmailTemplateProcessor emailTemplateProcessor;
  private final IdentityAccessService identityAccessService;
  private final TitleLookupService titleLookup;

  static {
    MESSAGE_SOURCE = new ResourceBundleMessageSource();
    MESSAGE_SOURCE.setBasename("email/messages");
    MESSAGE_SOURCE.setDefaultEncoding(StandardCharsets.UTF_8.displayName());
  }

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
    LOG.debug("Generating data for user email notification (successful download). " +
        "Download key is [{}], portal URL is [{}]", download.getKey(), portal);
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
    LOG.debug("Generating data for user email notification (failed download). " +
        "Download key is [{}], portal URL is [{}]", download.getKey(), portal);
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
    LOG.debug("Get creator's locale. Creator: {}", creator);
    Locale locale = Optional.ofNullable(creator)
        .map(AbstractGbifUser::getLocale)
        .map(this::findSuitableLocaleTagAmongAvailable)
        .map(Locale::forLanguageTag)
        .orElse(Locale.ENGLISH);

    LOG.debug("Creator's locale is [{}]", locale);
    return locale;
  }

  /**
   * Gets a human readable version of the occurrence search query used.
   */
  public String getHumanQuery(Download download, Locale locale) {
    try {
      String query =
          new HumanPredicateBuilder(titleLookup)
              .humanFilterString(((PredicateDownloadRequest) download.getRequest()).getPredicate());

      if ("{ }".equals(query)) {
        LOG.debug("Empty query was used");
        query = MESSAGE_SOURCE.getMessage("download.query.all", null, locale);
      }

      if (query.length() > 1000) {
        LOG.debug("Query is too long, abbreviate");
        query = query.substring(0, 1000) + MESSAGE_SOURCE.getMessage("download.query.abbreviated", null, locale);
      }
      return query;
    } catch (Exception e) {
      LOG.warn("Exception while getting human query: {}", e.getMessage());
      return MESSAGE_SOURCE.getMessage("download.query.complex", null, locale);
    }
  }

  /**
   * Gets the list of notification addresses from the download object.
   * If the list of addresses is empty, the email of the creator is used.
   */
  private Set<String> getNotificationAddresses(Download download, GbifUser creator) {
    LOG.debug("Get notification addresses. Download: [{}]", download.getKey());
    Set<String> emails = new HashSet<>();
    if (download.getRequest().getNotificationAddresses() == null
        || download.getRequest().getNotificationAddresses().isEmpty()) {
      if (creator != null) {
        emails.add(creator.getEmail());
      }
    } else {
      emails.addAll(download.getRequest().getNotificationAddresses());
    }

    LOG.debug("Notification addresses are: [{}]", emails);
    return emails;
  }

  private GbifUser getCreator(Download download) {
    String creator = download.getRequest().getCreator();
    LOG.debug("Download's creator name: [{}]", creator);
    GbifUser user = identityAccessService.get(creator);
    if (creator != null && user == null) {
      LOG.warn("User with name [{}] was not found!", creator);
    }
    return user;
  }

  private String findSuitableLocaleTagAmongAvailable(Locale locale) {
    LOG.debug("Trying to find a suitable locale tag for locale [{}]", locale);
    String localeTag = Locale.lookupTag(Locale.LanguageRange.parse(locale.toLanguageTag()), SUPPORTED_LOCALES);
    LOG.debug("Use locale tag [{}]", localeTag);
    return localeTag;
  }
}
