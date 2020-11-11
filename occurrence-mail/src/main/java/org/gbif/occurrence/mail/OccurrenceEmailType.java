package org.gbif.occurrence.mail;

import java.nio.charset.StandardCharsets;
import java.util.Locale;

import org.springframework.context.support.ResourceBundleMessageSource;

/**
 * Type of emails related to occurrence
 */
public enum OccurrenceEmailType implements EmailType {

  SUCCESSFUL_DOWNLOAD("successfulDownload", "successful_download.ftl"),

  FAILED_DOWNLOAD("failedDownload", "failed_download.ftl");

  private static final ResourceBundleMessageSource MESSAGE_SOURCE;

  private final String key;
  private final String template;

  static {
    MESSAGE_SOURCE = new ResourceBundleMessageSource();
    MESSAGE_SOURCE.setBasename("email/subjects/occurrence_email_subjects");
    MESSAGE_SOURCE.setDefaultEncoding(StandardCharsets.UTF_8.displayName());
  }

  OccurrenceEmailType(String key, String template) {
    this.key = key;
    this.template = template;
  }

  @Override
  public String getKey() {
    return key;
  }

  @Override
  public String getTemplate() {
    return template;
  }

  @Override
  public String getSubject(Locale locale, String... subjectParams) {
    return MESSAGE_SOURCE.getMessage(this.getKey(), subjectParams, locale);
  }
}
