package org.gbif.occurrence.mail;

import java.text.MessageFormat;
import java.util.Locale;
import java.util.ResourceBundle;

/**
 * Type of emails related to occurrence
 */
public enum OccurrenceEmailType implements EmailType {

  SUCCESSFUL_DOWNLOAD("successfulDownload", "successful_download.ftl"),

  FAILED_DOWNLOAD("failedDownload", "failed_download.ftl");

  private final String key;
  private final String template;

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
  public String getSubject(Locale locale, EmailType emailType, String... subjectParams) {
    ResourceBundle bundle = ResourceBundle.getBundle(OCCURRENCE_EMAIL_SUBJECTS_PATH, locale);
    String rawSubjectString = bundle.getString(emailType.getKey());
    if (subjectParams.length == 0) {
      return rawSubjectString;
    } else {
      return MessageFormat.format(rawSubjectString, (Object[]) subjectParams);
    }
  }
}
