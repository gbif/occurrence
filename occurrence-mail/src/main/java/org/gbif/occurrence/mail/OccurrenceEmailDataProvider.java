package org.gbif.occurrence.mail;

import org.springframework.stereotype.Service;

import java.text.MessageFormat;
import java.util.Locale;
import java.util.ResourceBundle;

@Service
public class OccurrenceEmailDataProvider implements EmailDataProvider {

  public static final String OCCURRENCE_EMAIL_SUBJECTS_PATH = "email/subjects/occurrence_email_subjects";

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

  @Override
  public String getTemplate(Locale locale, EmailType emailType) {
    ResourceBundle bundle = ResourceBundle.getBundle(EMAIL_TEMPLATES_PATH, locale);
    return bundle.getString(emailType.getKey());
  }
}
