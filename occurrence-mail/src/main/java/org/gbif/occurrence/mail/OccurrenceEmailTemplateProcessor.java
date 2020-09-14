package org.gbif.occurrence.mail;

import org.springframework.stereotype.Service;

@Service
public class OccurrenceEmailTemplateProcessor extends FreemarkerEmailTemplateProcessor {

  private final EmailDataProvider emailDataProvider;

  public OccurrenceEmailTemplateProcessor(EmailDataProvider emailDataProvider) {
    this.emailDataProvider = emailDataProvider;
  }

  @Override
  public EmailDataProvider getEmailDataProvider() {
    return emailDataProvider;
  }
}
