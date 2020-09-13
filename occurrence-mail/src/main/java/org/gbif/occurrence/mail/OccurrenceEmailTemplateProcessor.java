package org.gbif.occurrence.mail;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
@Qualifier("occurrenceEmailTemplateProcessor")
public class OccurrenceEmailTemplateProcessor extends FreemarkerEmailTemplateProcessor {

  private final EmailDataProvider emailDataProvider;

  public OccurrenceEmailTemplateProcessor(
      @Qualifier("occurrenceEmailDataProvider") EmailDataProvider emailDataProvider) {
    this.emailDataProvider = emailDataProvider;
  }

  @Override
  public EmailDataProvider getEmailDataProvider() {
    return emailDataProvider;
  }
}
