package org.gbif.occurrence.mail;

import freemarker.template.Configuration;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service
public class OccurrenceEmailTemplateProcessor extends FreemarkerEmailTemplateProcessor {

  public OccurrenceEmailTemplateProcessor(
      @Qualifier("occurrenceFreemarkerConfig") Configuration freemarkerConfig) {
    super(freemarkerConfig);
  }
}
