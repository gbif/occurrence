package org.gbif.occurrence.mail;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.TimeZone;

@Configuration
public class OccurrenceFreemarkerConfiguration {

  @Bean
  public freemarker.template.Configuration occurrenceFreemarkerConfig() {
    // shared config among all instances
    freemarker.template.Configuration freemarkerConfig =
        new freemarker.template.Configuration(freemarker.template.Configuration.VERSION_2_3_25);

      freemarkerConfig.setDefaultEncoding(StandardCharsets.UTF_8.name());
      freemarkerConfig.setLocale(Locale.ENGLISH);
      freemarkerConfig.setTimeZone(TimeZone.getTimeZone("GMT"));
      freemarkerConfig.setNumberFormat("0.####");
      freemarkerConfig.setDateFormat("d MMMM yyyy");
      freemarkerConfig.setTimeFormat("HH:mm:ss");
      freemarkerConfig.setDateTimeFormat("HH:mm:ss d MMMM yyyy");
      freemarkerConfig.setClassForTemplateLoading(
          FreemarkerEmailTemplateProcessor.class, "/email/occurrence/templates");

      return freemarkerConfig;
  }
}
