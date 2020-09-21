/*
 * Copyright 2020 Global Biodiversity Information Facility (GBIF)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.mail;

import com.google.common.collect.Sets;
import org.gbif.occurrence.mail.util.OccurrenceMailUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;
import java.util.Date;
import java.util.Set;

import static org.gbif.occurrence.mail.util.OccurrenceMailUtils.EMAIL_SPLITTER;

/**
 * Allows to send {@link BaseEmailModel}
 */
@Service
public class OccurrenceEmailSender implements EmailSender {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceEmailSender.class);

  private final JavaMailSender mailSender;
  private final String fromAddress;
  private final Set<String> bccAddresses;

  @Value("classpath:email/images/GBIF-2015-full.jpg")
  private Resource logoFile;

  public OccurrenceEmailSender(
      JavaMailSender mailSender,
      @Value("${occurrence.download.mail.from}") String fromAddress,
      @Value("${occurrence.download.mail.bcc}") String bccAddresses) {
    this.mailSender = mailSender;
    this.fromAddress = fromAddress;
    this.bccAddresses = Sets.newHashSet(EMAIL_SPLITTER.split(bccAddresses));
  }

  /**
   * Method that generates (using a template) and send an email containing a username and a
   * challenge code. This method will generate an HTML email.
   */
  @Override
  public void send(BaseEmailModel emailModel) {
    if (emailModel == null) {
      LOG.warn("Email model is null, skip email sending");
      return;
    }

    if (emailModel.getEmailAddresses().isEmpty() && bccAddresses.isEmpty()) {
      LOG.warn("No valid notification addresses given for download");
      return;
    }

    prepareAndSend(emailModel);
  }

  private void prepareAndSend(BaseEmailModel emailModel) {
    try {
      // Send E-Mail
      final MimeMessage msg = mailSender.createMimeMessage();
      MimeMessageHelper helper = new MimeMessageHelper(msg, true, "UTF-8");

      helper.setFrom(fromAddress);
      helper.setTo(emailModel.getEmailAddresses().toArray(new String[0]));
      helper.setBcc(bccAddresses.toArray(new String[0]));
      helper.setSubject(emailModel.getSubject());
      helper.setSentDate(new Date());
      helper.setText(emailModel.getBody(), true);
      helper.addInline("logo.png", logoFile);

      mailSender.send(msg);
    } catch (MessagingException e) {
      LOG.error(
          OccurrenceMailUtils.NOTIFY_ADMIN,
          "Sending of notification Mail for [{}] failed",
          emailModel.getEmailAddresses(),
          e);
    }
  }
}
