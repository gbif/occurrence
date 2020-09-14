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

import org.gbif.occurrence.mail.util.OccurrenceMailUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.URLDataSource;
import javax.mail.Address;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import static org.gbif.occurrence.mail.util.OccurrenceMailUtils.EMAIL_SPLITTER;
import static org.gbif.occurrence.mail.util.OccurrenceMailUtils.toInternetAddresses;

/**
 * Allows to send {@link BaseEmailModel}
 */
@Service
public class OccurrenceEmailSender implements EmailSender {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceEmailSender.class);

  private static final String HTML_CONTENT_TYPE = "text/html; charset=UTF-8";

  private final Session session;
  private final JavaMailSender mailSender;
  private final Set<Address> bccAddresses;

  public OccurrenceEmailSender(
      Session session,
      JavaMailSender mailSender,
      @Value("${occurrence.download.mail.bcc}") String bccAddresses) {
    this.session = session;
    this.mailSender = mailSender;
    this.bccAddresses = new HashSet<>(toInternetAddresses(EMAIL_SPLITTER.split(bccAddresses)));
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
      final MimeMessage msg = new MimeMessage(session);
      msg.setFrom();
      msg.setRecipients(
          Message.RecipientType.TO, emailModel.getEmailAddresses().toArray(new Address[0]));
      msg.setRecipients(
          Message.RecipientType.BCC, bccAddresses.toArray(new Address[0]));
      msg.setSubject(emailModel.getSubject());
      msg.setSentDate(new Date());

      MimeMultipart multipart = new MimeMultipart();

      BodyPart htmlPart = new MimeBodyPart();
      htmlPart.setContent(emailModel.getBody(), HTML_CONTENT_TYPE);
      multipart.addBodyPart(htmlPart);

      BodyPart imagePart = new MimeBodyPart();
      imagePart.setDataHandler(new DataHandler(getImage()));
      imagePart.setHeader("Content-ID", "<image>");
      multipart.addBodyPart(imagePart);

      msg.setContent(multipart);

      mailSender.send(msg);
    } catch (MessagingException e) {
      LOG.error(
          OccurrenceMailUtils.NOTIFY_ADMIN,
          "Sending of notification Mail for [{}] failed",
          emailModel.getEmailAddresses(),
          e);
    }
  }

  private DataSource getImage() {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = this.getClass().getClassLoader();
    }
    return new URLDataSource(classLoader.getResource("email/images/GBIF-2015-full.jpg"));
  }
}
