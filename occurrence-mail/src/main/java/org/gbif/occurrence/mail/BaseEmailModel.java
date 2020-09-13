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

import com.google.common.base.MoreObjects;

import javax.mail.Address;
import java.util.Collections;
import java.util.List;

/** Very basic email model that holds the main components of an email to send. */
public class BaseEmailModel {

  private final List<Address> emailAddresses;
  private final String subject;
  private final String body;
  private final List<String> ccAddresses;

  public BaseEmailModel(List<Address> emailAddresses, String subject, String body) {
    this(emailAddresses, subject, body, Collections.emptyList());
  }

  public BaseEmailModel(
      List<Address> emailAddresses, String subject, String body, List<String> ccAddresses) {
    this.emailAddresses = emailAddresses != null ? emailAddresses : Collections.emptyList();
    this.subject = subject;
    this.body = body;
    this.ccAddresses = ccAddresses != null ? ccAddresses : Collections.emptyList();
  }

  public List<Address> getEmailAddresses() {
    return emailAddresses;
  }

  public String getSubject() {
    return subject;
  }

  public String getBody() {
    return body;
  }

  public List<String> getCcAddresses() {
    return ccAddresses;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("emailAddresses", emailAddresses)
        .add("subject", subject)
        .add("body", body)
        .add("ccAddresses", ccAddresses)
        .toString();
  }
}
