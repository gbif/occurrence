/*
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
package org.gbif.occurrence.download.file.dwca.archive;

import org.gbif.api.model.registry.Contact;
import org.gbif.api.vocabulary.ContactType;

import com.google.common.collect.Lists;

import lombok.experimental.UtilityClass;

/**
 * Utility class used to manage contacts for DwcA download files.
 */
@UtilityClass
public class DwcaContactsUtil {
  /**
   * Utility method that creates a Contact with a limited number of fields.
   */
  static Contact createContact(String name, String email, ContactType type, boolean preferred) {
    return createContact(null, name, email, type, preferred);
  }

  /**
   * Creates a contact using the parameters.
   */
  static Contact createContact(String firstName, String lastName, String email, ContactType type,
                               boolean preferred) {
    Contact contact = new Contact();
    contact.setEmail(Lists.newArrayList(email));
    contact.setFirstName(firstName);
    contact.setLastName(lastName);
    contact.setType(type);
    contact.setPrimary(preferred);
    return contact;
  }
}
