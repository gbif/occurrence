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

import lombok.extern.slf4j.Slf4j;
import org.gbif.api.model.registry.Contact;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.vocabulary.ContactType;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.commons.beanutils.PropertyUtils;

import lombok.experimental.UtilityClass;

/**
 * Utility class used to manage contacts for DwcA download files.
 */
@UtilityClass
@Slf4j
public class DwcaContactsUtil {

  private static final Set<ContactType> AUTHOR_TYPES =
    Set.of(ContactType.ORIGINATOR, ContactType.AUTHOR, ContactType.POINT_OF_CONTACT);

  private static final Predicate<Contact> IS_AUTHOR_PREDICATE = contact -> AUTHOR_TYPES.contains(contact.getType()) || contact.isPrimary();

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
    contact.setEmail(new ArrayList<>(List.of(email)));
    contact.setFirstName(firstName);
    contact.setLastName(lastName);
    contact.setType(type);
    contact.setPrimary(preferred);
    return contact;
  }

  /**
   * Checks the contacts of a dataset and finds the preferred contact that should be used as the main author
   * of a dataset.
   *
   * @return preferred author contact or null
   */
  public static Optional<Contact> getContentProviderContact(Dataset dataset) {
    return findFirstAuthor(dataset).map(author-> {
              Contact provider = null;
              try {
                provider = new Contact();
                PropertyUtils.copyProperties(provider, author);
                provider.setKey(null);
                provider.setType(ContactType.CONTENT_PROVIDER);
                provider.setPrimary(false);
              } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                log.error("Error setting provider contact", e);
              }
              return provider;
              }
            );

  }

  /**
   * Iterates over the dataset contacts to find the first contact of author type.
   */
  private static Optional<Contact> findFirstAuthor(Dataset dataset) {
     return dataset.getContacts().stream().filter(IS_AUTHOR_PREDICATE).findFirst();
  }

}
