package org.gbif.occurrence.download.file.dwca;

import org.gbif.api.model.registry.Contact;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.vocabulary.ContactType;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.beanutils.PropertyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class used to manage contacts for DwcA download files.
 */
public class DwcaContactsUtil {

  private static final Logger LOG = LoggerFactory.getLogger(DwcaContactsUtil.class);

  private static final List<ContactType> AUTHOR_TYPES =
    ImmutableList.of(ContactType.ORIGINATOR, ContactType.AUTHOR, ContactType.POINT_OF_CONTACT);

  private static final Predicate<Contact> IS_AUTHOR_PREDICATE = contact -> AUTHOR_TYPES.contains(contact.getType()) || contact.isPrimary();

  /**
   * Utility method that creates a Contact with a limited number of fields.
   */
  protected static Contact createContact(String name, String email, ContactType type, boolean preferred) {
    return createContact(null, name, email, type, preferred);
  }

  /**
   * Creates a contact using the parameters.
   */
  protected static Contact createContact(String firstName, String lastName, String email, ContactType type,
                                         boolean preferred) {
    Contact contact = new Contact();
    contact.setEmail(Lists.newArrayList(email));
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
                LOG.error("Error setting provider contact", e);
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

  /**
   * Hidden constructor.
   */
  private DwcaContactsUtil() {
    //empty constructor
  }
}
