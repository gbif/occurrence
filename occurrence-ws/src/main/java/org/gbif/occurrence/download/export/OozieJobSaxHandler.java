package org.gbif.occurrence.download.export;

import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.occurrence.download.service.Constants;

import java.io.IOException;
import java.util.Set;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.codehaus.jackson.map.ObjectMapper;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * SAX Handler that can parse a single oozie download job definition and extract a download instance from it.
 * Extracts download properties using the Hadoop configuration conventions.
 *
 * <pre>
 * {@code
 * <configuration>
 *   <property>
 *     <name>property name</name>
 *     <value>property value</value>
 *   </property>
 * </configuration>}
 * </pre>
 */
public class OozieJobSaxHandler extends DefaultHandler {

  private static final String CONFIGURATION_ELEMENT = "configuration";
  private static final String PROPERTY_ELEMENT = "property";
  private static final String NAME_ELEMENT = "name";
  private static final String VALUE_ELEMENT = "value";
  // ObjectMappers are thread safe if not reconfigured in code
  private final ObjectMapper mapper = new ObjectMapper();
  private static final Splitter EMAIL_SPLITTER = Splitter.on(';').omitEmptyStrings().trimResults();

  // download data
  private String creator;
  private Predicate predicate;
  private Set<String> notificationAddresses;
  private boolean sendNotification;
  private DownloadFormat downloadFormat;

  private boolean inConfiguration;
  private boolean inProperty;
  private StringBuffer chars;
  private String propName;
  private String propValue;

  public DownloadRequest buildDownload() {
    return new DownloadRequest(predicate, creator, notificationAddresses, sendNotification, downloadFormat);
  }

  @Override
  public void characters(char[] ch, int start, int length) {
    chars.append(ch, start, length);
  }

  @Override
  public void endElement(String uri, String localName, String qName) throws SAXException {
    if (localName.equalsIgnoreCase(CONFIGURATION_ELEMENT)) {
      inConfiguration = false;
    }

    if (inProperty) {
      String content = Strings.emptyToNull(chars.toString().trim());
      if (localName.equals(PROPERTY_ELEMENT)) {
        inProperty = false;
        // interpret name & value
        if (propNameEquals(Constants.USER_PROPERTY)) {
          creator = propValue.trim();

        } else if (propNameEquals(Constants.NOTIFICATION_PROPERTY)) {
          notificationAddresses = Sets.newHashSet(EMAIL_SPLITTER.split(Strings.nullToEmpty(propValue)));

        } else if (propNameEquals(Constants.SEND_NOTIFICATION_PROPERTY)) {
          sendNotification = Boolean.parseBoolean(propValue);

        } else if (propNameEquals(Constants.FILTER_PROPERTY) && !Strings.isNullOrEmpty(propValue)) {
          try {
            predicate = mapper.readValue(propValue, Predicate.class);
          } catch (IOException e) {
            // TODO: Handle exception
          }
        }

      } else if (localName.equals(NAME_ELEMENT)) {
        propName = content;

      } else if (localName.equals(VALUE_ELEMENT)) {
        propValue = content;
      }

    }
  }

  @Override
  public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
    chars = new StringBuffer();
    if (localName.equalsIgnoreCase(CONFIGURATION_ELEMENT)) {
      inConfiguration = true;
    }
    if (inConfiguration && localName.equalsIgnoreCase(PROPERTY_ELEMENT)) {
      inProperty = true;
      propName = null;
      propValue = null;
    }
  }

  /**
   * Normalizes the current property name so that old and new property naming styles with . or _ are supported
   * and then does a case insensitive comparison against the desired name.
   */
  private boolean propNameEquals(String wantedPropName) {
    return propName.replace('.', '_').equalsIgnoreCase(wantedPropName);
  }
}
