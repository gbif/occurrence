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
package org.gbif.occurrence.ws.provider;

import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.google.common.base.Optional;

/**
 * Simple wrapper around {@link Document} to generate DarwinCore XML file.
 * This class is a candidate to be moved to the dwca-io project.
 *
 */
public class DwcXMLDocument {

  private static final String NS_URI = "http://www.w3.org/2000/xmlns/";
  private static final String NS_PREFIX = "xmlns:";

  /**
   * Currently private but could be exposed if needed elsewhere.
   * Could also be refactor if the Term interface expose a getPrefix and getNamespace methods.
   */
  private enum DwcXmlNamespace {

    DWC(DwcTerm.class, "dwc", "http://rs.tdwg.org/dwc/terms/"),
    DC(DcTerm.class, "dcterms", "http://purl.org/dc/terms/"),
    GBIF(GbifTerm.class, "gbif", "http://rs.gbif.org/terms/1.0/");

    private final Class<? extends Term> termClass;
    private final String prefix;
    private final String namespace;

    DwcXmlNamespace(Class<? extends Term> termClass, String prefix, String namespace) {
      this.termClass = termClass;
      this.prefix = prefix;
      this.namespace = namespace;
    }

    /**
     * Get a DwcXmlNamespace from {@Term}.
     *
     * @param term
     * @return corresponding DwcXmlNamespace of Optional.empty() if the provided term is not supported
     */
    public static Optional<DwcXmlNamespace> fromTerm(Term term) {
      for (DwcXmlNamespace dwcXmlNamespace : DwcXmlNamespace.values()) {
        if (dwcXmlNamespace.termClass.equals(term.getClass())) {
          return Optional.of(dwcXmlNamespace);
        }
      }
      return Optional.absent();
    }
  }

  private final Document doc;
  private final Element currentElement;

  /**
   * Private constructor, newInstance method should be used to get an instance.
   *
   * @param doc
   * @param rootElementTerm
   */
  private DwcXMLDocument(Document doc, DwcTerm rootElementTerm) {
    this.doc = doc;
    currentElement = createDwcXMLRootElement(rootElementTerm);
  }

  /**
   *  Creates a new DwcXMLDocument using the specified Term as root element.
   *
   * @param rootElementTerm
   * @return
   * @throws ParserConfigurationException
   */
  public static DwcXMLDocument newInstance(DwcTerm rootElementTerm) throws ParserConfigurationException {
    DocumentBuilderFactory icFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder icBuilder = icFactory.newDocumentBuilder();
    return new DwcXMLDocument(icBuilder.newDocument(), rootElementTerm);
  }

  /**
   *
   * @return newly created root Element
   */
  private Element createDwcXMLRootElement(DwcTerm rootElementTerm) {
    Element rootElement = doc.createElement(DwcXmlNamespace.DWC.prefix + ":" + rootElementTerm.simpleName());
    for (DwcXmlNamespace dwcXmlNS : DwcXmlNamespace.values()) {
      rootElement.setAttributeNS(NS_URI, NS_PREFIX + dwcXmlNS.prefix, dwcXmlNS.namespace);
    }
    doc.appendChild(rootElement);
    return rootElement;
  }

  public void append(DcTerm term, String value) {
    append(currentElement, DwcXmlNamespace.DC, term, value);
  }
  public void append(DwcTerm term, String value) {
    append(currentElement, DwcXmlNamespace.DWC, term, value);
  }
  public void append(GbifTerm term, String value) {
    append(currentElement, DwcXmlNamespace.GBIF, term, value);
  }

  /**
   * Get the underlying {@link Document}.
   *
   * @return
   */
  public Document getDocument() {
    return doc;
  }

  /**
   * Try to append the provided {@Term term} if it can be matched to a supported namespace.
   *
   * @param term
   * @param value
   * @return appended to the document or not
   */
  public boolean tryAppend(Term term, String value) {
    Optional<DwcXmlNamespace> dwcXmlNamespace = DwcXmlNamespace.fromTerm(term);
    if (dwcXmlNamespace.isPresent()) {
      append(currentElement, dwcXmlNamespace.get(), term, value);
    } else {
      return false;
    }
    return true;
  }

  /**
   * Appends the term,value to the document under the parentElement with dwcXmlNamespace. null values are simply ignored.
   *
   * @param parentElement
   * @param dwcXmlNamespace
   * @param term
   * @param value the value to add or null
   */
  private void append(Element parentElement, DwcXmlNamespace dwcXmlNamespace, Term term, String value) {
    if (value == null) {
      return;
    }

    Element node = doc.createElement(dwcXmlNamespace.prefix + ":" + term.simpleName());
    node.appendChild(doc.createTextNode(value));
    parentElement.appendChild(node);
  }

}
