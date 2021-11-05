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
package org.gbif.occurrence.ws.resources.provider;

import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.occurrence.ws.provider.DwcXMLDocument;

import javax.xml.parsers.ParserConfigurationException;

import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for {@link DwcXMLDocument} behavior.
 */
public class DwcXMLDocumentTest {

  @Test
  public void testDwcXMLDocumentTryAppend() throws ParserConfigurationException {
    DwcXMLDocument doc = DwcXMLDocument.newInstance(DwcTerm.Occurrence);
    assertTrue(doc.tryAppend(DwcTerm.behavior, "calm"));
    // This UnknownTerm should be ignored
    assertFalse(doc.tryAppend(TermFactory.instance().findTerm("myTerm"), "my term value"));

    Document xmlDoc = doc.getDocument();
    assertEquals(1, xmlDoc.getChildNodes().getLength());

    Node occurrenceElement = xmlDoc.getChildNodes().item(0);
    assertEquals(1, occurrenceElement.getChildNodes().getLength(), "Occurrence element has only one child");
    Node firstChild = occurrenceElement.getFirstChild();

    assertEquals("dwc" + ":" + DwcTerm.behavior.simpleName(), firstChild.getNodeName());
    assertEquals("calm", firstChild.getTextContent());
  }

}
