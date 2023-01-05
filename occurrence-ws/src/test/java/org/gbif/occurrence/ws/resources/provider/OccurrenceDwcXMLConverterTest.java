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

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.vocabulary.Country;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.occurrence.ws.provider.OccurrenceDwcXMLConverter;
import org.gbif.utils.file.FileUtils;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import com.google.common.base.CharMatcher;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test for {@link OccurrenceDwcXMLConverter} behavior.
 */
@SuppressWarnings("UnstableApiUsage")
public class OccurrenceDwcXMLConverterTest {

  @Test
  public void testOccurrenceXML() throws IOException {
    Occurrence occ = new Occurrence();

    occ.setCountry(Country.MADAGASCAR);
    occ.setVerbatimField(DwcTerm.verbatimLocality, "mad");
    occ.setReferences(URI.create("http://www.gbif.org"));

    Term customTerm = TermFactory.instance().findTerm("MyTerm");
    occ.setVerbatimField(customTerm, "MyTerm value");

    String expectedContent = IOUtils.toString(Files.newInputStream(FileUtils.getClasspathFile("dwc_xml/occurrence.xml")
                                                                     .toPath()));
    assertEquals(CharMatcher.whitespace().removeFrom(expectedContent),
        CharMatcher.whitespace().removeFrom(OccurrenceDwcXMLConverter.occurrenceXMLAsString(occ)));
  }
}
