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

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.occurrence.ws.provider.OccurrenceVerbatimDwcXMLConverter;
import org.gbif.utils.file.FileUtils;

import java.io.FileInputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import com.google.common.base.CharMatcher;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test for {@link OccurrenceVerbatimDwcXMLConverter} behavior.
 */
@SuppressWarnings("UnstableApiUsage")
public class VerbatimOccurrenceDwcXMLConverterTest {

  @Test
  public void testVerbatimOccurrenceXML() throws IOException{
    VerbatimOccurrence occ = new VerbatimOccurrence();

    occ.setVerbatimField(DwcTerm.verbatimLocality, "mad");
    Term customTerm = TermFactory.instance().findTerm("MyTerm");
    occ.setVerbatimField(customTerm, "MyTerm value");

    String expectedContent = IOUtils.toString(
        new FileInputStream(FileUtils.getClasspathFile("dwc_xml/verbatim_occurrence.xml")));
    assertEquals(CharMatcher.WHITESPACE.removeFrom(expectedContent),
        CharMatcher.WHITESPACE.removeFrom(OccurrenceVerbatimDwcXMLConverter.verbatimOccurrenceXMLAsString(occ)));
  }
}
