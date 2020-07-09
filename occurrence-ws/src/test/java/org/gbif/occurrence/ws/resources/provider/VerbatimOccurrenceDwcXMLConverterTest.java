package org.gbif.occurrence.ws.resources.provider;

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.occurrence.ws.provider.OccurrenceVerbatimDwcXMLConverter;
import org.gbif.utils.file.FileUtils;

import java.io.FileInputStream;
import java.io.IOException;

import com.google.common.base.CharMatcher;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test for {@link OccurrenceVerbatimDwcXMLConverter} behavior.
 *
 */
public class VerbatimOccurrenceDwcXMLConverterTest {

  @Test
  public void testVerbatimOccurrenceXML() throws IOException{
    VerbatimOccurrence occ = new VerbatimOccurrence();

    occ.setVerbatimField(DwcTerm.verbatimLocality, "mad");
    Term customTerm = TermFactory.instance().findTerm("MyTerm");
    occ.setVerbatimField(customTerm, "MyTerm value");

    String expectedContent = IOUtils.toString(new FileInputStream(FileUtils.getClasspathFile("dwc_xml/verbatim_occurrence.xml")));
    assertEquals(CharMatcher.WHITESPACE.removeFrom(expectedContent), CharMatcher.WHITESPACE.removeFrom(OccurrenceVerbatimDwcXMLConverter.verbatimOccurrenceXMLAsString(occ)));
  }
}
