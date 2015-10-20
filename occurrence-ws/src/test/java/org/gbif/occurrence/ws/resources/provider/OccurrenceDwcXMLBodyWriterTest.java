package org.gbif.occurrence.ws.resources.provider;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.vocabulary.Country;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.occurrence.ws.provider.DwcXMLDocument;
import org.gbif.occurrence.ws.provider.OccurrenceDwcXMLBodyWriter;
import org.gbif.utils.file.FileUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import javax.ws.rs.ext.MessageBodyWriter;

import com.google.common.base.CharMatcher;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

/**
 * Test for {@link OccurrenceDwcXMLBodyWriter} behavior.
 *
 */
public class OccurrenceDwcXMLBodyWriterTest {

  @Test
  public void testOccurrenceXML() throws IOException {
    MessageBodyWriter<Occurrence> occurrenceDwcXMLBodyWriter = new OccurrenceDwcXMLBodyWriter();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Occurrence occ = new Occurrence();

    occ.setCountry(Country.MADAGASCAR);
    occ.setVerbatimField(DwcTerm.verbatimLocality, "mad");
    occ.setReferences(URI.create("http://www.gbif.org"));

    Term customTerm = TermFactory.instance().findTerm("MyTerm");
    occ.setVerbatimField(customTerm, "MyTerm value");

    occurrenceDwcXMLBodyWriter.writeTo(occ, null, null, null, null, null, baos);

    String expectedContent = IOUtils.toString(new FileInputStream(FileUtils.getClasspathFile("dwc_xml/occurrence.xml")));
    assertEquals(CharMatcher.WHITESPACE.removeFrom(expectedContent), CharMatcher.WHITESPACE.removeFrom(baos.toString()));
  }
}
