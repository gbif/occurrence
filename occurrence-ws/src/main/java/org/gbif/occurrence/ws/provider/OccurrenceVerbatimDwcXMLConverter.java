package org.gbif.occurrence.ws.provider;

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;

import java.io.StringWriter;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyAdvice;

/**
 * Custom {@link ResponseBodyAdvice} to serialize {@link VerbatimOccurrence} in DarwinCore XML.
 * We do not use JAXB annotations to keep it easy to manage dynamic properties like verbatim fields map.
 *
 */
public class OccurrenceVerbatimDwcXMLConverter {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceVerbatimDwcXMLConverter.class);

  /**
   * Transforms a {@link VerbatimOccurrence} object into a byte[] representing a XML document.
   *
   * @param occurrence
   * @return the {@link VerbatimOccurrence} as byte[]
   * @throws ResponseStatusException if something went wrong while generating the XML document
   */
  public static String verbatimOccurrenceXMLAsString(VerbatimOccurrence occurrence) throws ResponseStatusException {
    StringWriter result = new StringWriter();

    try {
      DwcXMLDocument dwcXMLDocument = DwcXMLDocument.newInstance(DwcTerm.Occurrence);
      for (Term term : occurrence.getVerbatimFields().keySet()) {
        dwcXMLDocument.tryAppend(term, occurrence.getVerbatimField(term));
      }

      Transformer transformer = TransformerFactory.newInstance().newTransformer();
      transformer.setOutputProperty(OutputKeys.INDENT, "yes");
      DOMSource source = new DOMSource(dwcXMLDocument.getDocument());
      transformer.transform(source, new StreamResult(result));
    } catch (ParserConfigurationException | TransformerException e) {
      LOG.error("Can't generate Dwc XML for VerbatimOccurrence [{}]", occurrence);
      throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
    return result.toString();
  }

}
