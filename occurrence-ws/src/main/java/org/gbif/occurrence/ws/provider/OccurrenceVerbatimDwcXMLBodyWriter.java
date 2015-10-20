package org.gbif.occurrence.ws.provider;

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom {@link MessageBodyWriter} to serialize {@link VerbatimOccurrence} in DarwinCore XML.
 * We do not use JAXB annotations to keep it easy to manage dynamic properties like verbatim fields map.
 *
 */
@Provider
@Produces(MediaType.APPLICATION_XML)
public class OccurrenceVerbatimDwcXMLBodyWriter implements MessageBodyWriter<VerbatimOccurrence> {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceVerbatimDwcXMLBodyWriter.class);

  /**
   * Transforms a {@link VerbatimOccurrence} object into a byte[] representing a XML document.
   *
   * @param occurrence
   * @return the {@link VerbatimOccurrence} as byte[]
   * @throws WebApplicationException if something went wrong while generating the XML document
   */
  private byte[] verbatimOccurrenceXMLAsByteArray(VerbatimOccurrence occurrence) throws WebApplicationException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    try {
      DwcXMLDocument dwcXMLDocument = DwcXMLDocument.newInstance(DwcTerm.Occurrence);
      for(Term term : occurrence.getVerbatimFields().keySet()){
        dwcXMLDocument.tryAppend(term, occurrence.getVerbatimField(term));
      }

      Transformer transformer = TransformerFactory.newInstance().newTransformer();
      transformer.setOutputProperty(OutputKeys.INDENT, "yes");
      DOMSource source = new DOMSource(dwcXMLDocument.getDocument());
      StreamResult result = new StreamResult(baos);
      transformer.transform(source, result);
    } catch (ParserConfigurationException | TransformerException e) {
      LOG.error("Can't generate Dwc XML for VerbatimOccurrence [{}]", occurrence);
      throw new WebApplicationException(e, Response.Status.INTERNAL_SERVER_ERROR);
    }
    return baos.toByteArray();
  }

  @Override
  public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
    return type == VerbatimOccurrence.class;
  }

  @Override
  public long getSize(VerbatimOccurrence occurrence, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
    // deprecated by JAX-RS 2.0 and ignored by Jersey runtime
    return -1L;
  }

  @Override
  public void writeTo(VerbatimOccurrence occurrence, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream) throws IOException, WebApplicationException {
    entityStream.write(verbatimOccurrenceXMLAsByteArray(occurrence));
  }

}
