package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.common.MediaObject;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.MediaType;
import org.gbif.dwc.terms.AcTerm;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.Maps;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class MultiMediaInterpreterTest {

  private SimpleDateFormat ISO = new SimpleDateFormat("yyyy-MM-dd");

  @Test
  public void testInterpretMediaCore() throws Exception {
    VerbatimOccurrence v = new VerbatimOccurrence();
    Occurrence o = new Occurrence();

    v.setVerbatimField(
      DwcTerm.associatedMedia,
      "http://farm8.staticflickr.com/7093/7039524065_3ed0382368.jpg, http://www.flickr.com/photos/70939559@N02/7039524065.png");
    MultiMediaInterpreter.interpretMedia(v, o);

    assertEquals(2, o.getMedia().size());
    for (MediaObject m : o.getMedia()) {
      assertEquals(MediaType.StillImage, m.getType());
      assertTrue(m.getFormat().startsWith("image/"));
      assertNotNull(m.getIdentifier());
    }
  }

  @Test
  public void testInterpretMediaExtension() throws Exception {
    VerbatimOccurrence v = new VerbatimOccurrence();
    Occurrence o = new Occurrence();

    List<Map<Term, String>> media = Lists.newArrayList();
    v.getExtensions().put(Extension.IMAGE, media);

    Map<Term, String> rec = Maps.newHashMap();
    rec.put(DcTerm.identifier, "http://farm8.staticflickr.com/7093/7039524065_3ed0382368.jpg");
    rec.put(DcTerm.references, "http://www.flickr.com/photos/70939559@N02/7039524065");
    rec.put(DcTerm.format, "jpg");
    rec.put(DcTerm.title, "Geranium Plume Moth 0032");
    rec.put(DcTerm.description, "Geranium Plume Moth 0032 description");
    rec.put(DcTerm.license, "BY-NC-SA 2.0");
    rec.put(DcTerm.creator, "Moayed Bahajjaj");
    rec.put(DcTerm.created, "2012-03-29");
    media.add(rec);

    rec = Maps.newHashMap();
    rec.put(DcTerm.identifier, "http://www.flickr.com/photos/70939559@N02/7039524065.jpg");
    media.add(rec);

    MultiMediaInterpreter.interpretMedia(v, o);

    assertEquals(2, o.getMedia().size());
    for (MediaObject m : o.getMedia()) {
      assertEquals(MediaType.StillImage, m.getType());
      assertNotNull(m.getIdentifier());
    }

    assertEquals(MediaType.StillImage, o.getMedia().get(0).getType());
    assertEquals("image/jpeg", o.getMedia().get(0).getFormat());
    assertEquals("Geranium Plume Moth 0032", o.getMedia().get(0).getTitle());
    assertEquals("Geranium Plume Moth 0032 description", o.getMedia().get(0).getDescription());
    assertEquals("http://creativecommons.org/licenses/by-nc-sa/2.0/", o.getMedia().get(0).getLicense());
    assertEquals("Moayed Bahajjaj", o.getMedia().get(0).getCreator());
    assertEquals("2012-03-29", ISO.format(o.getMedia().get(0).getCreated()));
    assertEquals("http://www.flickr.com/photos/70939559@N02/7039524065", o.getMedia().get(0).getReferences().toString());
    assertEquals("http://farm8.staticflickr.com/7093/7039524065_3ed0382368.jpg", o.getMedia().get(0).getIdentifier()
      .toString());
  }

  @Test
  public void testInterpretAudubonExtension() throws Exception {
    VerbatimOccurrence v = new VerbatimOccurrence();
    Occurrence o = new Occurrence();

    List<Map<Term, String>> media = Lists.newArrayList();
    v.getExtensions().put(Extension.AUDUBON, media);

    Map<Term, String> rec = Maps.newHashMap();
    rec.put(AcTerm.accessURI, "http://specify-attachments-saiab.saiab.ac.za/originals/sp6-3853933608872243693.att.JPG");
    rec.put(DcTerm.identifier, "d79633d3-0967-40fa-9557-d6915e4d1353");
    rec.put(DcTerm.format, "jpg");
    rec.put(DcTerm.title, "Geranium Plume Moth 0032");
    rec.put(DcTerm.description, "Geranium Plume Moth 0032 description");
    rec.put(AcTerm.derivedFrom, "http://farm8.staticflickr.com/7093/7039524065_3ed0382368.jpg");
    rec.put(DcTerm.license, "BY-NC-SA 2.0");
    rec.put(DcTerm.creator, "Moayed Bahajjaj");
    rec.put(DcTerm.created, "2012-03-29");
    media.add(rec);

    rec = Maps.newHashMap();
    rec.put(DcTerm.identifier, "http://www.flickr.com/photos/70939559@N02/7039524065.jpg");
    media.add(rec);

    MultiMediaInterpreter.interpretMedia(v, o);

    assertEquals(2, o.getMedia().size());
    for (MediaObject m : o.getMedia()) {
      assertEquals(MediaType.StillImage, m.getType());
      assertNotNull(m.getIdentifier());
    }

    assertEquals(MediaType.StillImage, o.getMedia().get(0).getType());
    assertEquals("image/jpeg", o.getMedia().get(0).getFormat());
    assertEquals("Geranium Plume Moth 0032", o.getMedia().get(0).getTitle());
    assertEquals("Geranium Plume Moth 0032 description", o.getMedia().get(0).getDescription());
    assertEquals("http://creativecommons.org/licenses/by-nc-sa/2.0/", o.getMedia().get(0).getLicense());
    assertEquals("Moayed Bahajjaj", o.getMedia().get(0).getCreator());
    assertEquals("2012-03-29", ISO.format(o.getMedia().get(0).getCreated()));
    assertEquals("http://specify-attachments-saiab.saiab.ac.za/originals/sp6-3853933608872243693.att.JPG", o.getMedia().get(0).getIdentifier()
            .toString());
  }

  /**
   * If the information about the same image (same URI) is given in the core AND the extension we should use the one
   * from the extension (richer data).
   * @throws Exception
   */
  @Test
  public void testExtensionsPriority() throws Exception {
    VerbatimOccurrence v = new VerbatimOccurrence();
    Occurrence o = new Occurrence();
    v.setVerbatimField(
            DwcTerm.associatedMedia,
            "http://farm8.staticflickr.com/7093/7039524065_3ed0382368.jpg, http://www.flickr.com/photos/70939559@N02/7039524065.png");

    List<Map<Term, String>> media = Lists.newArrayList();
    v.getExtensions().put(Extension.AUDUBON, media);

    Map<Term, String> rec = Maps.newHashMap();
    rec.put(AcTerm.accessURI, "http://farm8.staticflickr.com/7093/7039524065_3ed0382368.jpg");
    rec.put(DcTerm.format, "jpg");
    rec.put(DcTerm.title, "Geranium Plume Moth 0032");
    rec.put(DcTerm.description, "Geranium Plume Moth 0032 description");
    rec.put(AcTerm.derivedFrom, "http://farm8.staticflickr.com/7093/7039524065_3ed0382368.jpg");
    rec.put(DcTerm.license, "BY-NC-SA 2.0");
    rec.put(DcTerm.creator, "Moayed Bahajjaj");
    rec.put(DcTerm.created, "2012-03-29");
    media.add(rec);

    MultiMediaInterpreter.interpretMedia(v, o);

    assertEquals(2, o.getMedia().size());
    assertEquals("http://creativecommons.org/licenses/by-nc-sa/2.0/", o.getMedia().get(0).getLicense());
  }
}
