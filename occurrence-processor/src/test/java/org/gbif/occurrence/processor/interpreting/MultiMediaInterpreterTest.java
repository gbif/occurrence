package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.common.MediaObject;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.MediaType;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;

import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.Maps;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class MultiMediaInterpreterTest {

  private SimpleDateFormat ISO = new SimpleDateFormat("yyyy-MM-dd");

  @Test
  public void testDetectType() throws Exception {
    assertEquals(MediaType.StillImage, MultiMediaInterpreter.detectType(buildMO("image/jp2", "abies_alba.jp2"))
      .getType());
    assertEquals(MediaType.StillImage, MultiMediaInterpreter.detectType(buildMO("image/jpeg", null)).getType());
    assertEquals(MediaType.StillImage, MultiMediaInterpreter.detectType(buildMO("image/jpg", null)).getType());
    assertEquals(MediaType.StillImage, MultiMediaInterpreter.detectType(buildMO(null, "abies_alba.jp2")).getType());
    assertEquals(MediaType.StillImage, MultiMediaInterpreter.detectType(buildMO(null, "abies_alba.jpg")).getType());
    assertEquals(MediaType.StillImage, MultiMediaInterpreter.detectType(buildMO(null, "abies_alba.JPG")).getType());
    assertEquals(MediaType.StillImage, MultiMediaInterpreter.detectType(buildMO(null, "abies_alba.JPeg")).getType());
    assertEquals(MediaType.StillImage, MultiMediaInterpreter.detectType(buildMO(null, "abies_alba.TIFF")).getType());

    assertEquals(MediaType.Sound, MultiMediaInterpreter.detectType(buildMO(null, "abies_alba.mp3")).getType());
    assertEquals(MediaType.Sound, MultiMediaInterpreter.detectType(buildMO(null, "abies_alba.flac")).getType());
    assertEquals(MediaType.Sound, MultiMediaInterpreter.detectType(buildMO(null, "abies_alba.ogg")).getType());
  }

  private MediaObject buildMO(String format, String uri) {
    MediaObject mo = new MediaObject();

    mo.setType(null);
    mo.setFormat(format);
    mo.setIdentifier(uri == null ? null : URI.create(uri));

    return mo;
  }

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
    assertEquals("BY-NC-SA 2.0", o.getMedia().get(0).getLicense());
    assertEquals("Moayed Bahajjaj", o.getMedia().get(0).getCreator());
    assertEquals("2012-03-29", ISO.format(o.getMedia().get(0).getCreated()));
    assertEquals("http://www.flickr.com/photos/70939559@N02/7039524065", o.getMedia().get(0).getReferences().toString());
    assertEquals("http://farm8.staticflickr.com/7093/7039524065_3ed0382368.jpg", o.getMedia().get(0).getIdentifier()
      .toString());
  }

  @Test
  public void testParseAssociatedMedia() throws Exception {
    assertEquals(0, MultiMediaInterpreter.parseAssociatedMedia(null).size());
    assertEquals(0, MultiMediaInterpreter.parseAssociatedMedia("").size());
    assertEquals(0, MultiMediaInterpreter.parseAssociatedMedia(" ").size());
    assertEquals(0, MultiMediaInterpreter.parseAssociatedMedia("-").size());

    assertEquals(1, MultiMediaInterpreter.parseAssociatedMedia("http://gbif.org/logo.png").size());
    assertEquals(1, MultiMediaInterpreter.parseAssociatedMedia(" http://gbif.org/logo.png").size());
    assertEquals(1, MultiMediaInterpreter.parseAssociatedMedia("www.gbif.org/logo.png").size());
    assertEquals(1, MultiMediaInterpreter.parseAssociatedMedia("www.gbif.org/image?id=12").size());
    assertEquals(1, MultiMediaInterpreter.parseAssociatedMedia("http://www.gbif.org/image?id=12").size());
    assertEquals(1, MultiMediaInterpreter.parseAssociatedMedia("http://www.gbif.org/image?id=12&format=gif,jpg").size());

    assertEquals(2, MultiMediaInterpreter.parseAssociatedMedia("http://gbif.org/logo.png, http://gbif.org/logo2.png")
      .size());
    assertEquals(2, MultiMediaInterpreter.parseAssociatedMedia("http://gbif.org/logo.png; http://gbif.org/logo2.png")
      .size());
    assertEquals(2, MultiMediaInterpreter.parseAssociatedMedia("http://gbif.org/logo.png | http://gbif.org/logo2.png")
      .size());
    assertEquals(2,
      MultiMediaInterpreter.parseAssociatedMedia("http://gbif.org/logo.png |#DELIMITER#| http://gbif.org/logo2.png")
        .size());

    assertEquals(
      3,
      MultiMediaInterpreter.parseAssociatedMedia(
        "http://gbif.org/logo.png, http://gbif.org/logo2.png, http://gbif.org/logo3.png").size());
  }


  @Test
  public void testParseMimeType() throws Exception {
    assertNull(MultiMediaInterpreter.parseMimeType((String) null));
    assertEquals("image/jp2", MultiMediaInterpreter.parseMimeType("image/jp2"));
    assertEquals("image/jpg", MultiMediaInterpreter.parseMimeType("image/jpg"));
    assertEquals("image/jpeg", MultiMediaInterpreter.parseMimeType("image/jpeg"));
    assertEquals("image/jpg", MultiMediaInterpreter.parseMimeType("image/JPG"));
    assertNull(MultiMediaInterpreter.parseMimeType("JPG"));
    assertNull(MultiMediaInterpreter.parseMimeType("gif"));
    assertNull(MultiMediaInterpreter.parseMimeType("tiff"));
    assertEquals("audio/mp3", MultiMediaInterpreter.parseMimeType("audio/mp3"));
    assertNull(MultiMediaInterpreter.parseMimeType("mp3"));
    assertEquals("audio/mp3", MultiMediaInterpreter.parseMimeType(" audio/mp3"));
    assertNull(MultiMediaInterpreter.parseMimeType("mpg"));


    assertNull(MultiMediaInterpreter.parseMimeType((URI) null));
    assertEquals("image/jp2", MultiMediaInterpreter.parseMimeType(URI.create("abies_alba.jp2")));
    assertEquals("image/jpeg", MultiMediaInterpreter.parseMimeType(URI.create("abies_alba.jpg")));
    assertEquals("image/jpeg", MultiMediaInterpreter.parseMimeType(URI.create("abies_alba.jpeg")));
    assertEquals("image/jpeg", MultiMediaInterpreter.parseMimeType(URI.create("abies_alba.JPG")));
    assertEquals("image/jpeg", MultiMediaInterpreter.parseMimeType(URI.create("abies_alba.JPG")));
    assertEquals("image/gif", MultiMediaInterpreter.parseMimeType(URI.create("abies_alba.gif")));
    assertEquals("image/tiff", MultiMediaInterpreter.parseMimeType(URI.create("abies_alba.tiff")));
    assertEquals("audio/mpeg", MultiMediaInterpreter.parseMimeType(URI.create("abies_alba.mp3")));
    assertEquals("video/mpeg", MultiMediaInterpreter.parseMimeType(URI.create("abies_alba.mpg")));
  }

}
