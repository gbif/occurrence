package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.common.MediaObject;
import org.gbif.api.vocabulary.MediaType;

import java.net.URI;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 *
 */
public class MultiMediaInterpreterTest {

  @Test
  public void testDetectType() throws Exception {
    assertEquals(MediaType.StillImage, MultiMediaInterpreter.detectType(buildMO("image/jp2", "abies_alba.jp2")).getType());
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

  private MediaObject buildMO(String format, String uri){
    MediaObject mo = new MediaObject();

    mo.setType(null);
    mo.setFormat(format);
    mo.setUrl(uri == null ? null : URI.create(uri));

    return mo;
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

    assertEquals(2, MultiMediaInterpreter.parseAssociatedMedia("http://gbif.org/logo.png, http://gbif.org/logo2.png").size());
    assertEquals(2, MultiMediaInterpreter.parseAssociatedMedia("http://gbif.org/logo.png; http://gbif.org/logo2.png").size());
    assertEquals(2, MultiMediaInterpreter.parseAssociatedMedia("http://gbif.org/logo.png | http://gbif.org/logo2.png").size());
    assertEquals(2, MultiMediaInterpreter.parseAssociatedMedia("http://gbif.org/logo.png |#DELIMITER#| http://gbif.org/logo2.png").size());

    assertEquals(3, MultiMediaInterpreter.parseAssociatedMedia("http://gbif.org/logo.png, http://gbif.org/logo2.png, http://gbif.org/logo3.png").size());
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
