/*
 * Copyright 2011 Global Biodiversity Information Facility (GBIF)
 *
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
package org.gbif.occurrence;

import org.gbif.occurrence.model.RawOccurrenceRecord;
import org.gbif.occurrence.parsing.RawXmlOccurrence;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.zip.GZIPInputStream;

import com.google.common.io.Closeables;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class OccurrenceParserTest {

  private OccurrenceParser parser;

  @Before
  public void setUp() {
    parser = new OccurrenceParser();
  }

  @Test
  public void testAbcd12() {
    String fileName = getClass().getResource("/responses/abcd12/abcd12_all_simple_fields.gz").getFile();
    File file = new File(fileName);
    List<RawOccurrenceRecord> records = parser.parseResponseFileToRor(file);
    assertNotNull(records);
    assertEquals(1, records.size());
  }

  @Test
  public void testAbcd206() {
    String fileName = getClass().getResource("/responses/abcd206/abcd206_id_latin.gz").getFile();
    File file = new File(fileName);
    List<RawOccurrenceRecord> records = parser.parseResponseFileToRor(file);
    assertNotNull(records);
    assertEquals(1, records.size());
  }

  @Test
  public void testDwc10() {
    String fileName = getClass().getResource("/responses/dwc10/dwc10_typification.gz").getFile();
    File file = new File(fileName);
    List<RawOccurrenceRecord> records = parser.parseResponseFileToRor(file);
    assertNotNull(records);
    assertEquals(1, records.size());
  }

  @Test
  public void testDwc14() {
    String fileName = getClass().getResource("/responses/dwc14/dwc14_links.gz").getFile();
    File file = new File(fileName);
    List<RawOccurrenceRecord> records = parser.parseResponseFileToRor(file);
    assertNotNull(records);
    assertEquals(1, records.size());
  }

  @Test
  public void testDwcManis() {
    String fileName = getClass().getResource("/responses/dwc_manis/dwc_manis_all_simple_fields.gz").getFile();
    File file = new File(fileName);
    List<RawOccurrenceRecord> records = parser.parseResponseFileToRor(file);
    assertNotNull(records);
    assertEquals(1, records.size());
    assertEquals("513", records.get(0).getCatalogueNumber());
  }

  @Test
  public void testDwc2009() {
    String fileName = getClass().getResource("/responses/dwc2009/dwc2009_simple_fields.gz").getFile();
    File file = new File(fileName);
    List<RawOccurrenceRecord> records = parser.parseResponseFileToRor(file);
    assertNotNull(records);
    assertEquals(1, records.size());
    assertEquals("2004", records.get(0).getCatalogueNumber());
  }

  @Test
  public void testProblematic() {
    String fileName = getClass().getResource("/responses/problematic/dwc_10_utf8_badcase.gz").getFile();
    File file = new File(fileName);
    List<RawOccurrenceRecord> records = parser.parseResponseFileToRor(file);
    assertNotNull(records);
    assertEquals(21, records.size());
  }

  @Test
  public void testProblematic2() {
    String fileName = getClass().getResource("/responses/problematic/dwc_confused_charsets.gz").getFile();
    File file = new File(fileName);
    List<RawOccurrenceRecord> records = parser.parseResponseFileToRor(file);
    assertNotNull(records);
    assertEquals(1, records.size());
  }

  @Test
  public void testStreamAbcd12() throws IOException, ParsingException {
    testStreamParsing("/responses/abcd12/abcd12_all_simple_fields.gz", 1);
  }

  @Test
  public void testStreamAbcd206() throws IOException, ParsingException {
    testStreamParsing("/responses/abcd206/abcd206_id_latin.gz", 1);
  }

  @Test
  public void testStreamDwc10() throws IOException, ParsingException {
    testStreamParsing("/responses/dwc10/dwc10_typification.gz", 1);
  }

  @Test
  public void testStreamDwc14() throws IOException, ParsingException {
    testStreamParsing("/responses/dwc14/dwc14_links.gz", 1);
  }

  @Test
  public void testStreamDwcManis() throws IOException, ParsingException {
    testStreamParsing("/responses/dwc_manis/dwc_manis_all_simple_fields.gz", 1);
  }

  @Test
  public void testStreamDwc2009() throws IOException, ParsingException {
    testStreamParsing("/responses/dwc2009/dwc2009_simple_fields.gz", 1);
  }

  private void testStreamParsing(String name, int expected) throws IOException, ParsingException {
    InputStream tmpStream = getClass().getResource(name).openStream();
    GZIPInputStream is = new GZIPInputStream(tmpStream);
    try {
      List<RawXmlOccurrence> records = parser.parseStream(is);
      assertNotNull(records);
      assertEquals(expected, records.size());
    } finally {
      Closeables.closeQuietly(is);
      Closeables.closeQuietly(tmpStream);
    }
  }

}
