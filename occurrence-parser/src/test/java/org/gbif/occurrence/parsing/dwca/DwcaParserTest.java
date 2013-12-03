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
package org.gbif.occurrence.parsing.dwca;

import org.gbif.occurrence.OccurrenceParser;
import org.gbif.occurrence.model.RawOccurrenceRecord;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DwcaParserTest {

  OccurrenceParser occParser;

  @Before
  public void setUp() {
    occParser = new OccurrenceParser();
  }

  @Test
  public void testArchiveUncompressed() {
    String fileName = getClass().getResource("/dwca/plants_dwca").getFile();
    List<RawOccurrenceRecord> records = new ArrayList<RawOccurrenceRecord>();
    for (RawOccurrenceRecord record : occParser.parseDwca(new File(fileName))) {
      records.add(record);
    }
    assertEquals(307, records.size());
  }

  @Test
  public void testArchiveZip() {
    String fileName = getClass().getResource("/dwca/plants.zip").getFile();
    List<RawOccurrenceRecord> records = new ArrayList<RawOccurrenceRecord>();
    for (RawOccurrenceRecord record : occParser.parseDwca(new File(fileName))) {
      records.add(record);
    }
    assertEquals(307, records.size());
  }

  @Test
  public void testArchiveTarGzip() {
    String fileName = getClass().getResource("/dwca/plants.tar.gz").getFile();
    List<RawOccurrenceRecord> records = new ArrayList<RawOccurrenceRecord>();
    for (RawOccurrenceRecord record : occParser.parseDwca(new File(fileName))) {
      records.add(record);
    }
    assertEquals(307, records.size());
  }

  @Test
  public void testSingleFileUncompressed() {
    String fileName = getClass().getResource("/dwca/archive.csv").getFile();
    List<RawOccurrenceRecord> records = new ArrayList<RawOccurrenceRecord>();
    for (RawOccurrenceRecord record : occParser.parseDwca(new File(fileName))) {
      records.add(record);
    }
    assertEquals(13, records.size());
  }

  @Test
  public void testSingleFileCompressed() {
    String fileName = getClass().getResource("/dwca/archive.csv.gz").getFile();
    List<RawOccurrenceRecord> records = new ArrayList<RawOccurrenceRecord>();
    for (RawOccurrenceRecord record : occParser.parseDwca(new File(fileName))) {
      records.add(record);
    }
    assertEquals(13, records.size());
  }

  @Test
  public void testDwcaParserStartingAtDwcaParser() {
    String fileName = getClass().getResource("/dwca/plants_dwca").getFile();
    BufferedDwcaParser parser = new BufferedDwcaParser(new File(fileName), 100);
    List<RawOccurrenceRecord> records = new ArrayList<RawOccurrenceRecord>();
    for (RawOccurrenceRecord record : parser) {
      records.add(record);
    }
    assertEquals(307, records.size());
  }


  // hudson ignores @Ignore ?
  //  @Test
  //  @Ignore
  //  public void testDwcaParserHugeFile() {
  //    File file = new File("/Users/oliver/SourceCode/dwca/unzipped/ebird-archive");
  //    OccurrenceParser occParser = new OccurrenceParser();
  //    List<RawOccurrenceRecord> records = new ArrayList<RawOccurrenceRecord>();
  //    int count = 0;
  //    for (RawOccurrenceRecord record : occParser.parseDwca(file)) {
  //      count++;
  //      if (count % 10000 == 0) System.out.println("Got count [" + count + "]");
  //    }
  //  }
}
