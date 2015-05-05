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
package org.gbif.occurrence.parsing.xml;

import org.gbif.occurrence.OccurrenceParser;
import org.gbif.occurrence.model.IdentifierRecord;
import org.gbif.occurrence.model.ImageRecord;
import org.gbif.occurrence.model.LinkRecord;
import org.gbif.occurrence.model.RawOccurrenceRecord;
import org.gbif.occurrence.model.TypificationRecord;
import org.gbif.occurrence.parsing.RawXmlOccurrence;

import java.io.File;
import java.util.List;

import org.junit.Before;

public abstract class ParserTestCase {

  protected OccurrenceParser occurrenceParser;

  @Before
  public void setUp() {
    occurrenceParser = new OccurrenceParser();
  }

  protected List<RawOccurrenceRecord> setupRor(String fileName) {
    File response = new File(fileName);
    RawXmlOccurrence xmlRecord = occurrenceParser.parseResponseFileToRawXml(response).get(0);
//    System.out.println("got raw record:\n" + xmlRecord.getXml());
    List<RawOccurrenceRecord> records = XmlFragmentParser.parseRecord(xmlRecord);

    return records;
  }

  protected void showIdentifiers(RawOccurrenceRecord ror) {
    System.out.println("got [" + ror.getIdentifierRecords().size() + "] identifier records");
    for (IdentifierRecord idRec : ror.getIdentifierRecords()) {
      System.out.println("IdRec type [" + idRec.getIdentifierType() + "] identifier [" + idRec.getIdentifier() + "]");
    }
  }

  protected void showTaxons(RawOccurrenceRecord ror) {
    System.out.println("got taxons:");
    System.out.println("Kingdom: [" + ror.getKingdom() + "]");
    System.out.println("Phylum: [" + ror.getPhylum() + "]");
    System.out.println("Class: [" + ror.getKlass() + "]");
    System.out.println("Order: [" + ror.getOrder() + "]");
    System.out.println("Family: [" + ror.getFamily() + "]");
    System.out.println("Genus: [" + ror.getGenus() + "]");
    System.out.println("Species: [" + ror.getSpecies() + "]");
    System.out.println("Subspecies: [" + ror.getSubspecies() + "]");
  }

  protected void showTypifications(RawOccurrenceRecord ror) {
    System.out.println("got [" + ror.getTypificationRecords().size() + "] typification records");
    for (TypificationRecord typRec : ror.getTypificationRecords()) {
      System.out.println(typRec.debugDump());
    }
  }

  protected void showImages(RawOccurrenceRecord ror) {
    System.out.println("got [" + ror.getImageRecords().size() + "] image records");
    for (ImageRecord image : ror.getImageRecords()) {
      System.out.println(image.debugDump());
    }
  }

  protected void showLinks(RawOccurrenceRecord ror) {
    System.out.println("got [" + ror.getLinkRecords().size() + "] link records");
    for (LinkRecord link : ror.getLinkRecords()) {
      System.out.println(link.debugDump());
    }
  }

}
