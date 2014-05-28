package org.gbif.occurrence;

import org.gbif.occurrence.model.RawOccurrenceRecord;
import org.gbif.occurrence.parsing.RawXmlOccurrence;
import org.gbif.occurrence.parsing.xml.XmlFragmentParser;

import java.io.File;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class DigesterLogTest {

  private OccurrenceParser occurrenceParser = new OccurrenceParser();

  @Test
  public void testCommonLoggin() {
    Log log = LogFactory.getLog("org.apache.commons.digester.Digester");
    assertTrue(log.isDebugEnabled());
    log.error("Fail logger");

    try {
      int x = 7 / 0;
    } catch (Exception e) {
      log.error("Fail logger, fail", e);
    }
  }

  @Test
  public void testParseBasicFields() {
    File response = new File(getClass().getResource("/digester_bad.xml.gz").getFile());
    RawXmlOccurrence xmlRecord = occurrenceParser.parseResponseFileToRawXml(response).get(0);
    System.out.println("got raw record:\n" + xmlRecord.getXml());
    List<RawOccurrenceRecord> records = XmlFragmentParser.parseRecord(xmlRecord);
    for (RawOccurrenceRecord r : records) {
      System.out.println("got record:\n" + r);
    }
  }
}
