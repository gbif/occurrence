package org.gbif.occurrence.download.service.freemarker;

import java.util.Date;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NiceDateTemplateMethodModelTest {

  @Test
  public void testFormat() throws Exception {
    NiceDateTemplateMethodModel tm = new NiceDateTemplateMethodModel();
    assertEquals("17th December 2014", tm.format(new Date(1418828850114l)));
    assertEquals("1st December 2014", tm.format(new Date(1417388400000l)));
    assertEquals("2nd December 2014", tm.format(new Date(1417474800000l)));
    assertEquals("11th December 2014", tm.format(new Date(1418252400000l)));
    assertEquals("23rd December 2014", tm.format(new Date(1419289200000l)));
  }
}
