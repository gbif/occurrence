package org.gbif.occurrence.download.service.freemarker;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NiceDateTemplateMethodModelTest {

  @Test
  public void testFormat() throws Exception {
    assertEquals("17th December 2014", NiceDateTemplateMethodModel.format(Date.from(LocalDate.of(2014,12, 17).atStartOfDay(ZoneId.systemDefault()).toInstant())));
    assertEquals("1st December 2014", NiceDateTemplateMethodModel.format(Date.from(LocalDate.of(2014,12, 1).atStartOfDay(ZoneId.systemDefault()).toInstant())));
    assertEquals("2nd December 2014", NiceDateTemplateMethodModel.format(Date.from(LocalDate.of(2014,12, 2).atStartOfDay(ZoneId.systemDefault()).toInstant())));
    assertEquals("11th December 2014", NiceDateTemplateMethodModel.format(Date.from(LocalDate.of(2014,12, 11).atStartOfDay(ZoneId.systemDefault()).toInstant())));
    assertEquals("23rd December 2014", NiceDateTemplateMethodModel.format(Date.from(LocalDate.of(2014,12, 23).atStartOfDay(ZoneId.systemDefault()).toInstant())));
  }

}
