package org.gbif.occurrence.persistence.util;

import org.gbif.api.util.IsoDateParsingUtils.IsoDateFormat;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.Term;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;


/**
 * Test class for ExtensionsUtil class.
 */
public class ExtensionsUtilTest {


  /**
   * Generates a test object.
   */
  private List<Map<Term, String>> getExtensionTest() {
    Map<Term, String> verbatimRecord = new HashMap<Term, String>();
    Date today = new Date();
    verbatimRecord.put(DcTerm.created, IsoDateFormat.FULL.getDateFormat().format(today));
    verbatimRecord.put(DcTerm.creator, "fede");
    verbatimRecord.put(DcTerm.description, "testDescription");
    verbatimRecord.put(DcTerm.format, "jpg");
    verbatimRecord.put(DcTerm.license, "licenseTest");
    verbatimRecord.put(DcTerm.publisher, "publisherTest");
    verbatimRecord.put(DcTerm.title, "titleTest");
    verbatimRecord.put(DcTerm.references, "http://www.gbif.org/");
    verbatimRecord.put(DcTerm.identifier, "http://www.gbif.org/");
    List<Map<Term, String>> verbatimRecords = Lists.newArrayList();
    verbatimRecords.add(verbatimRecord);
    return verbatimRecords;
  }

  /**
   * Test the serialization of a List<Map<Term, String>>.
   */
  @Test
  public void toJsonTest() {
    Assert.assertNotNull(ExtensionsUtil.toJson(getExtensionTest()));
  }

  /**
   * Test the deserialization of a List<Map<Term, String>>.
   */
  @Test
  public void fromJsonTest() {
    String jsonExtension = ExtensionsUtil.toJson(getExtensionTest());
    List<Map<Term, String>> serExtensions = ExtensionsUtil.fromJson(jsonExtension);
    Assert.assertNotNull(serExtensions);
    Assert.assertTrue(serExtensions.size() == 1);
  }
}
