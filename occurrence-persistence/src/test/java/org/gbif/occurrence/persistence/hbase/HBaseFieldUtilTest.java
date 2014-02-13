package org.gbif.occurrence.persistence.hbase;

import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.IucnTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.UnknownTerm;
import org.gbif.occurrence.common.constants.FieldName;

import java.util.List;
import java.util.Set;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

/**
 *
 */
public class HBaseFieldUtilTest {

  /**
   * Verify all fieldnames are mapped and produce a unique column name
   * @throws Exception
   */
  @Test
  public void testUniqueColumns() throws Exception {
    Set<HBaseColumn> columns = Sets.newHashSet();
    for (Term t : allTerms()) {
      HBaseColumn col = HBaseFieldUtil.getHBaseColumn(t);
      assertNotNull("No hbase mapping for term " + t, col);
      assertFalse("Duplicate HBase column " + col, columns.contains(col));
      columns.add(col);
    }
    for (FieldName fn : FieldName.values()) {
      if (fn == FieldName.KEY) continue;
      HBaseColumn col = HBaseFieldUtil.getHBaseColumn(fn);
      assertNotNull("No hbase mapping for FieldName " + fn, col);
      assertFalse("Duplicate HBase column " + col, columns.contains(col));
      columns.add(col);
    }
    System.out.println(columns.size() + " unique HBase columns mapped");
  }

  private List<? extends Term> allTerms() {
    List<? extends Term> dwc = Lists.newArrayList(DwcTerm.values());
    List<? extends Term> dc = Lists.newArrayList(DcTerm.values());
    List<? extends Term> gbif = Lists.newArrayList(GbifTerm.values());
    List<? extends Term> iucn = Lists.newArrayList(IucnTerm.values());
    List<? extends Term> unknown = Lists.newArrayList(
      UnknownTerm.build("http://unknown.org/hareKrishna"),
      UnknownTerm.build("http://purl.org/germplasm/germplasmTerm#acquisitionID"),
      UnknownTerm.build("http://purl.org/germplasm/germplasmTerm#acquisitionRemarks"),
      UnknownTerm.build("http://rs.gbif.org/terms/1.0/haploidCount"),
      UnknownTerm.build("http://rs.tdwg.org/dwc/extension/dna/terms/geneticLocus"),
      UnknownTerm.build("http://rs.gbif.org/terms/1.0/temperature"),
      UnknownTerm.build("http://www.pliniancore.org/plic/pcfcore/typification"),
      UnknownTerm.build("http://www.eol.org/transfer/content/0.3/mimeType"),
      UnknownTerm.build("http://www.eol.org/transfer/content/0.3/license")
    );
    return Lists.newArrayList(Iterables.concat(dwc,dc,gbif,iucn, unknown));
  }

}
