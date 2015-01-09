package org.gbif.occurrence.download.oozie;

import org.gbif.occurrence.query.TitleLookup;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ArchiveBuilderTest {
  TitleLookup tl;
  String query;

  @Before
  public void init() throws Exception {
    TitleLookup tl = mock(TitleLookup.class);
    when(tl.getDatasetTitle(Matchers.<String>any())).thenReturn("The little Mermaid");
    when(tl.getSpeciesName(Matchers.<String>any())).thenReturn("Abies alba Mill.");

    query = "{\"type\":\"and\",\"predicates\":[{\"type\":\"or\",\"predicates\":[{\"type\":\"equals\",\"key\":\"TAXON_KEY\",\"value\":\"4408732\"},{\"type\":\"equals\",\"key\":\"TAXON_KEY\",\"value\":\"2490613\"},{\"type\":\"equals\",\"key\":\"TAXON_KEY\",\"value\":\"2494708\"},{\"type\":\"equals\",\"key\":\"TAXON_KEY\",\"value\":\"5231198\"},{\"type\":\"equals\",\"key\":\"TAXON_KEY\",\"value\":\"2490669\"},{\"type\":\"equals\",\"key\":\"TAXON_KEY\",\"value\":\"2492606\"},{\"type\":\"equals\",\"key\":\"TAXON_KEY\",\"value\":\"2492371\"},{\"type\":\"equals\",\"key\":\"TAXON_KEY\",\"value\":\"2494642\"},{\"type\":\"equals\",\"key\":\"TAXON_KEY\",\"value\":\"5231209\"},{\"type\":\"equals\",\"key\":\"TAXON_KEY\",\"value\":\"5231190\"},{\"type\":\"equals\",\"key\":\"TAXON_KEY\",\"value\":\"2494155\"},{\"type\":\"equals\",\"key\":\"TAXON_KEY\",\"value\":\"2491557\"},{\"type\":\"equals\",\"key\":\"TAXON_KEY\",\"value\":\"2490604\"},{\"type\":\"equals\",\"key\":\"TAXON_KEY\",\"value\":\"2491506\"},{\"type\":\"equals\",\"key\":\"TAXON_KEY\",\"value\":\"2482501\"},{\"type\":\"equals\",\"key\":\"TAXON_KEY\",\"value\":\"5229493\"},{\"type\":\"equals\",\"key\":\"TAXON_KEY\",\"value\":\"2491544\"},{\"type\":\"equals\",\"key\":\"TAXON_KEY\",\"value\":\"2491534\"},{\"type\":\"equals\",\"key\":\"TAXON_KEY\",\"value\":\"2490681\"},{\"type\":\"equals\",\"key\":\"TAXON_KEY\",\"value\":\"2492462\"},{\"type\":\"equals\",\"key\":\"TAXON_KEY\",\"value\":\"2494422\"},{\"type\":\"equals\",\"key\":\"TAXON_KEY\",\"value\":\"2482492\"}]},{\"type\":\"within\",\"geometry\":\"POLYGON((-10.063476 43.992814,-10.063476 35.889050,5.317382 35.889050,5.317382 43.992814,-10.063476 43.992814))\"},{\"type\":\"or\",\"predicates\":[{\"type\":\"equals\",\"key\":\"COUNTRY\",\"value\":\"PT\"},{\"type\":\"equals\",\"key\":\"COUNTRY\",\"value\":\"ES\"}]}]}";
  }

  @Test
  public void testGetDatasetDescription() throws Exception {
    ArchiveBuilder ab = new ArchiveBuilder("abc", null, query, null, null, null, null, null, null, null, null, null,
      null, null, null, "http://api.gbif.org/v1/occurrence/download/request/0008474-141123120432318.zip", tl, false);
    assertEquals("A dataset containing all occurrences available in GBIF matching the query:<br/>\n"
                 + "TaxonKey: Abies alba Mill. or Abies alba Mill. or Abies alba Mill. or Abies alba Mill. or Abies alba Mill. or Abies alba Mill. or Abies alba Mill. or Abies alba Mill. or Abies alba Mill. or Abies alba Mill. or Abies alba Mill. or Abies alba Mill. or Abies alba Mill. or Abies alba Mill. or Abies alba Mill. or Abies alba Mill. or Abies alba Mill. or Abies alba Mill. or Abies alba Mill. or Abies alba Mill. or Abies alba Mill. or Abies alba Mill. \n"
                 + "Geometry: POLYGON((-10.063476 43.992814,-10.063476 35.889050,5.317382 35.889050,5.317382 43.992814,-10.063476 43.992814)) \n"
                 + "Country: Portugal or Spain"
                 + "<br/>\nThe dataset includes records from the following constituent datasets. "
                 + "The full metadata for each constituent is also included in this archive:<br/>\n",
      ab.getDatasetDescription());
  }

}