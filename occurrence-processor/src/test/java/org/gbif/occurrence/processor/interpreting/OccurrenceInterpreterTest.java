package org.gbif.occurrence.processor.interpreting;

import com.google.common.collect.Lists;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.model.registry.Dataset;
import org.gbif.api.model.registry.Organization;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.License;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.processor.guice.OccurrenceProcessorModule;
import org.gbif.occurrence.processor.guice.ProcessorConfiguration;
import org.gbif.occurrence.processor.interpreting.result.OccurrenceInterpretationResult;

import java.net.URI;
import java.util.UUID;

import com.google.common.collect.Sets;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OccurrenceInterpreterTest {

  private static class OccurrenceProcessorMockModule extends AbstractModule {

    private ProcessorConfiguration cfg;

    public OccurrenceProcessorMockModule(ProcessorConfiguration cfg){
      this.cfg = cfg;
    }
    @Override
    protected void configure() {
      Organization organizationMock = new Organization();
      organizationMock.setKey(UUID.randomUUID());
      organizationMock.setTitle("Mock organization");
      organizationMock.setCountry(Country.DENMARK);
      Dataset mockDataset = new Dataset();
      mockDataset.setPublishingOrganizationKey(organizationMock.getKey());
      mockDataset.setInstallationKey(UUID.randomUUID());
      mockDataset.setLicense(License.CC_BY_4_0);
      DatasetInfoInterpreter datasetInfoInterpreterMock = Mockito.mock(DatasetInfoInterpreter.class);
      Mockito.when(datasetInfoInterpreterMock.getDatasetData(Mockito.anyObject())).thenReturn(
              new DatasetInfoInterpreter.DatasetCacheData(mockDataset, Lists.newArrayList(), organizationMock));
      bind(DatasetInfoInterpreter.class).toInstance(datasetInfoInterpreterMock);
      install(new OccurrenceProcessorModule(cfg));
    }
  }

  private static OccurrenceInterpreter occurrenceInterpreter;

  @BeforeClass
  public static void initOccurrenceInterpreter() {
    ProcessorConfiguration cfg = new ProcessorConfiguration();
    cfg.api.url = URI.create("http://api.gbif-dev.org/v1/");
    Injector injector = Guice.createInjector(new OccurrenceProcessorMockModule(cfg));
    occurrenceInterpreter = injector.getInstance(OccurrenceInterpreter.class);
  }

  @Test
  public void testOccurrenceInterpretation() {
    VerbatimOccurrence verbatimOccurrence = new VerbatimOccurrence();
    verbatimOccurrence.setVerbatimField(DwcTerm.verbatimLatitude, "10.123");
    verbatimOccurrence.setVerbatimField(DwcTerm.verbatimLongitude, "55.678");
    verbatimOccurrence.setDatasetKey(UUID.randomUUID());
    verbatimOccurrence.setCrawlId(8);
    OccurrenceInterpretationResult result = occurrenceInterpreter.interpret(verbatimOccurrence, null);
    assertTrue(result.getUpdated().getIssues().containsAll(Sets.newHashSet(OccurrenceIssue.GEODETIC_DATUM_ASSUMED_WGS84,
                                                                  OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES,
                                                                  OccurrenceIssue.TAXON_MATCH_NONE,
                                                                  OccurrenceIssue.BASIS_OF_RECORD_INVALID)));
    assertEquals(8, result.getUpdated().getCrawlId().intValue());
  }

  /**
   * https://github.com/gbif/portal-feedback/issues/136
   */
  @Test
  public void testOccurrenceInterpretationCeratiaceae() {
    VerbatimOccurrence v = new VerbatimOccurrence();
    v.setVerbatimField(DwcTerm.verbatimLatitude, "10.123");
    v.setVerbatimField(DwcTerm.verbatimLongitude, "55.678");
    v.setDatasetKey(UUID.randomUUID());
    v.setVerbatimField(DwcTerm.kingdom, "Chromista");
    v.setVerbatimField(DwcTerm.phylum, "Dinophyta");
    v.setVerbatimField(DwcTerm.class_, "Dinophyceae");
    v.setVerbatimField(DwcTerm.order, "Peridiniales");
    v.setVerbatimField(DwcTerm.family, "Ceratiaceae");
    v.setVerbatimField(DwcTerm.genus, "Ceratium");
    v.setVerbatimField(DwcTerm.scientificName, "Ceratium hirundinella");
    v.setVerbatimField(DwcTerm.taxonRank, "species");

    v.setVerbatimField(DwcTerm.verbatimEventDate, "Tue Aug 04 2015 12:22:35 GMT-0400 (EDT)");
    v.setVerbatimField(DwcTerm.taxonID, "345252");
    v.setVerbatimField(DwcTerm.collectionCode, "Observations");
    v.setVerbatimField(DwcTerm.verbatimLocality, "2â€“28 Bonemill Rd, Mansfield, CT, US");
    v.setVerbatimField(DcTerm.rightsHolder, "Karolina Fucikova");
    v.setVerbatimField(DwcTerm.basisOfRecord, "HumanObservation");
    v.setVerbatimField(DwcTerm.identificationID, "3443749");
    v.setVerbatimField(DwcTerm.decimalLatitude, "41.803398");
    v.setVerbatimField(DcTerm.modified, "2015-08-04T19:34:01Z");
    v.setVerbatimField(DwcTerm.eventTime, "16:22:35Z");
    v.setVerbatimField(DwcTerm.recordedBy, "Karolina Fucikova");
    v.setVerbatimField(DcTerm.license, "http://creativecommons.org/licenses/by-nc/4.0/");
    v.setVerbatimField(DwcTerm.coordinateUncertaintyInMeters, "24");
    v.setVerbatimField(DcTerm.identifier, "1831763");
    v.setVerbatimField(DwcTerm.occurrenceID, "http://www.inaturalist.org/observations/1831763");
    v.setVerbatimField(DwcTerm.eventDate, "2015-08-04T12:22:35-04:00");
    v.setVerbatimField(DwcTerm.catalogNumber, "1831763");
    v.setVerbatimField(DwcTerm.establishmentMeans, "wild");
    v.setVerbatimField(DwcTerm.decimalLongitude, "-72.280233");
    v.setVerbatimField(DcTerm.references, "http://www.inaturalist.org/observations/1831763");
    v.setVerbatimField(DwcTerm.institutionCode, "iNaturalist");
    v.setVerbatimField(DwcTerm.countryCode, "US");
    v.setVerbatimField(DwcTerm.dateIdentified, "2015-08-04T17:15:02Z");
    v.setVerbatimField(DcTerm.rights, "© Karolina Fucikova some rights reserved");

    OccurrenceInterpretationResult result = occurrenceInterpreter.interpret(v, null);

    Occurrence o = result.getUpdated();
    assertEquals(7598904, (int)o.getTaxonKey());
    assertEquals(7479242, (int)o.getFamilyKey());
  }

}
