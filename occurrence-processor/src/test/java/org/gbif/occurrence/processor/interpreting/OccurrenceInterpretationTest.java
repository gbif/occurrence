package org.gbif.occurrence.processor.interpreting;

import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.model.registry.Organization;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.License;
import org.gbif.api.vocabulary.OccurrenceIssue;
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

import static org.junit.Assert.assertTrue;

public class OccurrenceInterpretationTest {

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
      DatasetInfoInterpreter datasetInfoInterpreterMock = Mockito.mock(DatasetInfoInterpreter.class);
      Mockito.when(datasetInfoInterpreterMock.getOrgCountry(Mockito.anyObject())).thenReturn(Country.DENMARK);
      Mockito.when(datasetInfoInterpreterMock.getDatasetLicense(Mockito.anyObject())).thenReturn(License.CC_BY_4_0);
      Mockito.when(datasetInfoInterpreterMock.getOrgByDataset(Mockito.anyObject())).thenReturn(organizationMock);
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
    OccurrenceInterpretationResult result = occurrenceInterpreter.interpret(verbatimOccurrence);
    assertTrue(result.getUpdated().getIssues().containsAll(Sets.newHashSet(OccurrenceIssue.GEODETIC_DATUM_ASSUMED_WGS84,
                                                                  OccurrenceIssue.COUNTRY_DERIVED_FROM_COORDINATES,
                                                                  OccurrenceIssue.TAXON_MATCH_NONE,
                                                                  OccurrenceIssue.BASIS_OF_RECORD_INVALID)));


  }
}
