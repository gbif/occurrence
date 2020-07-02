package org.gbif.occurrence.it.ws;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.api.vocabulary.EndpointType;
import org.gbif.occurrence.test.extensions.ElasticsearchInitializer;
import org.gbif.occurrence.test.extensions.FragmentInitializer;
import org.gbif.occurrence.ws.provider.OccurrenceDwcXMLConverter;
import org.gbif.occurrence.ws.provider.OccurrenceVerbatimDwcXMLConverter;
import org.gbif.occurrence.ws.resources.OccurrenceResource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@ActiveProfiles("test")
@AutoConfigureMockMvc
@SpringBootTest(
  classes = OccurrenceWsItApp.class,
  webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class OccurrenceResourceIT {

  private static final String TEST_DATA_FILE = "classpath:occurrences-test.json";

  private static final Long TEST_KEY = 648006L;

  @RegisterExtension
  static FragmentInitializer fragmentInitializer = FragmentInitializer.builder()
                                                    .testDataFile(TEST_DATA_FILE)
                                                    .fragmentTableTable(OccurrenceWsItConfiguration.FRAGMENT_TABLE)
                                                    .build();

  @RegisterExtension
  static ElasticsearchInitializer elasticsearchInitializer = ElasticsearchInitializer.builder()
                                                              .testDataFile(TEST_DATA_FILE)
                                                              .build();



  private final OccurrenceResource occurrenceResource;

  @Autowired
  public OccurrenceResourceIT(OccurrenceResource occurrenceResource) {
    this.occurrenceResource = occurrenceResource;
  }

  @Test
  public void testGetByKey() {
    Occurrence occurrence = occurrenceResource.get(TEST_KEY);
    Assertions.assertNotNull(occurrence);
  }

  @Test
  public void testGetFragment() {
    String fragment = occurrenceResource.getFragment(TEST_KEY);
    Assertions.assertNotNull(fragment);
  }

  @Test
  public void testGetVerbatim() {
    VerbatimOccurrence verbatim = occurrenceResource.getVerbatim(TEST_KEY);
    Assertions.assertNotNull(verbatim);
  }

  @Test
  public void testGetAnnosysVerbatim() {
    String annosysVerbatim = occurrenceResource.getAnnosysVerbatim(TEST_KEY);
    Assertions.assertNotNull(annosysVerbatim);

    String verbatimXml = OccurrenceVerbatimDwcXMLConverter.verbatimOccurrenceXMLAsString(occurrenceResource.getVerbatim(TEST_KEY));
    Assertions.assertEquals(annosysVerbatim, verbatimXml);
  }

  @Test
  public void testGetAnnosysOccurrence() {

    String annosysOccurrence = occurrenceResource.getAnnosysOccurrence(TEST_KEY);
    Assertions.assertNotNull(annosysOccurrence);

    String occurrenceXml = OccurrenceDwcXMLConverter.occurrenceXMLAsString(occurrenceResource.get(TEST_KEY));
    Assertions.assertEquals(annosysOccurrence, occurrenceXml);

  }

}
