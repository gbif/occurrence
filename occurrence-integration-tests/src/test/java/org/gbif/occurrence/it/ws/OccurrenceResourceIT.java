package org.gbif.occurrence.it.ws;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.occurrence.test.extensions.ElasticsearchInitializer;
import org.gbif.occurrence.test.extensions.FragmentInitializer;
import org.gbif.occurrence.test.extensions.OccurrenceRelationshipInitializer;
import org.gbif.occurrence.ws.client.OccurrenceWsClient;
import org.gbif.occurrence.ws.provider.OccurrenceDwcXMLConverter;
import org.gbif.occurrence.ws.provider.OccurrenceVerbatimDwcXMLConverter;
import org.gbif.occurrence.ws.resources.OccurrenceResource;
import org.gbif.ws.client.ClientFactory;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@ActiveProfiles("test")
@AutoConfigureMockMvc
@SpringBootTest(
  classes = OccurrenceWsItConfiguration.class,
  webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class OccurrenceResourceIT {

  private static final ObjectMapper MAPPER = JacksonJsonObjectMapperProvider.getObjectMapper();

  private static final String TEST_DATA_FILE = "classpath:occurrences-test.json";

  private static final String RELATED_DATA_FILE =" classpath:occurrence-relationships.json";

  private static final Long TEST_KEY = 648006L;

  private static final Long RELATION_TEST_KEY = 1019596829L;

  @RegisterExtension
  static FragmentInitializer fragmentInitializer = FragmentInitializer.builder()
                                                    .testDataFile(TEST_DATA_FILE)
                                                    .build();

  @RegisterExtension
  static ElasticsearchInitializer elasticsearchInitializer = ElasticsearchInitializer.builder()
                                                              .testDataFile(TEST_DATA_FILE)
                                                              .build();

  @RegisterExtension
  static OccurrenceRelationshipInitializer occurrenceRelationshipInitializer = OccurrenceRelationshipInitializer.builder()
                                                                                .testDataFile(RELATED_DATA_FILE)
                                                                                .build();



  private final OccurrenceResource occurrenceResource;

  private final OccurrenceWsClient occurrenceWsClient;

  @Autowired
  public OccurrenceResourceIT(@LocalServerPort int localServerPort,
                              OccurrenceResource occurrenceResource) {
    this.occurrenceResource = occurrenceResource;
    ClientFactory clientFactory = new ClientFactory("http://localhost:" + localServerPort);
    occurrenceWsClient = clientFactory.newInstance(OccurrenceWsClient.class);
  }

  @Test
  public void testGetByKey() {
    Occurrence occurrence = occurrenceWsClient.get(TEST_KEY);
    Assertions.assertNotNull(occurrence, "Empty occurrence receuved");
  }

  @Test
  public void testGetFragment() {
    String fragment = occurrenceWsClient.getFragment(TEST_KEY);
    Assertions.assertNotNull(fragment, "Empty fragment received");
  }

  @Test
  public void testGetVerbatim() {
    VerbatimOccurrence verbatim = occurrenceWsClient.getVerbatim(TEST_KEY);
    Assertions.assertNotNull(verbatim, "Empty verbatim record!");
  }

  @Test
  @SneakyThrows
  public void testRelatedOccurrences() {
    String relatedOccurrences = occurrenceResource.getRelatedOccurrences(RELATION_TEST_KEY);
    Assertions.assertNotNull(relatedOccurrences, "Empty related occurrence response");

    JsonNode jsonNode = MAPPER.readTree(relatedOccurrences);
    ArrayNode relatedRecords  = (ArrayNode)jsonNode.get("occurrences");
    Assertions.assertEquals(3, relatedRecords.size(), "Number is related occurrences is not what was expected!");
  }

  @Test
  public void testGetAnnosysVerbatim() {
    String annosysVerbatim = occurrenceResource.getAnnosysVerbatim(TEST_KEY);
    Assertions.assertNotNull(annosysVerbatim, "Empty Annosys response");

    String verbatimXml = OccurrenceVerbatimDwcXMLConverter.verbatimOccurrenceXMLAsString(occurrenceResource.getVerbatim(TEST_KEY));
    Assertions.assertEquals(annosysVerbatim, verbatimXml, "XML records different to expected response");
  }

  @Test
  public void testGetAnnosysOccurrence() {

    String annosysOccurrence = occurrenceResource.getAnnosysOccurrence(TEST_KEY);
    Assertions.assertNotNull(annosysOccurrence, "Empty verbatim Annosys!");

    String occurrenceXml = OccurrenceDwcXMLConverter.occurrenceXMLAsString(occurrenceResource.get(TEST_KEY));
    Assertions.assertEquals(annosysOccurrence, occurrenceXml, "XML records different to expected response");

  }

}
