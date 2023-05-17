package org.gbif.occurrence.downloads.launcher.services.launcher.oozie;

import static org.apache.oozie.client.OozieClient.EXTERNAL_ID;
import static org.gbif.occurrence.downloads.launcher.services.launcher.oozie.DownloadWorkflowParameters.DOWNLOAD_FORMAT;
import static org.gbif.occurrence.downloads.launcher.services.launcher.oozie.DownloadWorkflowParameters.DOWNLOAD_KEY;
import static org.gbif.occurrence.downloads.launcher.services.launcher.oozie.DownloadWorkflowParameters.GBIF_FILTER;
import static org.gbif.occurrence.downloads.launcher.services.launcher.oozie.DownloadWorkflowParameters.NOTIFICATION_PROPERTY;
import static org.gbif.occurrence.downloads.launcher.services.launcher.oozie.DownloadWorkflowParameters.USER_PROPERTY;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.model.occurrence.predicate.EqualsPredicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DownloadWorkflowParametersBuilderTest {

  private DownloadWorkflowParametersBuilder builder;

  @BeforeEach
  public void setUp() {
    Map<String, String> defaultProperties = new HashMap<>();
    defaultProperties.put("prop1", "value1");
    defaultProperties.put("prop2", "value2");
    builder = new DownloadWorkflowParametersBuilder(defaultProperties);
  }

  @Test
  void testBuildWorkflowParameters() {
    String downloadKey = "testKey";

    PredicateDownloadRequest request = new PredicateDownloadRequest();
    request.setCreator("testUser");
    request.setFormat(DownloadFormat.DWCA);
    request.setNotificationAddresses(Collections.singleton("testEmail"));
    request.setPredicate(
        new EqualsPredicate(
            OccurrenceSearchParameter.DATASET_KEY, UUID.randomUUID().toString(), false));

    Properties properties = builder.buildWorkflowParameters(downloadKey, request);

    assertEquals("value1", properties.getProperty("prop1"));
    assertEquals("value2", properties.getProperty("prop2"));
    assertEquals(downloadKey, properties.getProperty(EXTERNAL_ID));
    assertEquals(downloadKey, properties.getProperty(DOWNLOAD_KEY));
    assertEquals("testUser", properties.getProperty(USER_PROPERTY));
    assertEquals(DownloadFormat.DWCA.name(), properties.getProperty(DOWNLOAD_FORMAT));
    assertEquals("testEmail", properties.getProperty(NOTIFICATION_PROPERTY));
    assertEquals(
        DownloadWorkflowParametersBuilder.getJsonStringPredicate(request.getPredicate()),
        properties.getProperty(GBIF_FILTER));
  }
}
