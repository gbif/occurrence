package org.gbif.occurrence.ws.client.guice;

import org.gbif.api.service.occurrence.OccurrenceService;
import org.gbif.utils.file.properties.PropertiesUtil;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Properties;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class OccurrenceWsClientModuleTest {

  @Test
  public void testModule() throws IOException, URISyntaxException {
    Properties properties = PropertiesUtil.loadProperties("occurrence-test.properties");
    OccurrenceWsClientModule mod = new OccurrenceWsClientModule(properties);
    Injector inj = Guice.createInjector(mod);
    assertNotNull(inj.getInstance(OccurrenceService.class));
  }
}
