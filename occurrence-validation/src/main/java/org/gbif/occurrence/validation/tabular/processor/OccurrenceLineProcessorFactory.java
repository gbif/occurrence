package org.gbif.occurrence.validation.tabular.processor;

import org.gbif.dwc.terms.TermFactory;
import org.gbif.occurrence.processor.interpreting.CoordinateInterpreter;
import org.gbif.occurrence.processor.interpreting.DatasetInfoInterpreter;
import org.gbif.occurrence.processor.interpreting.LocationInterpreter;
import org.gbif.occurrence.processor.interpreting.OccurrenceInterpreter;
import org.gbif.occurrence.processor.interpreting.TaxonomyInterpreter;
import org.gbif.occurrence.validation.api.RecordProcessor;
import org.gbif.occurrence.validation.api.RecordProcessorFactory;
import org.gbif.ws.json.JacksonJsonContextResolver;
import org.gbif.ws.mixin.Mixins;

import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.client.apache.ApacheHttpClient;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;

public class OccurrenceLineProcessorFactory implements RecordProcessorFactory {

  private final String apiUrl;

  private static ApacheHttpClient httpClient;

  private static final int CLIENT_TO = 600000; // registry client default timeout

  private static final TermFactory TERM_FACTORY = TermFactory.instance();

  public OccurrenceLineProcessorFactory(String apiUrl) {
    this.apiUrl = apiUrl;
  }

  public RecordProcessor create() {
    return new OccurrenceLineProcessor(buidlOccurrenceInterpreter());
  }

  /**
   * Creates an HTTP client.
   */
  private ApacheHttpClient createHttpClient() {
    if (httpClient == null) {
      ClientConfig cc = new DefaultClientConfig();
      cc.getClasses().add(JacksonJsonContextResolver.class);
      cc.getClasses().add(JacksonJsonProvider.class);
      cc.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, true);
      cc.getProperties().put(ClientConfig.PROPERTY_CONNECT_TIMEOUT, CLIENT_TO);
      JacksonJsonContextResolver.addMixIns(Mixins.getPredefinedMixins());
      httpClient = ApacheHttpClient.create(cc);
    }
    return httpClient;
  }

  private OccurrenceInterpreter buidlOccurrenceInterpreter() {
    WebResource webResource = createHttpClient().resource(apiUrl);
    DatasetInfoInterpreter datasetInfoInterpreter = new DatasetInfoInterpreter(webResource);
    TaxonomyInterpreter taxonomyInterpreter = new TaxonomyInterpreter(webResource);
    LocationInterpreter locationInterpreter = new LocationInterpreter(new CoordinateInterpreter(webResource));
    return new OccurrenceInterpreter(datasetInfoInterpreter, taxonomyInterpreter, locationInterpreter);
  }
}
