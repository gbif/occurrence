package org.gbif.occurrence.validation;

import org.gbif.occurrence.processor.interpreting.CoordinateInterpreter;
import org.gbif.occurrence.processor.interpreting.DatasetInfoInterpreter;
import org.gbif.occurrence.processor.interpreting.LocationInterpreter;
import org.gbif.occurrence.processor.interpreting.OccurrenceInterpreter;
import org.gbif.occurrence.processor.interpreting.TaxonomyInterpreter;
import org.gbif.ws.json.JacksonJsonContextResolver;
import org.gbif.ws.mixin.Mixins;

import akka.actor.Actor;
import akka.actor.UntypedActorFactory;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.client.apache.ApacheHttpClient;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;

class FileLineEmitterFactory implements UntypedActorFactory {

  private String apiUrl;

  private static final int CLIENT_TO = 600000; // registry client default timeout

  public FileLineEmitterFactory(String apiUrl) {
    this.apiUrl = apiUrl;
  }

  @Override
  public Actor create() throws Exception {
    return new FileLineEmitter(buidlOccurrenceInterpreter());
  }

  /**
   * Creates an HTTP client.
   */
  private ApacheHttpClient createHttpClient() {
    ClientConfig cc = new DefaultClientConfig();
    cc.getClasses().add(JacksonJsonContextResolver.class);
    cc.getClasses().add(JacksonJsonProvider.class);
    cc.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, true);
    cc.getProperties().put(ClientConfig.PROPERTY_CONNECT_TIMEOUT, CLIENT_TO);
    JacksonJsonContextResolver.addMixIns(Mixins.getPredefinedMixins());
    return ApacheHttpClient.create(cc);
  }

  public OccurrenceInterpreter buidlOccurrenceInterpreter() {
    WebResource webResource = createHttpClient().resource(apiUrl);
    DatasetInfoInterpreter datasetInfoInterpreter = new DatasetInfoInterpreter(webResource);
    TaxonomyInterpreter taxonomyInterpreter = new TaxonomyInterpreter(webResource);
    LocationInterpreter locationInterpreter = new LocationInterpreter(new CoordinateInterpreter(webResource));
    return new OccurrenceInterpreter(datasetInfoInterpreter,taxonomyInterpreter,locationInterpreter);
  }
}
