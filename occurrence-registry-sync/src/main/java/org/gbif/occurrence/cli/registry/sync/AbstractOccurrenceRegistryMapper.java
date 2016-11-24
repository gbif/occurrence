package org.gbif.occurrence.cli.registry.sync;

import org.gbif.api.service.registry.DatasetService;
import org.gbif.api.service.registry.OrganizationService;
import org.gbif.common.messaging.api.MessagePublisher;
import org.gbif.common.messaging.guice.PostalServiceModule;
import org.gbif.occurrence.cli.registry.RegistryObjectMapperContextResolver;
import org.gbif.occurrence.common.config.OccHBaseConfiguration;
import org.gbif.occurrence.persistence.api.OccurrencePersistenceService;
import org.gbif.occurrence.persistence.guice.OccurrencePersistenceModule;
import org.gbif.registry.ws.client.DatasetWsClient;
import org.gbif.registry.ws.client.OrganizationWsClient;
import org.gbif.ws.mixin.Mixins;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.client.apache.ApacheHttpClient;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.codehaus.jackson.map.DeserializationConfig;

/**
 * Abstract class containing common logic for registry based Occurrence mapper
 */
public class AbstractOccurrenceRegistryMapper extends TableMapper<ImmutableBytesWritable, NullWritable> {

  public static final String PROP_OCCURRENCE_TABLE_NAME_KEY = "occurrence.db.table_name";
  public static final String PROP_ZK_CONNECTION_STRING_KEY = "occurrence.db.zookeeper.connection_string";

  protected DatasetService datasetService;
  protected OrganizationService orgService;
  protected RegistryBasedOccurrenceMutator occurrenceMutator;
  protected OccurrencePersistenceService occurrencePersistenceService;
  protected MessagePublisher messagePublisher;
  protected Client httpClient;

  /**
   *
   * @param context
   * @throws IOException
   * @throws InterruptedException
   */
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Properties props = new Properties();
    // extract the config properties from the job context
    for (Map.Entry<String, String> entry : context.getConfiguration()) {
      props.setProperty(entry.getKey(), entry.getValue());
    }

    ClientConfig cc = new DefaultClientConfig();
    cc.getClasses().add(JacksonJsonProvider.class);
    cc.getClasses().add(RegistryObjectMapperContextResolver.class);
    cc.getFeatures().put(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES.toString(), false);
    RegistryObjectMapperContextResolver.addMixIns(Mixins.getPredefinedMixins());

    httpClient = ApacheHttpClient.create(cc);
    WebResource regResource = httpClient.resource(props.getProperty(SyncCommon.REG_WS_PROPS_KEY));
    datasetService = new DatasetWsClient(regResource, null);
    orgService = new OrganizationWsClient(regResource, null);
    occurrenceMutator = new RegistryBasedOccurrenceMutator();

    OccHBaseConfiguration occHBaseConfiguration = new OccHBaseConfiguration();
    occHBaseConfiguration.occTable = props.getProperty(PROP_OCCURRENCE_TABLE_NAME_KEY);
    occHBaseConfiguration.zkConnectionString = props.getProperty(PROP_ZK_CONNECTION_STRING_KEY);

    Injector injector =
            Guice.createInjector(new PostalServiceModule("sync", props),
                    new OccurrencePersistenceModule(occHBaseConfiguration, context.getConfiguration()));
    occurrencePersistenceService = injector.getInstance(OccurrencePersistenceService.class);
    messagePublisher = injector.getInstance(MessagePublisher.class);
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    if (messagePublisher != null) {
      // wrapped to ensure we also close the httpClient if messagePublisher throws an exception (including runtime)
      try {
        messagePublisher.close();
      }
      catch (Exception ignore) {}
    }

    if (httpClient != null) {
      httpClient.destroy();
    }
  }
}
