package org.gbif.occurrence.ws;

import org.gbif.checklistbank.ws.client.guice.ChecklistBankWsClientModule;
import org.gbif.occurrence.download.service.OccurrenceDownloadServiceModule;
import org.gbif.occurrence.persistence.guice.OccurrencePersistenceModule;
import org.gbif.occurrence.search.guice.OccurrenceSearchModule;
import org.gbif.occurrence.ws.resources.FeaturedOccurrenceReader;
import org.gbif.registry.ws.client.guice.RegistryWsClientModule;
import org.gbif.service.guice.PrivateServiceModule;
import org.gbif.user.guice.DrupalMyBatisModule;
import org.gbif.ws.client.guice.AnonymousAuthModule;
import org.gbif.ws.client.guice.SingleUserAuthModule;
import org.gbif.ws.server.guice.GbifServletListener;
import org.gbif.ws.server.guice.WsAuthModule;

import java.util.List;
import java.util.Properties;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import org.apache.bval.guice.ValidationModule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;

public class OccurrenceWsListener extends GbifServletListener {

  private static final String DOWNLOAD_USER_KEY = "occurrence.download.ws.username";
  private static final String DOWNLOAD_PASSWORD_KEY = "occurrence.download.ws.password";

  /**
   * Wires up the featured module to be able to access the HBase table.
   */
  private static class FeaturedModule extends PrivateServiceModule {

    private static final String PREFIX = "occurrence.db.";

    public FeaturedModule(Properties properties) {
      super(PREFIX, properties);
    }

    @SuppressWarnings("unused")
    @Provides
    @Named("featured_table_pool")
    public HTablePool provideHTablePool(@Named("max_connection_pool") Integer maxConnectionPool) {
      return new HTablePool(HBaseConfiguration.create(), maxConnectionPool);
    }

    @Override
    protected void configureService() {
      install(new RegistryWsClientModule(this.getVerbatimProperties()));
      install(new AnonymousAuthModule());
      bind(FeaturedOccurrenceReader.class);
      expose(FeaturedOccurrenceReader.class);
    }
  }

  public OccurrenceWsListener() {
    super("occurrence.properties", "org.gbif.occurrence.ws", true);
  }

  @Override
  @VisibleForTesting
  protected Injector getInjector() {
    return super.getInjector();
  }

  @Override
  protected List<Module> getModules(Properties properties) {
    List<Module> modules = Lists.newArrayList();
    modules.add(new WsAuthModule(properties));
    modules.add(new SingleUserAuthModule(properties.getProperty(DOWNLOAD_USER_KEY),
                                         properties.getProperty(DOWNLOAD_PASSWORD_KEY)));
    modules.add(new RegistryWsClientModule(properties));
    modules.add(new DrupalMyBatisModule(properties));
    modules.add(new ValidationModule());
    modules.add(new ChecklistBankWsClientModule(properties, false, true, true));
    modules.add(new OccurrencePersistenceModule(properties));
    modules.add(new OccurrenceSearchModule(properties));
    modules.add(new OccurrenceDownloadServiceModule(properties));
    modules.add(new FeaturedModule(properties));
    return modules;
  }


}
